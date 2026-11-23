// Package chaos holds fault-injection tests of the whole async pipeline against real
// Postgres and Kafka: while payments flow through outbox -> Kafka -> worker, the tests
// kill database connections, restart workers, replay duplicate and poison messages and
// make the processor fail, then check that nothing was lost, duplicated or left
// inconsistent.
package chaos_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sayandip18/settlement-engine/internal/command"
	"github.com/sayandip18/settlement-engine/internal/deadletter"
	"github.com/sayandip18/settlement-engine/internal/outbox"
	"github.com/sayandip18/settlement-engine/internal/payment"
	kafkaplat "github.com/sayandip18/settlement-engine/internal/platform/kafka"
	"github.com/sayandip18/settlement-engine/internal/processor"
	"github.com/sayandip18/settlement-engine/internal/reconcile"
	"github.com/sayandip18/settlement-engine/internal/testutil"
)

func TestMain(m *testing.M) { os.Exit(testutil.Run(m)) }

// chaosProc is the mock processor that fails transiently at a configurable rate and
// records every successful call, so the test can see processor-side duplicates.
type chaosProc struct {
	processor.Mock
	failPercent atomic.Int32
	injected    atomic.Int64
	mu          sync.Mutex
	rng         *rand.Rand
	captures    map[string]int // successful captures per reference
}

func (p *chaosProc) maybeFail() error {
	p.mu.Lock()
	n := p.rng.Intn(100)
	p.mu.Unlock()
	if int32(n) < p.failPercent.Load() {
		p.injected.Add(1)
		return fmt.Errorf("%w: chaos", processor.ErrTemporary)
	}
	return nil
}

func (p *chaosProc) Authorize(ctx context.Context, r processor.Request) error {
	if err := p.maybeFail(); err != nil {
		return err
	}
	return p.Mock.Authorize(ctx, r)
}

func (p *chaosProc) Capture(ctx context.Context, r processor.Request) error {
	if err := p.maybeFail(); err != nil {
		return err
	}
	err := p.Mock.Capture(ctx, r)
	if err == nil {
		p.mu.Lock()
		p.captures[r.ReferenceID]++
		p.mu.Unlock()
	}
	return err
}

func (p *chaosProc) Refund(ctx context.Context, r processor.Request) error {
	if err := p.maybeFail(); err != nil {
		return err
	}
	return p.Mock.Refund(ctx, r)
}

type system struct {
	t        *testing.T
	pool     *pgxpool.Pool
	brokers  []string
	topic    string
	group    string
	proc     *chaosProc
	payments *payment.Service
	commands *command.Service
	dlq      *deadletter.Service
	policy   command.RetryPolicy
	log      *slog.Logger
	starts   atomic.Int64
}

func newSystem(t *testing.T) *system {
	t.Helper()
	pool := testutil.NewPool(t)
	brokers := testutil.NewKafka(t)
	id := uuid.NewString()[:8]
	s := &system{
		t: t, pool: pool, brokers: brokers, topic: "chaos-" + id, group: "chaos-group-" + id,
		proc:   &chaosProc{rng: rand.New(rand.NewSource(time.Now().UnixNano())), captures: map[string]int{}},
		policy: command.RetryPolicy{MaxAttempts: 6, Base: 50 * time.Millisecond, Max: 400 * time.Millisecond, Jitter: 0.2},
		log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	s.payments = payment.NewService(pool, s.proc, 200)
	s.commands = command.NewService(pool, s.payments, s.topic)
	s.dlq = deadletter.NewService(pool, s.commands)
	if err := kafkaplat.EnsureTopics(context.Background(), brokers,
		kafkaplat.Topic{Name: s.topic, Partitions: 3},
		kafkaplat.Topic{Name: command.DLQTopic(s.topic), Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	return s
}

// startInstance runs one complete worker (relay + command consumer + DLQ store), like one
// replica of cmd/worker. The returned function stops it cleanly.
func (s *system) startInstance() (stop func()) {
	s.starts.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	pub := kafkaplat.NewPublisher(s.brokers)
	relay := outbox.NewRelay(s.pool, pub, s.log, 50, 10*time.Millisecond)
	worker := command.NewWorker(s.pool, s.payments, s.topic, s.policy, s.log)
	consumer := kafkaplat.NewConsumer(s.brokers, s.group, s.topic, s.log)
	dlqConsumer := kafkaplat.NewConsumer(s.brokers, s.group+"-dlq", command.DLQTopic(s.topic), s.log)

	var wg sync.WaitGroup
	for _, fn := range []func(){
		func() { relay.Run(ctx) },                     //nolint:errcheck
		func() { consumer.Run(ctx, worker.Handle) },   //nolint:errcheck
		func() { dlqConsumer.Run(ctx, s.dlq.Record) }, //nolint:errcheck
	} {
		wg.Add(1)
		go func() { defer wg.Done(); fn() }()
	}
	return func() {
		cancel()
		wg.Wait()
		consumer.Close()
		dlqConsumer.Close()
		pub.Close()
	}
}

// retry keeps calling fn (the "client" retrying through outages) until it succeeds.
func retry(ctx context.Context, fn func() error) error {
	var err error
	for {
		if err = fn(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("gave up: %w (last error: %v)", ctx.Err(), err)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (s *system) waitTerminal(ctx context.Context, id uuid.UUID) (command.Status, error) {
	for {
		var op command.Operation
		err := retry(ctx, func() (err error) { op, err = s.commands.Get(ctx, id); return })
		if err != nil {
			return "", err
		}
		if op.Status.Terminal() {
			return op.Status, nil
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("operation %s still %s: %w", id, op.Status, ctx.Err())
		case <-time.After(40 * time.Millisecond):
		}
	}
}

func (s *system) submit(ctx context.Context, in command.SubmitInput) (command.Operation, error) {
	var res command.SubmitResult
	err := retry(ctx, func() (err error) { res, err = s.commands.Submit(ctx, in); return })
	return res.Operation, err
}

// journey is one client's full lifecycle for a payment: authorize, capture, two refunds.
// It stops at the first operation that does not succeed (e.g. a decline).
func (s *system) journey(ctx context.Context, n int, customer string) (opIDs []uuid.UUID, err error) {
	var created payment.CreateResult
	in := payment.CreateInput{MerchantID: "m1", CustomerID: customer, AmountPaise: int64(100000 + n), IdempotencyKey: uuid.NewString()}
	if err := retry(ctx, func() (err error) { created, err = s.payments.Create(ctx, in); return }); err != nil {
		return nil, err
	}
	id := created.Payment.ID
	half := int64(40000)
	steps := []command.SubmitInput{
		{PaymentID: id, Kind: command.KindAuthorize},
		{PaymentID: id, Kind: command.KindCapture},
		{PaymentID: id, Kind: command.KindRefund, AmountPaise: &half, IdempotencyKey: "r1"},
		{PaymentID: id, Kind: command.KindRefund, IdempotencyKey: "r2"},
	}
	for _, step := range steps {
		op, err := s.submit(ctx, step)
		if err != nil {
			return opIDs, err
		}
		opIDs = append(opIDs, op.ID)
		status, err := s.waitTerminal(ctx, op.ID)
		if err != nil {
			return opIDs, err
		}
		if status != command.StatusSucceeded {
			return opIDs, nil
		}
	}
	return opIDs, nil
}

// killBackends terminates every other database connection, like a failover or a
// network partition between the service and Postgres.
func (s *system) killBackends(ctx context.Context) (int64, error) {
	conn, err := pgx.Connect(ctx, s.pool.Config().ConnString())
	if err != nil {
		return 0, err
	}
	defer conn.Close(context.Background())
	var killed int64
	for ctx.Err() == nil {
		var n int64
		if err := conn.QueryRow(ctx,
			`SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
			  WHERE datname = current_database() AND pid <> pg_backend_pid()`).Scan(&n); err != nil {
			return killed, err
		}
		killed += n
		select {
		case <-ctx.Done():
		case <-time.After(400 * time.Millisecond):
		}
	}
	return killed, nil
}

func TestChaosPipelineKeepsEveryInvariant(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test is slow")
	}
	s := newSystem(t)
	s.proc.failPercent.Store(20)

	// Two replicas share the consumer group; chaos restarts them one at a time.
	var mu sync.Mutex
	stops := []func(){s.startInstance(), s.startInstance()}
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, st := range stops {
			st()
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	// ---- clients
	customers := []string{"decline_a", "capture_decline_b", "refund_decline_c"}
	const payments = 30
	type journeyResult struct {
		ops []uuid.UUID
		err error
	}
	results := make(chan journeyResult, payments)
	var clients sync.WaitGroup
	for i := 0; i < payments; i++ {
		customer := fmt.Sprintf("cust-%d", i)
		if i%10 == 7 {
			customer = customers[(i/10)%len(customers)]
		}
		clients.Add(1)
		go func() {
			defer clients.Done()
			ops, err := s.journey(ctx, i, customer)
			results <- journeyResult{ops, err}
		}()
	}

	// ---- chaos
	chaosCtx, stopChaos := context.WithTimeout(ctx, 14*time.Second)
	defer stopChaos()
	var chaosWG sync.WaitGroup
	var killed, restarts, duplicates, poisoned atomic.Int64

	chaosWG.Add(1)
	go func() { // database connections die
		defer chaosWG.Done()
		n, err := s.killBackends(chaosCtx)
		if err != nil && chaosCtx.Err() == nil {
			t.Errorf("killer: %v", err)
		}
		killed.Store(n)
	}()

	chaosWG.Add(1)
	go func() { // workers crash and restart
		defer chaosWG.Done()
		for i := 0; chaosCtx.Err() == nil; i++ {
			select {
			case <-chaosCtx.Done():
				return
			case <-time.After(4 * time.Second):
			}
			idx := i % 2
			mu.Lock()
			old := stops[idx]
			mu.Unlock()
			old() // stop the old replica mid-flight
			fresh := s.startInstance()
			mu.Lock()
			stops[idx] = fresh
			mu.Unlock()
			restarts.Add(1)
		}
	}()

	chaosWG.Add(1)
	go func() { // duplicate and poison messages arrive from the broker
		defer chaosWG.Done()
		pub := kafkaplat.NewPublisher(s.brokers)
		defer pub.Close()
		for chaosCtx.Err() == nil {
			select {
			case <-chaosCtx.Done():
				return
			case <-time.After(300 * time.Millisecond):
			}
			var opID, payID uuid.UUID
			err := s.pool.QueryRow(chaosCtx, `SELECT id, payment_id FROM operations ORDER BY random() LIMIT 1`).Scan(&opID, &payID)
			if err == nil {
				body, _ := json.Marshal(map[string]any{"operation_id": opID, "attempt": 1})
				if pub.Publish(chaosCtx, []outbox.Message{{Topic: s.topic, Key: payID.String(), Payload: body}}) == nil {
					duplicates.Add(1)
				}
			}
			if poisoned.Load() < 5 && rand.Intn(3) == 0 {
				if pub.Publish(chaosCtx, []outbox.Message{{Topic: s.topic, Key: "poison", Payload: []byte(fmt.Sprintf("not json %d", poisoned.Load()))}}) == nil {
					poisoned.Add(1)
				}
			}
		}
	}()

	chaosWG.Wait()
	for poisoned.Load() < 5 { // make sure all five arrive, whatever the dice said
		pub := kafkaplat.NewPublisher(s.brokers)
		err := pub.Publish(ctx, []outbox.Message{{Topic: s.topic, Key: "poison", Payload: []byte(fmt.Sprintf("not json %d", poisoned.Load()))}})
		pub.Close()
		if err == nil {
			poisoned.Add(1)
		}
	}

	// ---- recovery: clients finish, everything drains
	clients.Wait()
	close(results)
	var allOps []uuid.UUID
	for r := range results {
		if r.err != nil {
			t.Errorf("a client journey failed: %v", r.err)
		}
		allOps = append(allOps, r.ops...)
	}
	t.Logf("chaos: %d backends killed, %d worker restarts (%d instance starts), %d duplicate messages, %d poison messages, %d injected processor failures, %d operations",
		killed.Load(), restarts.Load(), s.starts.Load(), duplicates.Load(), poisoned.Load(), s.proc.injected.Load(), len(allOps))
	if killed.Load() == 0 || restarts.Load() < 2 || duplicates.Load() == 0 || s.proc.injected.Load() == 0 {
		t.Fatal("the chaos did not actually happen; the test proves nothing")
	}

	count := func(sql string, args ...any) int {
		var n int
		if err := retry(ctx, func() error { return s.pool.QueryRow(ctx, sql, args...).Scan(&n) }); err != nil {
			t.Fatal(err)
		}
		return n
	}
	waitFor := func(what string, cond func() bool) {
		deadline := time.Now().Add(90 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	waitFor("the outbox to drain", func() bool {
		return count(`SELECT count(*) FROM outbox WHERE published_at IS NULL AND available_at <= now()`) == 0
	})
	waitFor("every poison message to be dead-lettered", func() bool {
		return count(`SELECT count(*) FROM dead_letters WHERE operation_id IS NULL`) >= 5
	})
	// Let any in-flight duplicate deliveries land before judging the end state.
	time.Sleep(3 * time.Second)

	// ---- invariants
	// 1. Every operation reached a final state.
	if n := count(`SELECT count(*) FROM operations WHERE status = 'PENDING'`); n != 0 {
		t.Errorf("%d operations still PENDING", n)
	}
	// 2. Poison messages are dead-lettered exactly once each, however often they were redelivered.
	if n := count(`SELECT count(*) FROM dead_letters WHERE operation_id IS NULL`); n != 5 {
		t.Errorf("%d raw dead letters, want exactly 5 (duplicates not deduplicated?)", n)
	}
	// 3. The ledger balances overall, and money was posted at most once per event.
	if n := count(`SELECT count(*) FROM (SELECT 1 FROM ledger_entries
	                  HAVING COALESCE(SUM(amount_paise) FILTER (WHERE direction = 'DEBIT'), 0)
	                      <> COALESCE(SUM(amount_paise) FILTER (WHERE direction = 'CREDIT'), 0)) x`); n != 0 {
		t.Error("ledger debits do not equal credits")
	}
	if n := count(`SELECT count(*) FROM (SELECT payment_id FROM ledger_transactions WHERE kind = 'CAPTURE'
	                  GROUP BY payment_id HAVING count(*) > 1) x`); n != 0 {
		t.Errorf("%d payments were captured more than once in the ledger", n)
	}
	if a, b := count(`SELECT count(*) FROM refunds`), count(`SELECT count(*) FROM ledger_transactions WHERE kind = 'REFUND'`); a != b {
		t.Errorf("%d refunds but %d REFUND ledger transactions", a, b)
	}
	// 4. A payment's state agrees with the operations that succeeded for it.
	if n := count(`SELECT count(*) FROM operations o JOIN payments p ON p.id = o.payment_id
	                WHERE o.kind = 'CAPTURE' AND o.status = 'SUCCEEDED'
	                  AND p.status NOT IN ('CAPTURED', 'SETTLED', 'PARTIALLY_REFUNDED', 'REFUNDED')`); n != 0 {
		t.Errorf("%d payments have a succeeded capture but are not in a captured state", n)
	}
	// 5. The reconciler agrees: no ledger, state or plumbing problem. The only acceptable
	// finding is an operation that was legitimately dead-lettered after exhausting retries.
	rec := reconcile.NewService(s.pool, reconcile.DefaultThresholds(), s.log)
	var res reconcile.RunResult
	if err := retry(ctx, func() (err error) { res, err = rec.Run(ctx); return }); err != nil {
		t.Fatal(err)
	}
	for _, c := range res.Checks {
		if c.Error != "" {
			t.Errorf("reconciliation check %s failed: %s", c.Type, c.Error)
		}
		if c.Found > 0 && c.Type != reconcile.TypeOperationStuck {
			t.Errorf("reconciliation found %d %s problems after chaos", c.Found, c.Type)
		}
	}
	deadLettered := count(`SELECT count(*) FROM operations WHERE status = 'DEAD_LETTERED'`)
	for _, c := range res.Checks {
		if c.Type == reconcile.TypeOperationStuck && c.Found != deadLettered {
			t.Errorf("reconciler reports %d stuck operations, database has %d dead-lettered", c.Found, deadLettered)
		}
	}

	// Informational: the processor can legitimately see a capture twice when a crash hits
	// between the processor call and the commit; it is expected to dedupe on the reference.
	s.proc.mu.Lock()
	dup := 0
	for _, n := range s.proc.captures {
		if n > 1 {
			dup += n - 1
		}
	}
	s.proc.mu.Unlock()
	t.Logf("end state: %d dead-lettered operations; %d processor-side duplicate captures (processor must dedupe on ReferenceID)", deadLettered, dup)
}
