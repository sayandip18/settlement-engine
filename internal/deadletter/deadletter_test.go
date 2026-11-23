package deadletter_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sayandip18/settlement-engine/internal/command"
	"github.com/sayandip18/settlement-engine/internal/deadletter"
	"github.com/sayandip18/settlement-engine/internal/httpapi"
	"github.com/sayandip18/settlement-engine/internal/payment"
	"github.com/sayandip18/settlement-engine/internal/platform/kafka"
	"github.com/sayandip18/settlement-engine/internal/processor"
	"github.com/sayandip18/settlement-engine/internal/testutil"
)

func TestMain(m *testing.M) { os.Exit(testutil.Run(m)) }

const topic = "payments.commands"

type env struct {
	pool     *pgxpool.Pool
	payments *payment.Service
	commands *command.Service
	svc      *deadletter.Service
}

func setup(t *testing.T) env {
	t.Helper()
	pool := testutil.NewPool(t)
	payments := payment.NewService(pool, processor.NewMock(), 200)
	commands := command.NewService(pool, payments, topic)
	return env{pool, payments, commands, deadletter.NewService(pool, commands)}
}

// deadLetteredOp creates an authorize operation and forces it into the state the worker
// leaves it in after exhausting its retries.
func (e env) deadLetteredOp(t *testing.T) command.Operation {
	t.Helper()
	res, err := e.payments.Create(context.Background(), payment.CreateInput{
		MerchantID: "m1", CustomerID: "c1", AmountPaise: 1000, IdempotencyKey: uuid.NewString()})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := e.commands.Submit(context.Background(), command.SubmitInput{PaymentID: res.Payment.ID, Kind: command.KindAuthorize})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(context.Background(),
		`UPDATE operations SET status = 'DEAD_LETTERED', attempts = 3,
		        error_code = 'processor_unavailable', error_message = 'timed out' WHERE id = $1`, sub.Operation.ID); err != nil {
		t.Fatal(err)
	}
	// The submit's own message is irrelevant to replay; clear the outbox so tests can
	// count only what replay adds.
	if _, err := e.pool.Exec(context.Background(), `DELETE FROM outbox`); err != nil {
		t.Fatal(err)
	}
	return sub.Operation
}

// dlqMessage mimics what the worker publishes for an exhausted operation.
func dlqMessage(op command.Operation, offset int64) kafka.Message {
	return kafka.Message{
		Topic: topic + ".dlq", Partition: 0, Offset: offset + 1000, Key: op.PaymentID.String(),
		Value: []byte(fmt.Sprintf(`{"operation_id":%q,"attempt":3}`, op.ID)),
		Headers: map[string]string{
			"error": "processor unavailable", "attempts": "3", "operation_id": op.ID.String(),
			"source_topic": topic, "source_partition": "1", "source_offset": fmt.Sprint(offset),
		},
	}
}

func poison(offset int64, value []byte) kafka.Message {
	return kafka.Message{
		Topic: topic + ".dlq", Offset: offset + 2000, Key: "k", Value: value,
		Headers: map[string]string{
			"error": "undecodable message", "attempts": "0",
			"source_topic": topic, "source_partition": "2", "source_offset": fmt.Sprint(offset),
		},
	}
}

func (e env) record(t *testing.T, m kafka.Message) deadletter.Entry {
	t.Helper()
	if err := e.svc.Record(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	items, _, err := e.svc.List(context.Background(), deadletter.ListFilter{Limit: 1})
	if err != nil || len(items) == 0 {
		t.Fatalf("list after record: %v %v", items, err)
	}
	return items[0]
}

func (e env) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRecordStoresFailureDetails(t *testing.T) {
	e := setup(t)
	op := e.deadLetteredOp(t)
	entry := e.record(t, dlqMessage(op, 42))

	if entry.Status != deadletter.StatusPending || entry.Error != "processor unavailable" || entry.Attempts != 3 ||
		entry.OperationID == nil || *entry.OperationID != op.ID || entry.Key != op.PaymentID.String() ||
		*entry.SourceTopic != topic || *entry.SourcePartition != 1 || *entry.SourceOffset != 42 || entry.ReplayCount != 0 {
		t.Fatalf("entry: %+v", entry)
	}
	if entry.OperationStatus == nil || *entry.OperationStatus != "DEAD_LETTERED" {
		t.Fatalf("operation status should be joined in live: %v", entry.OperationStatus)
	}
}

func TestRecordIsIdempotent(t *testing.T) {
	e := setup(t)
	op := e.deadLetteredOp(t)
	m := dlqMessage(op, 42)

	for i := 0; i < 3; i++ { // same message redelivered
		if err := e.svc.Record(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	// The same failed source message published to the DLQ twice lands at two DLQ offsets.
	again := m
	again.Offset += 1
	if err := e.svc.Record(context.Background(), again); err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, `SELECT count(*) FROM dead_letters`); n != 1 {
		t.Fatalf("%d rows, want 1", n)
	}
	// A different failed message is a different entry.
	if err := e.svc.Record(context.Background(), dlqMessage(op, 43)); err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, `SELECT count(*) FROM dead_letters`); n != 2 {
		t.Fatalf("%d rows, want 2", n)
	}
}

func TestRecordToleratesMissingOrBadHeaders(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	bare := kafka.Message{Topic: topic + ".dlq", Partition: 1, Offset: 5, Key: "", Value: []byte("x")}
	if err := e.svc.Record(ctx, bare); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.Record(ctx, bare); err != nil { // deduplicated on its DLQ coordinates
		t.Fatal(err)
	}
	junk := kafka.Message{Topic: topic + ".dlq", Offset: 6, Value: []byte("y"), Headers: map[string]string{
		"operation_id": "not-a-uuid", "source_topic": topic, "source_partition": "x", "source_offset": "y", "attempts": "many"}}
	if err := e.svc.Record(ctx, junk); err != nil {
		t.Fatal(err)
	}
	items, _, _ := e.svc.List(ctx, deadletter.ListFilter{})
	if len(items) != 2 {
		t.Fatalf("%d entries, want 2", len(items))
	}
	for _, it := range items {
		if it.OperationID != nil || it.SourceTopic != nil || it.Error != "unknown" || it.Attempts != 0 {
			t.Fatalf("expected tolerant defaults, got %+v", it)
		}
	}
}

func TestListFiltersAndPaginates(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	var ids []uuid.UUID
	for i := int64(0); i < 5; i++ {
		ids = append(ids, e.record(t, poison(i, []byte(fmt.Sprint("p", i)))).ID)
	}
	e.svc.Dismiss(ctx, ids[0], "")
	e.svc.Dismiss(ctx, ids[1], "")

	// Newest first, in pages of 2, with a cursor.
	var seen []uuid.UUID
	var cursor int64
	pages := 0
	for {
		items, next, err := e.svc.List(ctx, deadletter.ListFilter{Limit: 2, Before: cursor})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, it := range items {
			seen = append(seen, it.ID)
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if pages != 3 || len(seen) != 5 {
		t.Fatalf("pages=%d seen=%d", pages, len(seen))
	}
	for i := range seen {
		if seen[i] != ids[4-i] {
			t.Fatalf("position %d: not newest-first", i)
		}
	}

	pending := deadletter.StatusPending
	items, next, _ := e.svc.List(ctx, deadletter.ListFilter{Status: &pending})
	if len(items) != 3 || next != 0 {
		t.Fatalf("pending: %d items, next=%d", len(items), next)
	}
	dismissed := deadletter.StatusDismissed
	if items, _, _ := e.svc.List(ctx, deadletter.ListFilter{Status: &dismissed}); len(items) != 2 {
		t.Fatalf("dismissed: %d items", len(items))
	}
	// An exactly-full last page does not invent a next page.
	if _, next, _ := e.svc.List(ctx, deadletter.ListFilter{Limit: 5}); next != 0 {
		t.Fatalf("next cursor %d after exactly the last item", next)
	}
}

func TestGetShowsPayloadAsTextOrBase64(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	text := e.record(t, poison(1, []byte(`{"hello":"world"}`)))
	bin := e.record(t, poison(2, []byte{0xff, 0xfe, 0x00, 0x01}))

	got, err := e.svc.Get(ctx, text.ID)
	if err != nil || got.Payload == nil || *got.Payload != `{"hello":"world"}` || got.PayloadEncoding != "utf8" || got.PayloadSize != 17 {
		t.Fatalf("text: %+v %v", got, err)
	}
	got, err = e.svc.Get(ctx, bin.ID)
	if err != nil || got.Payload == nil || *got.Payload != "//4AAQ==" || got.PayloadEncoding != "base64" {
		t.Fatalf("binary: %+v %v", got, err)
	}
	if _, err := e.svc.Get(ctx, uuid.New()); !errors.Is(err, deadletter.ErrNotFound) {
		t.Fatalf("unknown id: %v", err)
	}
	// Listings stay light: no payloads.
	items, _, _ := e.svc.List(ctx, deadletter.ListFilter{})
	for _, it := range items {
		if it.Payload != nil {
			t.Fatal("list must not include payloads")
		}
	}
}

func TestReplayResetsOperationAndQueuesFreshAttempt(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	op := e.deadLetteredOp(t)
	entry := e.record(t, dlqMessage(op, 7))

	replayed, err := e.svc.Replay(ctx, entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Status != deadletter.StatusReplayed || replayed.ReplayCount != 1 || replayed.ReplayedAt == nil {
		t.Fatalf("entry: %+v", replayed)
	}
	got, err := e.commands.Get(ctx, op.ID)
	if err != nil || got.Status != command.StatusPending || got.Attempts != 0 || got.Error != nil {
		t.Fatalf("operation not reset: %+v %v", got, err)
	}
	// Exactly one fresh first-attempt message, keyed by payment, on the commands topic.
	var payload []byte
	var key, tp string
	if err := e.pool.QueryRow(ctx, `SELECT topic, msg_key, payload FROM outbox`).Scan(&tp, &key, &payload); err != nil {
		t.Fatalf("want exactly one outbox row: %v", err)
	}
	if tp != topic || key != op.PaymentID.String() || !strings.Contains(string(payload), `"attempt":1`) ||
		!strings.Contains(string(payload), op.ID.String()) {
		t.Fatalf("outbox: topic=%s key=%s payload=%s", tp, key, payload)
	}
}

func TestReplayTwiceIsRejected(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	entry := e.record(t, dlqMessage(e.deadLetteredOp(t), 7))

	if _, err := e.svc.Replay(ctx, entry.ID); err != nil {
		t.Fatal(err)
	}
	var handled *deadletter.AlreadyHandledError
	if _, err := e.svc.Replay(ctx, entry.ID); !errors.As(err, &handled) || handled.Status != deadletter.StatusReplayed {
		t.Fatalf("second replay: %v", err)
	}
	if n := e.count(t, `SELECT count(*) FROM outbox`); n != 1 {
		t.Fatalf("%d outbox rows after double replay", n)
	}
}

func TestConcurrentReplaysEnqueueOnce(t *testing.T) {
	e := setup(t)
	entry := e.record(t, dlqMessage(e.deadLetteredOp(t), 7))

	const n = 10
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, errs[i] = e.svc.Replay(context.Background(), entry.ID) }()
	}
	wg.Wait()
	ok := 0
	for _, err := range errs {
		var handled *deadletter.AlreadyHandledError
		switch {
		case err == nil:
			ok++
		case !errors.As(err, &handled):
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if ok != 1 || e.count(t, `SELECT count(*) FROM outbox`) != 1 {
		t.Fatalf("%d replays succeeded, %d outbox rows; want exactly 1 of each", ok, e.count(t, `SELECT count(*) FROM outbox`))
	}
}

func TestReplayOfUnreadableMessageRepublishesOriginalPayload(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	entry := e.record(t, poison(9, []byte("}{ garbage")))

	if _, err := e.svc.Replay(ctx, entry.ID); err != nil {
		t.Fatal(err)
	}
	var tp, key string
	var payload []byte
	if err := e.pool.QueryRow(ctx, `SELECT topic, msg_key, payload FROM outbox`).Scan(&tp, &key, &payload); err != nil {
		t.Fatal(err)
	}
	if tp != topic || key != "k" || string(payload) != "}{ garbage" {
		t.Fatalf("outbox: %s %s %q", tp, key, payload)
	}
}

func TestReplayRefusesWhenOperationMovedOn(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	op := e.deadLetteredOp(t)
	entry := e.record(t, dlqMessage(op, 7))
	// Someone resolved it another way in the meantime.
	if _, err := e.pool.Exec(ctx, `UPDATE operations SET status = 'SUCCEEDED' WHERE id = $1`, op.ID); err != nil {
		t.Fatal(err)
	}

	var nope *deadletter.NotReplayableError
	if _, err := e.svc.Replay(ctx, entry.ID); !errors.As(err, &nope) || !strings.Contains(nope.Reason, "SUCCEEDED") {
		t.Fatalf("got %v", err)
	}
	if n := e.count(t, `SELECT count(*) FROM outbox`); n != 0 {
		t.Fatal("nothing must be queued for a refused replay")
	}
	// The entry is untouched and can be dismissed instead.
	got, _ := e.svc.Get(ctx, entry.ID)
	if got.Status != deadletter.StatusPending || got.ReplayCount != 0 {
		t.Fatalf("entry changed by refused replay: %+v", got)
	}
	if d, err := e.svc.Dismiss(ctx, entry.ID, "fixed by hand"); err != nil || d.Status != deadletter.StatusDismissed || *d.Note != "fixed by hand" {
		t.Fatalf("dismiss: %+v %v", d, err)
	}
}

func TestReplayRefusesUnknownOperationAndEntriesWithNowhereToGo(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	var nope *deadletter.NotReplayableError

	ghost := dlqMessage(command.Operation{ID: uuid.New(), PaymentID: uuid.New()}, 1)
	if _, err := e.svc.Replay(ctx, e.record(t, ghost).ID); !errors.As(err, &nope) {
		t.Fatalf("unknown operation: %v", err)
	}
	bare := e.record(t, kafka.Message{Topic: topic + ".dlq", Offset: 3, Key: "k", Value: []byte("x")})
	if _, err := e.svc.Replay(ctx, bare.ID); !errors.As(err, &nope) {
		t.Fatalf("no operation, no source topic: %v", err)
	}
	if _, err := e.svc.Replay(ctx, uuid.New()); !errors.Is(err, deadletter.ErrNotFound) {
		t.Fatalf("unknown id: %v", err)
	}
}

func TestDismiss(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	entry := e.record(t, poison(1, []byte("x")))

	d, err := e.svc.Dismiss(ctx, entry.ID, "")
	if err != nil || d.Status != deadletter.StatusDismissed || d.Note != nil {
		t.Fatalf("%+v %v", d, err)
	}
	var handled *deadletter.AlreadyHandledError
	if _, err := e.svc.Dismiss(ctx, entry.ID, "again"); !errors.As(err, &handled) {
		t.Fatalf("second dismiss: %v", err)
	}
	if _, err := e.svc.Replay(ctx, entry.ID); !errors.As(err, &handled) || handled.Status != deadletter.StatusDismissed {
		t.Fatalf("replay of dismissed: %v", err)
	}
	if _, err := e.svc.Dismiss(ctx, uuid.New(), ""); !errors.Is(err, deadletter.ErrNotFound) {
		t.Fatalf("unknown id: %v", err)
	}
}

// --- HTTP ---

func server(t *testing.T, e env) *httptest.Server {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := httpapi.NewRouter(log, httpapi.NewHealth(nil), "admin-key", nil,
		func(r chi.Router) { deadletter.NewHandler(e.svc, log).Routes(r) })
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv
}

func call(t *testing.T, method, url, auth, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestHTTPRequiresAdminAuth(t *testing.T) {
	e := setup(t)
	srv := server(t, e)
	id := e.record(t, poison(1, []byte("x"))).ID.String()
	for _, c := range []struct{ method, path string }{
		{"GET", "/admin/dlq"}, {"GET", "/admin/dlq/" + id},
		{"POST", "/admin/dlq/" + id + "/replay"}, {"POST", "/admin/dlq/" + id + "/dismiss"},
	} {
		for _, auth := range []string{"", "Bearer wrong"} {
			if code, _ := call(t, c.method, srv.URL+c.path, auth, ""); code != http.StatusUnauthorized {
				t.Errorf("%s %s with auth %q: got %d, want 401", c.method, c.path, auth, code)
			}
		}
	}
	if n := e.count(t, `SELECT count(*) FROM dead_letters WHERE status <> 'PENDING'`); n != 0 {
		t.Fatal("an unauthenticated request changed state")
	}
}

func TestHTTPListGetReplayDismiss(t *testing.T) {
	e := setup(t)
	srv := server(t, e)
	auth := "Bearer admin-key"
	op := e.deadLetteredOp(t)
	a := e.record(t, dlqMessage(op, 1))
	b := e.record(t, poison(2, []byte("junk")))

	code, body := call(t, "GET", srv.URL+"/admin/dlq?limit=1", auth, "")
	var page struct {
		Items      []deadletter.Entry `json:"items"`
		NextCursor string             `json:"next_cursor"`
	}
	if err := json.Unmarshal([]byte(body), &page); err != nil || code != 200 ||
		len(page.Items) != 1 || page.Items[0].ID != b.ID || page.NextCursor == "" {
		t.Fatalf("page 1: %d %s", code, body)
	}
	_, body = call(t, "GET", srv.URL+"/admin/dlq?limit=1&cursor="+page.NextCursor, auth, "")
	var page2 struct {
		Items      []deadletter.Entry `json:"items"`
		NextCursor string             `json:"next_cursor"`
	}
	json.Unmarshal([]byte(body), &page2)
	if len(page2.Items) != 1 || page2.Items[0].ID != a.ID || page2.NextCursor != "" {
		t.Fatalf("page 2: %s", body)
	}
	if _, body := call(t, "GET", srv.URL+"/admin/dlq?status=REPLAYED", auth, ""); !strings.Contains(body, `"items":[]`) {
		t.Fatalf("empty list must be [] not null: %s", body)
	}

	if code, body := call(t, "GET", srv.URL+"/admin/dlq/"+b.ID.String(), auth, ""); code != 200 || !strings.Contains(body, `"payload":"junk"`) {
		t.Fatalf("get: %d %s", code, body)
	}
	if code, body := call(t, "POST", srv.URL+"/admin/dlq/"+a.ID.String()+"/replay", auth, ""); code != 200 || !strings.Contains(body, `"REPLAYED"`) {
		t.Fatalf("replay: %d %s", code, body)
	}
	if code, body := call(t, "POST", srv.URL+"/admin/dlq/"+a.ID.String()+"/replay", auth, ""); code != 409 || !strings.Contains(body, "already_handled") {
		t.Fatalf("second replay: %d %s", code, body)
	}
	if code, body := call(t, "POST", srv.URL+"/admin/dlq/"+b.ID.String()+"/dismiss", auth, `{"note":"garbage from a test"}`); code != 200 || !strings.Contains(body, "garbage from a test") {
		t.Fatalf("dismiss: %d %s", code, body)
	}
}

func TestHTTPValidationAndErrors(t *testing.T) {
	e := setup(t)
	srv := server(t, e)
	auth := "Bearer admin-key"
	id := e.record(t, kafka.Message{Topic: topic + ".dlq", Offset: 1, Key: "k", Value: []byte("x")}).ID.String() // nowhere to replay to

	for _, c := range []struct {
		name, method, path, body string
		want                     int
	}{
		{"bad status", "GET", "/admin/dlq?status=nope", "", 400},
		{"limit zero", "GET", "/admin/dlq?limit=0", "", 400},
		{"limit too big", "GET", "/admin/dlq?limit=201", "", 400},
		{"limit text", "GET", "/admin/dlq?limit=x", "", 400},
		{"bad cursor", "GET", "/admin/dlq?cursor=-3", "", 400},
		{"bad id", "GET", "/admin/dlq/nope", "", 400},
		{"unknown id", "GET", "/admin/dlq/" + uuid.NewString(), "", 404},
		{"replay unknown", "POST", "/admin/dlq/" + uuid.NewString() + "/replay", "", 404},
		{"not replayable", "POST", "/admin/dlq/" + id + "/replay", "", 409},
		{"dismiss bad body", "POST", "/admin/dlq/" + id + "/dismiss", `{"nope":1}`, 400},
		{"dismiss ok", "POST", "/admin/dlq/" + id + "/dismiss", "", 200},
		{"dismiss again", "POST", "/admin/dlq/" + id + "/dismiss", "", 409},
	} {
		if code, body := call(t, c.method, srv.URL+c.path, auth, c.body); code != c.want {
			t.Errorf("%s: got %d, want %d (%s)", c.name, code, c.want, body)
		}
	}
}
