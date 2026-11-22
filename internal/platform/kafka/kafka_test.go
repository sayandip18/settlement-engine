package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sayandip18/settlement-engine/internal/outbox"
	"github.com/sayandip18/settlement-engine/internal/platform/kafka"
	"github.com/sayandip18/settlement-engine/internal/testutil"
)

func TestMain(m *testing.M) { os.Exit(testutil.Run(m)) }

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// collector records handled messages and can fail chosen ones.
type collector struct {
	mu       sync.Mutex
	handled  []string // every handler invocation, including failed ones
	done     []string // successful ones
	failOnce map[string]bool
}

func (c *collector) handle(_ context.Context, m kafka.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := string(m.Value)
	c.handled = append(c.handled, v)
	if c.failOnce[v] {
		delete(c.failOnce, v)
		return errors.New("simulated handler failure")
	}
	c.done = append(c.done, v)
	return nil
}

func (c *collector) doneCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.done)
}

func setup(t *testing.T, partitions int) (brokers []string, topic string, pub *kafka.Publisher) {
	t.Helper()
	brokers = testutil.NewKafka(t)
	topic = "t-" + uuid.NewString()[:8]
	if err := kafka.EnsureTopics(context.Background(), brokers, kafka.Topic{Name: topic, Partitions: partitions}); err != nil {
		t.Fatal(err)
	}
	pub = kafka.NewPublisher(brokers)
	t.Cleanup(func() { pub.Close() })
	return
}

func run(t *testing.T, brokers []string, group, topic string, c *collector) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	consumer := kafka.NewConsumer(brokers, group, topic, quiet)
	done := make(chan struct{})
	go func() { defer close(done); consumer.Run(ctx, c.handle) }()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("consumer did not stop")
		}
		consumer.Close()
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func send(t *testing.T, pub *kafka.Publisher, topic, key string, payloads ...string) {
	t.Helper()
	var msgs []outbox.Message
	for _, p := range payloads {
		msgs = append(msgs, outbox.Message{Topic: topic, Key: key, Payload: []byte(p)})
	}
	if err := pub.Publish(context.Background(), msgs); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureTopicsIsIdempotent(t *testing.T) {
	brokers := testutil.NewKafka(t)
	topic := kafka.Topic{Name: "idem-" + uuid.NewString()[:8], Partitions: 2}
	for i := 0; i < 3; i++ {
		if err := kafka.EnsureTopics(context.Background(), brokers, topic); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if err := kafka.Check(brokers)(context.Background()); err != nil {
		t.Fatalf("Check: %v", err)
	}
}

func TestMessagesWithSameKeyArriveInOrder(t *testing.T) {
	brokers, topic, pub := setup(t, 3)
	var want []string
	for i := 0; i < 30; i++ {
		want = append(want, fmt.Sprintf("m%02d", i))
	}
	send(t, pub, topic, "payment-1", want...)

	c := &collector{}
	stop := run(t, brokers, "g-"+uuid.NewString()[:8], topic, c)
	defer stop()
	waitFor(t, "30 messages", func() bool { return c.doneCount() == 30 })

	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range want {
		if c.done[i] != want[i] {
			t.Fatalf("message %d = %s, want %s: ordering broken within a key", i, c.done[i], want[i])
		}
	}
}

func TestFailingHandlerRetriesSameMessageWithoutSkipping(t *testing.T) {
	brokers, topic, pub := setup(t, 1)
	send(t, pub, topic, "k", "a", "b", "c")

	c := &collector{failOnce: map[string]bool{"b": true}}
	stop := run(t, brokers, "g-"+uuid.NewString()[:8], topic, c)
	defer stop()
	waitFor(t, "all three handled", func() bool { return c.doneCount() == 3 })

	c.mu.Lock()
	defer c.mu.Unlock()
	if fmt.Sprint(c.handled) != "[a b b c]" { // b failed once, was retried, c waited behind it
		t.Fatalf("handler invocations: %v", c.handled)
	}
	if fmt.Sprint(c.done) != "[a b c]" {
		t.Fatalf("successful: %v", c.done)
	}
}

func TestCommittedOffsetsSurviveRestart(t *testing.T) {
	brokers, topic, pub := setup(t, 1)
	group := "g-" + uuid.NewString()[:8]
	send(t, pub, topic, "k", "one", "two", "three")

	first := &collector{}
	stop := run(t, brokers, group, topic, first)
	waitFor(t, "first run", func() bool { return first.doneCount() == 3 })
	stop()

	// A new consumer in the same group resumes after the committed offset: it sees
	// only what was published since, never the three it already handled.
	send(t, pub, topic, "k", "four")
	second := &collector{}
	stop2 := run(t, brokers, group, topic, second)
	defer stop2()
	waitFor(t, "second run", func() bool { return second.doneCount() >= 1 })
	time.Sleep(time.Second) // give any wrongly-redelivered messages time to show up

	second.mu.Lock()
	defer second.mu.Unlock()
	if fmt.Sprint(second.done) != "[four]" {
		t.Fatalf("restarted consumer handled %v, want only [four]", second.done)
	}
}

func TestUnhandledMessageIsRedeliveredAfterCrash(t *testing.T) {
	brokers, topic, pub := setup(t, 1)
	group := "g-" + uuid.NewString()[:8]
	send(t, pub, topic, "k", "x")

	// First consumer fails, then is shut down: the offset was never committed.
	ctx, cancel := context.WithCancel(context.Background())
	consumer := kafka.NewConsumer(brokers, group, topic, quiet)
	done := make(chan struct{})
	attempted := make(chan struct{}, 1)
	go func() {
		defer close(done)
		consumer.Run(ctx, func(ctx context.Context, m kafka.Message) error {
			select {
			case attempted <- struct{}{}:
			default:
			}
			return errors.New("crash before finishing")
		})
	}()
	select {
	case <-attempted:
	case <-time.After(45 * time.Second):
		t.Fatal("handler never invoked")
	}
	cancel()
	<-done
	consumer.Close()

	// A fresh consumer in the same group gets the message again.
	second := &collector{}
	stop := run(t, brokers, group, topic, second)
	defer stop()
	waitFor(t, "redelivery", func() bool { return second.doneCount() == 1 })
}
