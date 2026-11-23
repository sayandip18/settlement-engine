// Package kafka adapts segmentio/kafka-go to the outbox publisher and a
// commit-after-handle consumer loop.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	kg "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/sayandip18/settlement-engine/internal/obs"
	"github.com/sayandip18/settlement-engine/internal/outbox"
)

// Publisher implements outbox.Publisher.
type Publisher struct{ w *kg.Writer }

func NewPublisher(brokers []string) *Publisher {
	return &Publisher{w: &kg.Writer{
		Addr:         kg.TCP(brokers...),
		Balancer:     &kg.Hash{}, // same key -> same partition -> ordered
		RequiredAcks: kg.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}}
}

func (p *Publisher) Publish(ctx context.Context, msgs []outbox.Message) error {
	out := make([]kg.Message, len(msgs))
	for i, m := range msgs {
		out[i] = kg.Message{Topic: m.Topic, Key: []byte(m.Key), Value: m.Payload}
		for k, v := range m.Headers {
			out[i].Headers = append(out[i].Headers, kg.Header{Key: k, Value: []byte(v)})
		}
	}
	// A topic created moments ago can briefly answer "unknown topic or partition" to
	// produce requests even though its metadata is visible (the broker is still
	// applying the leader assignment). That is transient, so retry it for a short
	// while; every other error is returned for the caller (the outbox relay) to handle.
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := p.w.WriteMessages(ctx, out...)
		if err == nil || !isUnknownTopic(err) || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func isUnknownTopic(err error) bool {
	if errors.Is(err, kg.UnknownTopicOrPartition) {
		return true
	}
	var we kg.WriteErrors
	if errors.As(err, &we) {
		for _, e := range we {
			if errors.Is(e, kg.UnknownTopicOrPartition) {
				return true
			}
		}
	}
	return false
}

func (p *Publisher) Close() error { return p.w.Close() }

// Topic describes a topic to create if it does not exist.
type Topic struct {
	Name       string
	Partitions int
}

// EnsureTopics creates any missing topics. It is safe to call from every process.
func EnsureTopics(ctx context.Context, brokers []string, topics ...Topic) error {
	d := &kg.Dialer{Timeout: 5 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("find kafka controller: %w", err)
	}
	cconn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dial kafka controller: %w", err)
	}
	defer cconn.Close()

	for _, t := range topics {
		err := cconn.CreateTopics(kg.TopicConfig{Topic: t.Name, NumPartitions: t.Partitions, ReplicationFactor: 1})
		if err != nil && !errors.Is(err, kg.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", t.Name, err)
		}
	}
	// CreateTopics returns before partition leaders are elected; publishing straight
	// away would fail with "unknown topic or partition". Wait until each is usable.
	for _, t := range topics {
		if err := waitReady(ctx, conn, t); err != nil {
			return err
		}
	}
	return nil
}

func waitReady(ctx context.Context, conn *kg.Conn, t Topic) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		parts, err := conn.ReadPartitions(t.Name)
		if err == nil && len(parts) >= t.Partitions {
			ready := true
			for _, p := range parts {
				if p.Leader.ID < 0 || p.Leader.Host == "" {
					ready = false
				}
			}
			if ready {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("topic %s not ready after 30s (last error: %v)", t.Name, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Check verifies the broker answers Kafka protocol requests.
func Check(brokers []string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		conn, err := (&kg.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "tcp", brokers[0])
		if err != nil {
			return err
		}
		defer conn.Close()
		_, err = conn.Controller()
		return err
	}
}

// Message is a consumed record.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       string
	Value     []byte
	Headers   map[string]string
}

// Consumer reads one topic as part of a consumer group.
type Consumer struct {
	r   *kg.Reader
	log *slog.Logger
}

func NewConsumer(brokers []string, group, topic string, log *slog.Logger) *Consumer {
	return &Consumer{log: log, r: kg.NewReader(kg.ReaderConfig{
		Brokers:     brokers,
		GroupID:     group,
		Topic:       topic,
		MinBytes:    1,
		MaxBytes:    10 << 20,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kg.FirstOffset, // a brand-new group reads from the beginning
	})}
}

func (c *Consumer) Close() error { return c.r.Close() }

// Run delivers messages to handle one at a time and commits an offset only after
// handle returns nil, so a message is never lost (it may be redelivered, so handle
// must be idempotent). While handle keeps failing, the same message is retried
// after a short pause; this blocks its partition rather than skipping the message.
func (c *Consumer) Run(ctx context.Context, handle func(ctx context.Context, m Message) error) error {
	for {
		km, err := c.r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch: %w", err)
		}
		m := Message{Topic: km.Topic, Partition: km.Partition, Offset: km.Offset, Key: string(km.Key), Value: km.Value}
		if len(km.Headers) > 0 {
			m.Headers = make(map[string]string, len(km.Headers))
			for _, h := range km.Headers {
				m.Headers[h.Key] = string(h.Value)
			}
		}
		// The consume span continues the trace the publisher injected into the headers and
		// covers every delivery attempt of this message.
		hctx, span := obs.Tracer().Start(obs.Extract(ctx, m.Headers), "kafka consume "+m.Topic,
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(attribute.String("messaging.destination.name", m.Topic),
				attribute.Int("messaging.destination.partition.id", m.Partition),
				attribute.Int64("messaging.kafka.offset", m.Offset)))
		for {
			err := handle(hctx, m)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				span.End()
				return nil
			}
			obs.KafkaHandlerErrors.WithLabelValues(m.Topic).Inc()
			span.RecordError(err)
			span.SetStatus(codes.Error, "handler failed")
			c.log.Error("handler failed, will retry message", "topic", m.Topic,
				"partition", m.Partition, "offset", m.Offset, "err", err, "trace_id", obs.TraceID(hctx))
			select {
			case <-ctx.Done():
				span.End()
				return nil
			case <-time.After(time.Second):
			}
		}
		span.End()
		if err := c.r.CommitMessages(ctx, km); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("commit: %w", err)
		}
	}
}
