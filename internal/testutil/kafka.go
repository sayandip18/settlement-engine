package testutil

import (
	"context"
	"sync"
	"testing"

	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

var (
	kafkaOnce    sync.Once
	kafkaBrokers []string
	kafkaCtr     *tckafka.KafkaContainer
	kafkaErr     error
)

// NewKafka returns the broker addresses of a Kafka container shared by the whole
// test binary (started on first use, stopped by Run). Tests sharing it should use
// their own unique topic and consumer-group names.
func NewKafka(t *testing.T) []string {
	t.Helper()
	kafkaOnce.Do(func() {
		ctx := context.Background()
		c, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0", tckafka.WithClusterID("test-cluster"))
		if err != nil {
			kafkaErr = err
			return
		}
		kafkaCtr = c
		kafkaBrokers, kafkaErr = c.Brokers(ctx)
	})
	if kafkaErr != nil {
		t.Skipf("kafka test container unavailable (is Docker running?): %v", kafkaErr)
	}
	return kafkaBrokers
}
