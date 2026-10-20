package queueredis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/queue"

	queueredis "github.com/rachmanzz/gosuite/v3/queue-redis"
	"github.com/redis/go-redis/v9"
)

// Ensure RedisDriver satisfies queue.Adapter and queue.DelayedEnqueuer contracts at compile time.
var (
	_ queue.Adapter         = (*queueredis.RedisDriver)(nil)
	_ queue.DelayedEnqueuer = (*queueredis.RedisDriver)(nil)
)

func TestRedisDriver_ContractAndCapabilities(t *testing.T) {
	// Dummy client with no actual network calls
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()

	driver := queueredis.New(client, queueredis.WithPrefix("custom:prefix:"))

	if driver.Name() != "redis" {
		t.Fatalf("expected name 'redis', got %s", driver.Name())
	}

	caps := driver.Capabilities()
	if !caps.DelayedEnqueue {
		t.Fatal("expected DelayedEnqueue to be true")
	}
	if caps.NativePriority != queue.PrioritySoft {
		t.Fatalf("expected PrioritySoft, got %v", caps.NativePriority)
	}

	// B4: Redis dedups by message ID via last-write-wins, so Dedup is genuinely true.
	if !caps.Dedup {
		t.Fatal("expected Redis Dedup capability to be true (per-ID last-write-wins)")
	}

	// KON-3: the inflight lease redelivers unacked messages, so the driver is
	// crash-persistent and the capability must agree with Guarantee: AtLeastOnce.
	if !caps.Persistent {
		t.Fatal("expected Redis Persistent capability to be true (inflight lease redelivers on crash)")
	}
	if caps.Guarantee != queue.AtLeastOnce {
		t.Fatalf("expected AtLeastOnce guarantee, got %v", caps.Guarantee)
	}

	// Test Ack and Nack with nil tokens do not error
	ctx := context.Background()
	if err := driver.Ack(ctx, nil); err != nil {
		t.Fatalf("expected nil on nil delivery ack: %v", err)
	}
	if err := driver.Nack(ctx, nil, nil); err != nil {
		t.Fatalf("expected nil on nil delivery nack: %v", err)
	}

	if err := driver.Close(); err != nil {
		t.Fatalf("close returned error: %v", err)
	}
}

func TestRedisDriver_NewAdapterAlias(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()

	adapter := queueredis.NewRedisAdapter(client, "test:")
	if adapter == nil {
		t.Fatal("expected non-nil adapter")
	}
	if adapter.Name() != "redis" {
		t.Fatalf("expected name 'redis', got %s", adapter.Name())
	}
}

func TestRedisDriver_CapabilityConsistency(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()

	driver := queueredis.New(client)

	_, replayer := any(driver).(queue.Replayer)
	_, delayed := any(driver).(queue.DelayedEnqueuer)
	caps := driver.Capabilities()

	if caps.Replay != replayer {
		t.Fatalf("Replay capability (%v) disagrees with Replayer implementation (%v)", caps.Replay, replayer)
	}
	if caps.DelayedEnqueue != delayed {
		t.Fatalf("DelayedEnqueue capability (%v) disagrees with DelayedEnqueuer implementation (%v)", caps.DelayedEnqueue, delayed)
	}
}

func TestRedisDriver_NilMessageRejected(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()

	driver := queueredis.New(client)
	ctx := context.Background()

	if err := driver.Enqueue(ctx, nil); err == nil || err.Error() != queueredis.ErrNilMessage.Error() {
		t.Fatalf("expected ErrNilMessage on Enqueue(nil), got %v", err)
	}
	if err := driver.EnqueueAt(ctx, nil, time.Now()); err == nil || err.Error() != queueredis.ErrNilMessage.Error() {
		t.Fatalf("expected ErrNilMessage on EnqueueAt(nil), got %v", err)
	}
}

func TestRedisDriver_ErrDeliveryLostExported(t *testing.T) {
	if queueredis.ErrDeliveryLost == nil {
		t.Fatal("expected ErrDeliveryLost to be non-nil")
	}
	if !errors.Is(queueredis.ErrDeliveryLost, queue.ErrDeliveryLost) {
		t.Fatalf("expected queueredis.ErrDeliveryLost to alias queue.ErrDeliveryLost")
	}
}
