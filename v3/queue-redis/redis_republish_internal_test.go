package queueredis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/queue"
	"github.com/redis/go-redis/v9"
)

func newRepublishTestDriver(t *testing.T) (*RedisDriver, *redis.Client, context.Context, []string) {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	d := New(client, WithPrefix("test:republish:"+queue.NewUUID()+":"))
	keys := []string{d.queueKey(), d.dataKey(), d.delayedKey(), d.inflightKey(), d.inflightScoreKey(), d.inflightClaimKey()}
	t.Cleanup(func() { client.Del(context.Background(), keys...); client.Close(); cancel() })
	return d, client, ctx, keys
}

func TestRedisLiveRepublicationFencesOldSettlement(t *testing.T) {
	for _, delayed := range []bool{false, true} {
		for _, operation := range []string{"Ack", "Nack", "Reenqueue"} {
			t.Run(fmt.Sprintf("delayed=%t/%s", delayed, operation), func(t *testing.T) {
				d, client, ctx, keys := newRepublishTestDriver(t)
				m := queue.NewMessage("job", []byte("old"))
				if err := d.Enqueue(ctx, m); err != nil {
					t.Fatal(err)
				}
				old, err := d.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1})
				if err != nil || len(old) != 1 {
					t.Fatalf("old claim: %v", err)
				}
				replacement := *m
				replacement.Payload = []byte("latest")
				replacement.Attempt = 2
				due := time.Now().Add(40 * time.Millisecond)
				if delayed {
					err = d.EnqueueAt(ctx, &replacement, due)
				} else {
					err = d.Enqueue(ctx, &replacement)
				}
				if err != nil {
					t.Fatal(err)
				}
				snapshot := func() []string {
					out := make([]string, len(keys))
					for i, k := range keys {
						v, err := client.Dump(ctx, k).Result()
						if err != nil && err != redis.Nil {
							t.Fatal(err)
						}
						out[i] = v
					}
					return out
				}
				staleSettlement := func() {
					t.Helper()
					before := snapshot()
					switch operation {
					case "Ack":
						err = d.Ack(ctx, old[0])
					case "Nack":
						err = d.Nack(ctx, old[0], nil)
					case "Reenqueue":
						err = d.Reenqueue(ctx, old[0], m, time.Now().Add(time.Hour))
					}
					if !errors.Is(err, ErrDeliveryLost) {
						t.Fatalf("old %s accepted: %v", operation, err)
					}
					if !reflect.DeepEqual(before, snapshot()) {
						t.Fatal("old settlement changed replacement state")
					}
				}
				staleSettlement()
				if n, err := client.Exists(ctx, d.inflightKey(), d.inflightScoreKey(), d.inflightClaimKey()).Result(); err != nil || n != 0 {
					t.Fatalf("old claim state retained: %d %v", n, err)
				}
				if delayed {
					early, err := d.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1})
					if err != nil || len(early) != 0 {
						t.Fatal("replacement delivered early")
					}
					time.Sleep(time.Until(due) + time.Millisecond)
				}
				current, err := d.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1})
				if err != nil || len(current) != 1 {
					t.Fatalf("replacement missing: %v", err)
				}
				if string(current[0].Message.Payload) != "latest" || current[0].Message.Attempt != 2 {
					t.Fatalf("replacement corrupted: %+v", current[0].Message)
				}
				staleSettlement()
				if err := d.Ack(ctx, current[0]); err != nil {
					t.Fatal(err)
				}
				if n, err := client.Exists(ctx, keys...).Result(); err != nil || n != 0 {
					t.Fatalf("orphan state: %d %v", n, err)
				}
			})
		}
	}
}

func TestRedisLiveReplacementKeepsOnlyLatestSchedule(t *testing.T) {
	for _, initialDelayed := range []bool{false, true} {
		for _, replacementDelayed := range []bool{false, true} {
			t.Run(fmt.Sprintf("fromDelayed=%t/toDelayed=%t", initialDelayed, replacementDelayed), func(t *testing.T) {
				d, client, ctx, keys := newRepublishTestDriver(t)
				m := queue.NewMessage("job", []byte("old"))
				var err error
				if initialDelayed {
					err = d.EnqueueAt(ctx, m, time.Now().Add(-time.Second))
				} else {
					err = d.Enqueue(ctx, m)
				}
				if err != nil {
					t.Fatal(err)
				}
				replacement := *m
				replacement.Payload = []byte("latest")
				due := time.Now().Add(40 * time.Millisecond)
				if replacementDelayed {
					err = d.EnqueueAt(ctx, &replacement, due)
				} else {
					err = d.Enqueue(ctx, &replacement)
				}
				if err != nil {
					t.Fatal(err)
				}
				ready, _ := client.ZCard(ctx, d.queueKey()).Result()
				delayed, _ := client.ZCard(ctx, d.delayedKey()).Result()
				if replacementDelayed {
					if ready != 0 || delayed != 1 {
						t.Fatal("ready schedule survived delayed replacement")
					}
					time.Sleep(time.Until(due) + time.Millisecond)
				} else if ready != 1 || delayed != 0 {
					t.Fatal("delayed schedule survived ready replacement")
				}
				ds, err := d.Dequeue(ctx, queue.DequeueRequest{BatchSize: 2})
				if err != nil || len(ds) != 1 || string(ds[0].Message.Payload) != "latest" {
					t.Fatalf("latest schedule failed: %v", err)
				}
				if err := d.Ack(ctx, ds[0]); err != nil {
					t.Fatal(err)
				}
				if n, err := client.Exists(ctx, keys...).Result(); err != nil || n != 0 {
					t.Fatalf("duplicate/orphan schedule: %d %v", n, err)
				}
			})
		}
	}
}

func TestRedisLiveReplacementSurvivesConcurrentOriginalAck(t *testing.T) {
	d, _, ctx, _ := newRepublishTestDriver(t)
	for i := 0; i < 25; i++ {
		m := queue.NewMessage("job", []byte("old"))
		if err := d.Enqueue(ctx, m); err != nil {
			t.Fatal(err)
		}
		old, err := d.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1})
		if err != nil || len(old) != 1 {
			t.Fatalf("initial claim: %v", err)
		}
		latest := *m
		latest.Payload = []byte("latest")
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var publishErr, ackErr error
		go func() { defer wg.Done(); <-start; publishErr = d.Enqueue(ctx, &latest) }()
		go func() { defer wg.Done(); <-start; ackErr = d.Ack(ctx, old[0]) }()
		close(start)
		wg.Wait()
		if publishErr != nil || (ackErr != nil && !errors.Is(ackErr, ErrDeliveryLost)) {
			t.Fatalf("race errors: publish=%v ack=%v", publishErr, ackErr)
		}
		ds, err := d.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1})
		if err != nil || len(ds) != 1 || string(ds[0].Message.Payload) != "latest" {
			t.Fatalf("race lost replacement: %v", err)
		}
		if err := d.Ack(ctx, ds[0]); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRedisLiveStaleMigrationCannotMoveReplacement(t *testing.T) {
	for _, future := range []bool{false, true} {
		t.Run(fmt.Sprintf("future=%t", future), func(t *testing.T) {
			d, client, ctx, _ := newRepublishTestDriver(t)
			m := queue.NewMessage("job", []byte("old"), queue.WithPriority(queue.PriorityHigh))
			if err := d.EnqueueAt(ctx, m, time.Now().Add(-time.Second)); err != nil {
				t.Fatal(err)
			}
			oldMeta, err := client.HGet(ctx, d.dataKey(), metaField(m.ID)).Bytes()
			if err != nil {
				t.Fatal(err)
			}
			latest := *m
			latest.Payload = []byte("latest")
			due := time.Now().Add(-time.Second)
			if future {
				due = time.Now().Add(time.Hour)
			} else {
				latest.Priority = queue.PriorityLow
			}
			if err := d.EnqueueAt(ctx, &latest, due); err != nil {
				t.Fatal(err)
			}
			// Simulate a poller resuming a migration snapshot taken before replacement.
			moved, err := migrateDelayedScript.Run(ctx, client, []string{d.delayedKey(), d.queueKey(), d.dataKey()}, fmt.Sprintf("%f", float64(time.Now().UnixNano())/1e9), m.ID, 123, oldMeta).Slice()
			if err != nil || len(moved) != 0 {
				t.Fatalf("stale migration moved replacement: %v %v", moved, err)
			}
			if n, err := client.ZCard(ctx, d.queueKey()).Result(); err != nil || n != 0 {
				t.Fatal("stale snapshot inserted ready entry")
			}
			if !future {
				ds, err := d.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1})
				if err != nil || len(ds) != 1 || ds[0].Message.Priority != queue.PriorityLow {
					t.Fatalf("fresh migration failed: %v", err)
				}
				if err := d.Ack(ctx, ds[0]); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
