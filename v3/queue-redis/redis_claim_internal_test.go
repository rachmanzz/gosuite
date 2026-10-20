package queueredis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/queue"
	"github.com/redis/go-redis/v9"
)

func TestSettlementRejectsClaimsWithoutLeaseOwnershipBeforeRedisIO(t *testing.T) {
	tokens := map[string]any{
		"missing acquisition ID":  deliveryClaim{id: "message-id", lease: 123},
		"legacy ID":               "message-id",
		"empty legacy ID":         "",
		"nil pointer":             (*deliveryClaim)(nil),
		"zero lease":              deliveryClaim{id: "message-id", owner: "claim"},
		"negative lease":          deliveryClaim{id: "message-id", lease: -1, owner: "claim"},
		"empty ID":                deliveryClaim{lease: 123, owner: "claim"},
		"NaN lease":               deliveryClaim{id: "message-id", owner: "claim", lease: math.NaN()},
		"infinite lease":          deliveryClaim{id: "message-id", owner: "claim", lease: math.Inf(1)},
		"negative infinite lease": deliveryClaim{id: "message-id", owner: "claim", lease: math.Inf(-1)},
		"zero lease pointer":      &deliveryClaim{id: "message-id", owner: "claim"},
	}
	// No client: reaching Redis I/O instead of rejecting the token would panic.
	driver := New(nil)
	for name, token := range tokens {
		t.Run(name, func(t *testing.T) {
			msg := queue.NewMessage("job", nil)
			d := &queue.Delivery{Message: msg, Token: token}
			operations := map[string]func() error{
				"Ack":       func() error { return driver.Ack(context.Background(), d) },
				"Nack":      func() error { return driver.Nack(context.Background(), d, nil) },
				"Reenqueue": func() error { return driver.Reenqueue(context.Background(), d, msg, time.Now().Add(time.Second)) },
			}
			for op, run := range operations {
				t.Run(op, func(t *testing.T) {
					if err := run(); err == nil {
						t.Fatal("invalid claim accepted")
					}
				})
			}
		})
	}
}

func TestParseDeliveryClaimAcceptsOpaqueLeaseClaims(t *testing.T) {
	claim := deliveryClaim{id: "message-id", lease: 123.456789, owner: "unique-acquisition"}
	for _, tok := range []any{claim, &claim} {
		id, owner, err := parseDeliveryClaim(tok)
		if err != nil || id != claim.id || owner != claim.owner {
			t.Fatalf("valid claim changed: id=%q owner=%q err=%v", id, owner, err)
		}
	}
}

// Expiry timestamps may be identical or very close across different claims.
// Ownership must still be fenced by the unique acquisition identity.
func TestRedisLiveConsumedClaimCannotSettleNewOwnerWithMatchingDeadline(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, delta := range []float64{0, 0.0005} {
		for _, op := range []string{"Ack", "Nack", "Reenqueue"} {
			t.Run(fmt.Sprintf("delta=%g/%s", delta, op), func(t *testing.T) {
				d := New(client, WithPrefix("test:claim:"+queue.NewUUID()+":"))
				keys := []string{d.queueKey(), d.dataKey(), d.delayedKey(), d.inflightKey(), d.inflightScoreKey(), d.inflightClaimKey()}
				defer client.Del(context.Background(), keys...)
				m := queue.NewMessage("job", []byte("original"))
				if err := d.Enqueue(ctx, m); err != nil {
					t.Fatal(err)
				}
				first, err := d.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1})
				if err != nil || len(first) != 1 {
					t.Fatalf("first claim: %v", err)
				}
				if err := d.Nack(ctx, first[0], nil); err != nil {
					t.Fatal(err)
				}
				if n, err := client.HLen(ctx, d.inflightClaimKey()).Result(); err != nil || n != 0 {
					t.Fatalf("Nack leaked ownership: count=%d err=%v", n, err)
				}
				second, err := d.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1})
				if err != nil || len(second) != 1 {
					t.Fatalf("second claim: %v", err)
				}
				old := first[0].Token.(deliveryClaim)
				current := second[0].Token.(deliveryClaim)
				if old.owner == current.owner || current.owner == "" {
					t.Fatal("acquisition identity reused")
				}
				// Force equal/near deadlines deterministically; both claims themselves
				// were acquired through the real driver and have distinct ownership IDs.
				current.lease = old.lease + delta
				second[0].Token = current
				if err := client.ZAdd(ctx, d.inflightKey(), redis.Z{Score: current.lease, Member: m.ID}).Err(); err != nil {
					t.Fatal(err)
				}
				snapshot := func() []string {
					out := make([]string, len(keys))
					for i, key := range keys {
						v, err := client.Dump(ctx, key).Result()
						if err != nil && err != redis.Nil {
							t.Fatal(err)
						}
						out[i] = v
					}
					return out
				}
				before := snapshot()
				switch op {
				case "Ack":
					err = d.Ack(ctx, first[0])
				case "Nack":
					err = d.Nack(ctx, first[0], nil)
				case "Reenqueue":
					retry := *m
					retry.Payload = []byte("must not overwrite")
					retry.Attempt = 1
					err = d.Reenqueue(ctx, first[0], &retry, time.Now().Add(time.Minute))
				}
				if !errors.Is(err, ErrDeliveryLost) {
					t.Fatalf("consumed %s token accepted: %v", op, err)
				}
				after := snapshot()
				for i, key := range keys {
					if before[i] != after[i] {
						t.Fatalf("stale settlement modified %s", key)
					}
				}
				if err := d.Ack(ctx, second[0]); err != nil {
					t.Fatalf("new owner cannot Ack: %v", err)
				}
				if n, err := client.Exists(ctx, keys...).Result(); err != nil || n != 0 {
					t.Fatalf("settlement leaked state: count=%d err=%v", n, err)
				}
			})
		}
	}
}
