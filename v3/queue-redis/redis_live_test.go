package queueredis_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/queue"
	queueredis "github.com/rachmanzz/gosuite/v3/queue-redis"
	"github.com/redis/go-redis/v9"
)

func randSuffix(t *testing.T) string {
	t.Helper()
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("failed to generate random suffix: %v", err)
	}
	return hex.EncodeToString(b[:])
}

func newLiveMsg(topic string, prio queue.Priority) *queue.Message {
	return queue.NewMessage(topic, []byte(`{"live":true}`), queue.WithPriority(prio))
}

func assertOrder(t *testing.T, deliveries []*queue.Delivery, wantIDs []string) {
	t.Helper()
	for i, want := range wantIDs {
		if deliveries[i].Message.ID != want {
			t.Fatalf("delivery %d: expected message %s, got %s", i, want, deliveries[i].Message.ID)
		}
	}
}

// dequeueUntil polls the adapter until at least want deliveries are available
// or the timeout elapses, returning whatever was fetched on the last call.
func dequeueUntil(t *testing.T, ctx context.Context, adapter queue.Adapter, timeout time.Duration, want int) []*queue.Delivery {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var (
		out []*queue.Delivery
		err error
	)
	for {
		out, err = adapter.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
		if err != nil {
			t.Fatalf("dequeue failed: %v", err)
		}
		if len(out) >= want || time.Now().After(deadline) {
			return out
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestRedisDriver_Live exercises the full delivery loop against a real Redis
// instance: Enqueue -> priority Dequeue -> Ack -> Nack (redelivery) -> delayed EnqueueAt.
func TestRedisDriver_Live(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping live Redis integration test (run scripts/test-all-brokers.sh to enable)")
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("queue-redis: cannot reach live redis at %s: %v", addr, err)
	}

	prefix := "fiber:queue:live:" + randSuffix(t) + ":"
	dataKey := prefix + "messages_hash"
	driver := queueredis.New(client, queueredis.WithPrefix(prefix))

	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = client.Del(cctx, prefix+"priority_zset", prefix+"messages_hash", prefix+"delayed_zset", prefix+"inflight_zset", prefix+"inflight_scores", prefix+"inflight_claims").Err()
	})

	low := newLiveMsg("email.send", queue.PriorityLow)
	def := newLiveMsg("email.send", queue.PriorityDefault)
	high := newLiveMsg("email.send", queue.PriorityHigh)
	crit := newLiveMsg("email.send", queue.PriorityCritical)

	// Enqueue in ascending priority so ordering is only attributable to the broker.
	for _, m := range []*queue.Message{low, def, high, crit} {
		if err := driver.Enqueue(ctx, m); err != nil {
			t.Fatalf("queue-redis: enqueue %s failed: %v", m.ID, err)
		}
	}

	deliveries, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("queue-redis: dequeue failed: %v", err)
	}
	if len(deliveries) != 4 {
		t.Fatalf("queue-redis: expected 4 deliveries, got %d", len(deliveries))
	}
	assertOrder(t, deliveries, []string{crit.ID, high.ID, def.ID, low.ID})

	// Ack the critical message: its payload must be removed from the data hash.
	if err := driver.Ack(ctx, deliveries[0]); err != nil {
		t.Fatalf("queue-redis: ack failed: %v", err)
	}
	exists, err := client.HExists(ctx, dataKey, crit.ID).Result()
	if err != nil {
		t.Fatalf("queue-redis: hash existence probe failed: %v", err)
	}
	if exists {
		t.Fatal("queue-redis: acked message payload still present in hash")
	}

	// Nack the default message: it must be redelivered on the next Dequeue.
	if err := driver.Nack(ctx, deliveries[2], errors.New("transient failure")); err != nil {
		t.Fatalf("queue-redis: nack failed: %v", err)
	}

	redelivered := dequeueUntil(t, ctx, driver, 2*time.Second, 1)
	if len(redelivered) != 1 || redelivered[0].Message.ID != def.ID {
		t.Fatalf("queue-redis: expected redelivery of nack'd message %s, got %d delivery(ies)", def.ID, len(redelivered))
	}
	if err := driver.Ack(ctx, redelivered[0]); err != nil {
		t.Fatalf("queue-redis: ack after redelivery failed: %v", err)
	}

	// Delayed scheduling: the message must stay invisible until it matures.
	delayed := newLiveMsg("report.generate", queue.PriorityDefault)
	if err := driver.EnqueueAt(ctx, delayed, time.Now().Add(250*time.Millisecond)); err != nil {
		t.Fatalf("queue-redis: delayed enqueue failed: %v", err)
	}

	notYet, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("queue-redis: premature dequeue failed: %v", err)
	}
	for _, d := range notYet {
		if d.Message.ID == delayed.ID {
			t.Fatal("queue-redis: delayed message delivered before maturity")
		}
	}

	time.Sleep(400 * time.Millisecond)

	matured := dequeueUntil(t, ctx, driver, 2*time.Second, 1)
	found := false
	for _, d := range matured {
		if d.Message.ID == delayed.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("queue-redis: delayed message not delivered after maturity")
	}
	if err := driver.Ack(ctx, matured[0]); err != nil {
		t.Fatalf("queue-redis: ack of delayed message failed: %v", err)
	}
}

// TestRedisDriver_Live_PrioritySurvivesNack verifies that a nack'd message keeps
// its original priority class when it is restored into the priority ZSet.
func TestRedisDriver_Live_PrioritySurvivesNack(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping live Redis integration test (run scripts/test-all-brokers.sh to enable)")
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("queue-redis: cannot reach live redis at %s: %v", addr, err)
	}

	prefix := fmt.Sprintf("fiber:queue:live:%s:", randSuffix(t))
	driver := queueredis.New(client, queueredis.WithPrefix(prefix))

	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = client.Del(cctx, prefix+"priority_zset", prefix+"messages_hash", prefix+"delayed_zset", prefix+"inflight_zset", prefix+"inflight_scores", prefix+"inflight_claims").Err()
	})

	def := newLiveMsg("invoice.settle", queue.PriorityDefault)
	crit := newLiveMsg("invoice.settle", queue.PriorityCritical)

	if err := driver.Enqueue(ctx, def); err != nil {
		t.Fatalf("queue-redis: enqueue failed: %v", err)
	}

	batch, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1})
	if err != nil {
		t.Fatalf("queue-redis: dequeue failed: %v", err)
	}
	if len(batch) != 1 || batch[0].Message.ID != def.ID {
		t.Fatalf("queue-redis: expected message %s, got %d delivery(ies)", def.ID, len(batch))
	}

	if err := driver.Enqueue(ctx, crit); err != nil {
		t.Fatalf("queue-redis: enqueue of critical failed: %v", err)
	}

	if err := driver.Nack(ctx, batch[0], errors.New("transient failure")); err != nil {
		t.Fatalf("queue-redis: nack failed: %v", err)
	}

	round := dequeueUntil(t, ctx, driver, 2*time.Second, 2)
	if len(round) != 2 {
		t.Fatalf("queue-redis: expected 2 deliveries, got %d", len(round))
	}
	assertOrder(t, round, []string{crit.ID, def.ID})

	if err := driver.Ack(ctx, round[0]); err != nil {
		t.Fatalf("queue-redis: ack failed: %v", err)
	}
	if err := driver.Ack(ctx, round[1]); err != nil {
		t.Fatalf("queue-redis: ack failed: %v", err)
	}
}

// TestRedisDriver_Live_NackPreservesFIFOPosition guards M-R5: a nacked message
// must return to its original position (by CreatedAt) within its priority class,
// not be pushed to the back by a fresh time.Now() score.
func TestRedisDriver_Live_NackPreservesFIFOPosition(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping live Redis integration test (run scripts/test-all-brokers.sh to enable)")
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("queue-redis: cannot reach live redis at %s: %v", addr, err)
	}

	prefix := fmt.Sprintf("fiber:queue:live:%s:", randSuffix(t))
	driver := queueredis.New(client, queueredis.WithPrefix(prefix))

	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = client.Del(cctx, prefix+"priority_zset", prefix+"messages_hash", prefix+"delayed_zset", prefix+"inflight_zset", prefix+"inflight_scores", prefix+"inflight_claims").Err()
	})

	// The zset score ties FIFO to whole seconds (CreatedAt.Unix()), so separate
	// the two messages by well over a second to make the ordering deterministic.
	older := newLiveMsg("fifo.audit", queue.PriorityDefault)
	older.CreatedAt = time.Now().Add(-10 * time.Second)
	newer := newLiveMsg("fifo.audit", queue.PriorityDefault)
	newer.CreatedAt = time.Now()

	if err := driver.Enqueue(ctx, older); err != nil {
		t.Fatalf("queue-redis: enqueue older failed: %v", err)
	}
	first, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1})
	if err != nil || len(first) != 1 || first[0].Message.ID != older.ID {
		t.Fatalf("queue-redis: expected initial delivery of older, got %d (err=%v)", len(first), err)
	}

	if err := driver.Enqueue(ctx, newer); err != nil {
		t.Fatalf("queue-redis: enqueue newer failed: %v", err)
	}
	if err := driver.Nack(ctx, first[0], errors.New("transient")); err != nil {
		t.Fatalf("queue-redis: nack failed: %v", err)
	}

	round := dequeueUntil(t, ctx, driver, 2*time.Second, 2)
	if len(round) != 2 {
		t.Fatalf("queue-redis: expected 2 deliveries, got %d", len(round))
	}
	assertOrder(t, round, []string{older.ID, newer.ID})
}

// TestRedisDriver_Live_InFlightRedeliveredAfterLease verifies the at-least-once
// guarantee Redis lacks natively: a delivered message that is never acked stays
// in-flight and is redelivered once its visibility timeout (lease) expires. An
// acked message must not be redelivered.
func TestRedisDriver_Live_InFlightRedeliveredAfterLease(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping live Redis integration test (run scripts/test-all-brokers.sh to enable)")
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("queue-redis: cannot reach live redis at %s: %v", addr, err)
	}

	prefix := fmt.Sprintf("fiber:queue:live:%s:", randSuffix(t))
	driver := queueredis.New(client, queueredis.WithPrefix(prefix), queueredis.WithVisibilityTimeout(400*time.Millisecond))
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = client.Del(cctx, prefix+"priority_zset", prefix+"messages_hash", prefix+"delayed_zset", prefix+"inflight_zset", prefix+"inflight_scores", prefix+"inflight_claims").Err()
	})

	msg := newLiveMsg("lease.redeliver", queue.PriorityDefault)
	if err := driver.Enqueue(ctx, msg); err != nil {
		t.Fatalf("queue-redis: enqueue failed: %v", err)
	}

	// First delivery claims the message and holds a lease.
	first := dequeueUntil(t, ctx, driver, 2*time.Second, 1)
	if len(first) != 1 || first[0].Message.ID != msg.ID {
		t.Fatalf("queue-redis: expected first delivery of %s, got %d delivery(ies)", msg.ID, len(first))
	}

	// While the lease is active the message is invisible.
	if locked, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10}); err != nil {
		t.Fatalf("queue-redis: dequeue during lease failed: %v", err)
	} else if len(locked) != 0 {
		t.Fatalf("queue-redis: in-flight message redelivered before lease expiry: %d delivery(ies)", len(locked))
	}

	// After the lease expires the message must be redelivered.
	time.Sleep(600 * time.Millisecond)
	redelivered := dequeueUntil(t, ctx, driver, 2*time.Second, 1)
	if len(redelivered) != 1 || redelivered[0].Message.ID != msg.ID {
		t.Fatalf("queue-redis: expected redelivery of %s after lease expiry, got %d delivery(ies)", msg.ID, len(redelivered))
	}

	// Acking clears the in-flight entry permanently.
	if err := driver.Ack(ctx, redelivered[0]); err != nil {
		t.Fatalf("queue-redis: ack failed: %v", err)
	}
	time.Sleep(600 * time.Millisecond)
	if after, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10}); err != nil {
		t.Fatalf("queue-redis: dequeue after ack failed: %v", err)
	} else {
		for _, d := range after {
			if d.Message.ID == msg.ID {
				t.Fatalf("queue-redis: acked message %s was redelivered", msg.ID)
			}
		}
	}
	if n, err := client.ZCard(ctx, prefix+"inflight_zset").Result(); err != nil {
		t.Fatalf("queue-redis: inflight cardinality probe failed: %v", err)
	} else if n != 0 {
		t.Fatalf("queue-redis: expected empty inflight set after ack, got %d member(s)", n)
	}
}

// TestRedisDriver_Live_ReclaimPreservesPriority guards NEW-C2/M-R1: a message
// reclaimed after its lease expired must return to the ready ZSet at its
// original position, not downgraded to the default priority.
func TestRedisDriver_Live_ReclaimPreservesPriority(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping live Redis integration test (run scripts/test-all-brokers.sh to enable)")
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("queue-redis: cannot reach live redis at %s: %v", addr, err)
	}

	prefix := fmt.Sprintf("fiber:queue:live:%s:", randSuffix(t))
	driver := queueredis.New(client, queueredis.WithPrefix(prefix), queueredis.WithVisibilityTimeout(300*time.Millisecond))
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = client.Del(cctx, prefix+"priority_zset", prefix+"messages_hash", prefix+"delayed_zset", prefix+"inflight_zset", prefix+"inflight_scores", prefix+"inflight_claims").Err()
	})

	// Two HIGH messages one second apart so their ready scores (priority*1e9 -
	// created_at) differ deterministically; older must sort first.
	high1 := newLiveMsg("reclaim.order", queue.PriorityHigh)
	if err := driver.Enqueue(ctx, high1); err != nil {
		t.Fatalf("queue-redis: enqueue high1 failed: %v", err)
	}

	// Claim high1 and let its lease expire without acking.
	claimed := dequeueUntil(t, ctx, driver, 2*time.Second, 1)
	if len(claimed) != 1 || claimed[0].Message.ID != high1.ID {
		t.Fatalf("queue-redis: expected initial claim of %s, got %d", high1.ID, len(claimed))
	}

	time.Sleep(1100 * time.Millisecond)

	high2 := newLiveMsg("reclaim.order", queue.PriorityHigh)
	if err := driver.Enqueue(ctx, high2); err != nil {
		t.Fatalf("queue-redis: enqueue high2 failed: %v", err)
	}
	crit := newLiveMsg("reclaim.order", queue.PriorityCritical)
	if err := driver.Enqueue(ctx, crit); err != nil {
		t.Fatalf("queue-redis: enqueue crit failed: %v", err)
	}

	// Wait for high1's lease to lapse so the next Dequeue reclaims it.
	time.Sleep(350 * time.Millisecond)

	round := dequeueUntil(t, ctx, driver, 2*time.Second, 3)
	if len(round) != 3 {
		t.Fatalf("queue-redis: expected 3 deliveries, got %d", len(round))
	}
	// If the reclaim dropped high1's score, it would fall behind high2; the
	// preserved order proves its original priority survived the reclaim.
	assertOrder(t, round, []string{crit.ID, high1.ID, high2.ID})

	for _, d := range round {
		if err := driver.Ack(ctx, d); err != nil {
			t.Fatalf("queue-redis: ack failed: %v", err)
		}
	}
}

// TestRedisDriver_Live_UndecodablePayloadBecomesPoison verifies that a corrupt
// hash entry (or a missing one whose ZSet member was already popped) is not
// silently dropped: Dequeue surfaces a poison delivery (queue.PoisonTopic) so the
// consumer can route it to the DLQ.
func TestRedisDriver_Live_UndecodablePayloadBecomesPoison(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping live Redis integration test (run scripts/test-all-brokers.sh to enable)")
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("queue-redis: cannot reach live redis at %s: %v", addr, err)
	}

	prefix := fmt.Sprintf("fiber:queue:live:%s:", randSuffix(t))
	driver := queueredis.New(client, queueredis.WithPrefix(prefix))
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = client.Del(cctx, prefix+"priority_zset", prefix+"messages_hash", prefix+"delayed_zset", prefix+"inflight_zset", prefix+"inflight_scores", prefix+"inflight_claims").Err()
	})

	// Case 1a: v2 record present but its metadata field is corrupt.
	corrupt := newLiveMsg("audit.corrupt", queue.PriorityDefault)
	if err := driver.Enqueue(ctx, corrupt); err != nil {
		t.Fatalf("queue-redis: enqueue failed: %v", err)
	}
	if err := client.HSet(ctx, prefix+"messages_hash", corrupt.ID+":m", "this is not msgpack").Err(); err != nil {
		t.Fatalf("queue-redis: corrupting metadata field failed: %v", err)
	}

	batch, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("queue-redis: dequeue failed: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("queue-redis: expected 1 poison delivery, got %d", len(batch))
	}
	got := batch[0].Message
	if got == nil || got.Topic != queue.PoisonTopic {
		t.Fatalf("queue-redis: expected poison topic %q, got %+v", queue.PoisonTopic, got)
	}
	if got.ID != corrupt.ID {
		t.Fatalf("queue-redis: expected poison id %q, got %q", corrupt.ID, got.ID)
	}
	if got.Headers["decode_error"] == "" {
		t.Fatal("queue-redis: expected decode_error header on poison message")
	}
	if got.Headers["decode_adapter"] != driver.Name() {
		t.Fatalf("queue-redis: expected decode_adapter=%q, got %q", driver.Name(), got.Headers["decode_adapter"])
	}

	// Case 2: the split record vanished but the ZSet member remains.
	missing := newLiveMsg("audit.missing", queue.PriorityDefault)
	if err := driver.Enqueue(ctx, missing); err != nil {
		t.Fatalf("queue-redis: enqueue failed: %v", err)
	}
	if err := client.HDel(ctx, prefix+"messages_hash", missing.ID+":p", missing.ID+":m").Err(); err != nil {
		t.Fatalf("queue-redis: deleting hash entry failed: %v", err)
	}

	batch2 := dequeueUntil(t, ctx, driver, 2*time.Second, 1)
	if len(batch2) != 1 {
		t.Fatalf("queue-redis: expected 1 poison delivery for missing entry, got %d", len(batch2))
	}
	if batch2[0].Message == nil || batch2[0].Message.Topic != queue.PoisonTopic || batch2[0].Message.ID != missing.ID {
		t.Fatalf("queue-redis: missing-entry poison malformed: %+v", batch2[0].Message)
	}
}

// A legacy ID must not settle a newer worker's claim for that same message.
func TestRedisDriver_Live_LeaseLessTokenCannotModifyNewClaim(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	prefix := "fiber:queue:claim:" + randSuffix(t) + ":"
	keys := []string{prefix + "priority_zset", prefix + "messages_hash", prefix + "delayed_zset", prefix + "inflight_zset", prefix + "inflight_scores", prefix + "inflight_claims"}
	defer client.Del(context.Background(), keys...)
	oldDriver := queueredis.New(client, queueredis.WithPrefix(prefix), queueredis.WithVisibilityTimeout(100*time.Millisecond))
	currentDriver := queueredis.New(client, queueredis.WithPrefix(prefix), queueredis.WithVisibilityTimeout(10*time.Second))
	original := queue.NewMessage("claim.job", []byte("original payload"))
	if err := oldDriver.Enqueue(ctx, original); err != nil {
		t.Fatal(err)
	}
	old := dequeueUntil(t, ctx, oldDriver, time.Second, 1)
	if len(old) != 1 {
		t.Fatal("initial claim missing")
	}
	time.Sleep(150 * time.Millisecond)
	current := dequeueUntil(t, ctx, currentDriver, time.Second, 1)
	if len(current) != 1 || current[0].Message.ID != original.ID {
		t.Fatal("new worker did not reclaim message")
	}
	snapshot := func() []string {
		t.Helper()
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
	changed := *original
	changed.Payload = []byte("must not replace payload")
	changed.Attempt = 7
	for _, token := range []struct {
		name  string
		value any
		stale bool
	}{
		{"legacy ID", original.ID, false}, {"stale lease", old[0].Token, true},
	} {
		d := &queue.Delivery{Message: original, Token: token.value}
		for _, op := range []struct {
			name string
			run  func() error
		}{
			{"Ack", func() error { return oldDriver.Ack(ctx, d) }},
			{"Nack", func() error { return oldDriver.Nack(ctx, d, errors.New("old worker")) }},
			{"Reenqueue", func() error { return oldDriver.Reenqueue(ctx, d, &changed, time.Now().Add(time.Minute)) }},
		} {
			t.Run(token.name+"/"+op.name, func(t *testing.T) {
				err := op.run()
				if err == nil {
					t.Fatal("unauthorized claim accepted")
				}
				if token.stale && !errors.Is(err, queueredis.ErrDeliveryLost) {
					t.Fatalf("stale claim error: %v", err)
				}
				after := snapshot()
				for i := range keys {
					if before[i] != after[i] {
						t.Fatalf("claim/payload/index modified: %s", keys[i])
					}
				}
			})
		}
	}
	// The current worker still owns its token and can complete normal settlement.
	if err := currentDriver.Nack(ctx, current[0], nil); err != nil {
		t.Fatal(err)
	}
	next := dequeueUntil(t, ctx, currentDriver, time.Second, 1)
	if len(next) != 1 {
		t.Fatal("valid Nack did not requeue")
	}
	retry := *next[0].Message
	retry.Attempt = 1
	if err := currentDriver.Reenqueue(ctx, next[0], &retry, time.Now().Add(30*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	ready := dequeueUntil(t, ctx, currentDriver, time.Second, 1)
	if len(ready) != 1 || ready[0].Message.Attempt != 1 || string(ready[0].Message.Payload) != string(original.Payload) {
		t.Fatal("valid retry corrupted")
	}
	if err := currentDriver.Ack(ctx, ready[0]); err != nil {
		t.Fatal(err)
	}
}
