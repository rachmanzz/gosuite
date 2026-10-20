//go:build stress

package queueredis_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/queue"
	queueredis "github.com/rachmanzz/gosuite/v3/queue-redis"
	"github.com/redis/go-redis/v9"
)

// requireStressEnv returns a required broker environment variable. When
// STRESS_REQUIRE_LIVE is set a missing broker is a hard failure (so a CI job
// that claims to run live tests cannot silently pass via t.Skip); otherwise a
// missing broker skips the test to keep local runs dependency-free.
func requireStressEnv(t *testing.T, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		if os.Getenv("STRESS_REQUIRE_LIVE") != "" {
			t.Fatalf("%s must be set because STRESS_REQUIRE_LIVE is enabled", name)
		}
		t.Skipf("%s not set; skipping live stress test", name)
	}
	return v
}

// stressErr records the first background error so a failure surfaces
// deterministically after all worker goroutines have been joined.
type stressErr struct {
	mu  sync.Mutex
	err error
}

func (s *stressErr) add(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
}

func (s *stressErr) fatalIfAny(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		t.Fatalf("background stress error: %v", s.err)
	}
}

// assertExactlyOnceSettled proves the strong claim behind the stress tests:
// every enqueued ID was settled exactly once, and no unexpected (phantom) ID
// was settled or delivered. Counters must be read after all workers have
// stopped (no concurrent access).
func assertExactlyOnceSettled(t *testing.T, expected map[string]struct{}, settled, deliveries map[string]int) {
	t.Helper()
	for id := range expected {
		if settled[id] == 0 {
			t.Fatalf("message %s was enqueued but never settled (lost)", id)
		}
	}
	for id := range settled {
		if _, ok := expected[id]; !ok {
			t.Fatalf("unexpected message %s was settled (never enqueued)", id)
		}
	}
	for id := range deliveries {
		if _, ok := expected[id]; !ok {
			t.Fatalf("unexpected message %s was delivered (never enqueued)", id)
		}
	}
	if len(settled) != len(expected) {
		t.Fatalf("settled %d distinct messages, expected %d", len(settled), len(expected))
	}
	for id, n := range settled {
		if n != 1 {
			t.Fatalf("message %s settled %d times (want exactly once)", id, n)
		}
	}
	for id, n := range deliveries {
		if n != 1 {
			t.Fatalf("message %s delivered %d times (want exactly once)", id, n)
		}
	}
}

func newStressDriver(t *testing.T, lease time.Duration) (*queueredis.RedisDriver, *redis.Client, string) {
	t.Helper()
	addr := requireStressEnv(t, "TEST_REDIS_ADDR")
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("queue-redis: cannot reach %s: %v", addr, err)
	}
	prefix := fmt.Sprintf("fiber:queue:stress:%s:", randSuffix(t))
	driver := queueredis.New(client, queueredis.WithPrefix(prefix), queueredis.WithVisibilityTimeout(lease))
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Del(cctx, prefix+"priority_zset", prefix+"messages_hash", prefix+"delayed_zset", prefix+"inflight_zset", prefix+"inflight_scores", prefix+"inflight_claims").Err()
		_ = client.Close()
	})
	return driver, client, prefix
}

// TestStress_Redis_ConcurrentNoLossNoDup floods the broker with concurrent
// producers/consumers under a generous lease and asserts exactly-once
// settlement: every enqueued ID is Acked exactly once and none delivered twice.
func TestStress_Redis_ConcurrentNoLossNoDup(t *testing.T) {
	const (
		producers   = 8
		perProducer = 500
		consumers   = 8
		total       = producers * perProducer
	)
	driver, _, _ := newStressDriver(t, 60*time.Second)
	ctx := context.Background()

	var (
		mu       sync.Mutex
		expected = make(map[string]struct{}, total)
		seen     = make(map[string]int, total)
		settled  = make(map[string]int, total)
	)
	var settledN int64
	sink := &stressErr{}

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				m := queue.NewMessage("redis.stress", []byte("x"))
				mu.Lock()
				expected[m.ID] = struct{}{}
				mu.Unlock()
				if err := driver.Enqueue(ctx, m); err != nil {
					sink.add(fmt.Errorf("enqueue %s: %w", m.ID, err))
					return
				}
			}
		}()
	}

	stop := make(chan struct{})
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ds, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 64})
				if err != nil {
					sink.add(fmt.Errorf("dequeue: %w", err))
					return
				}
				for _, d := range ds {
					mu.Lock()
					seen[d.Message.ID]++
					mu.Unlock()
					// Ack first; only a successful settlement advances the
					// progress counter, so a failed Ack can never be mistaken
					// for "no loss".
					if err := driver.Ack(ctx, d); err != nil {
						sink.add(fmt.Errorf("ack %s: %w", d.Message.ID, err))
						continue
					}
					mu.Lock()
					settled[d.Message.ID]++
					mu.Unlock()
					atomic.AddInt64(&settledN, 1)
				}
				if len(ds) == 0 {
					time.Sleep(2 * time.Millisecond)
				}
			}
		}()
	}

	deadline := time.Now().Add(90 * time.Second)
	for atomic.LoadInt64(&settledN) < total {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d/%d deliveries settled", atomic.LoadInt64(&settledN), total)
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	sink.fatalIfAny(t)

	mu.Lock()
	defer mu.Unlock()
	assertExactlyOnceSettled(t, expected, settled, seen)
}

// TestStress_Redis_CrashReclaimNoLoss simulates a mass consumer crash: every
// message is claimed but left unacked, the lease expires, then concurrent
// consumers reclaim and settle them. Asserts the reclaim Lua script loses
// nothing, hands no message to two consumers at once, and settles each ID once.
func TestStress_Redis_CrashReclaimNoLoss(t *testing.T) {
	const total = 2000
	lease := 2 * time.Second
	driver, _, _ := newStressDriver(t, lease)
	ctx := context.Background()
	sink := &stressErr{}

	expected := make(map[string]struct{}, total)
	for i := 0; i < total; i++ {
		m := queue.NewMessage("redis.reclaim", nil)
		expected[m.ID] = struct{}{}
		if err := driver.Enqueue(ctx, m); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	// Phase 1: claim everything, then "crash" (never Ack). Distinct claims are
	// tracked so an early reclaim (a lease bug) shows up as a stall here.
	claimedSet := make(map[string]struct{}, total)
	deadline := time.Now().Add(10 * time.Second)
	for len(claimedSet) < total {
		if time.Now().After(deadline) {
			t.Fatalf("phase1 timeout: distinct claimed %d/%d", len(claimedSet), total)
		}
		ds, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 256})
		if err != nil {
			t.Fatalf("phase1 dequeue: %v", err)
		}
		for _, d := range ds {
			if _, dup := claimedSet[d.Message.ID]; dup {
				t.Fatalf("phase1: message %s claimed twice while its lease was still valid", d.Message.ID)
			}
			claimedSet[d.Message.ID] = struct{}{}
		}
		if len(ds) == 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}

	time.Sleep(lease + 300*time.Millisecond)

	// Phase 2: concurrent reclaim + Ack. Any duplicate claim or failed
	// settlement is caught here.
	var (
		mu      sync.Mutex
		seen    = make(map[string]int, total)
		settled = make(map[string]int, total)
	)
	var settledN int64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for c := 0; c < 8; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ds, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 64})
				if err != nil {
					sink.add(fmt.Errorf("phase2 dequeue: %w", err))
					return
				}
				for _, d := range ds {
					mu.Lock()
					seen[d.Message.ID]++
					mu.Unlock()
					if err := driver.Ack(ctx, d); err != nil {
						sink.add(fmt.Errorf("phase2 ack %s: %w", d.Message.ID, err))
						continue
					}
					mu.Lock()
					settled[d.Message.ID]++
					mu.Unlock()
					atomic.AddInt64(&settledN, 1)
				}
				if len(ds) == 0 {
					time.Sleep(5 * time.Millisecond)
				}
			}
		}()
	}

	deadline = time.Now().Add(30 * time.Second)
	for atomic.LoadInt64(&settledN) < total {
		if time.Now().After(deadline) {
			t.Fatalf("phase2 timeout: only %d/%d reclaimed messages settled", atomic.LoadInt64(&settledN), total)
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	sink.fatalIfAny(t)

	mu.Lock()
	defer mu.Unlock()
	assertExactlyOnceSettled(t, expected, settled, seen)
}

// TestStress_Redis_DelayedMigrationConcurrent schedules many delayed messages
// maturing concurrently while consumers drain, and asserts every one is
// delivered and settled exactly once (the TxPipeline migration must not drop
// or duplicate).
func TestStress_Redis_DelayedMigrationConcurrent(t *testing.T) {
	const total = 1000
	driver, _, _ := newStressDriver(t, 30*time.Second)
	ctx := context.Background()
	sink := &stressErr{}

	var (
		mu      sync.Mutex
		seen    = make(map[string]int, total)
		settled = make(map[string]int, total)
	)
	expected := make(map[string]struct{}, total)
	for i := 0; i < total; i++ {
		m := queue.NewMessage("redis.delayed", nil)
		expected[m.ID] = struct{}{}
		at := time.Now().Add(time.Duration(i%20) * 10 * time.Millisecond)
		if err := driver.EnqueueAt(ctx, m, at); err != nil {
			t.Fatalf("enqueueAt: %v", err)
		}
	}

	var settledN int64
	deadline := time.Now().Add(30 * time.Second)
	for atomic.LoadInt64(&settledN) < total {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d/%d delayed messages settled", atomic.LoadInt64(&settledN), total)
		}
		ds, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 128})
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		for _, d := range ds {
			mu.Lock()
			seen[d.Message.ID]++
			mu.Unlock()
			if err := driver.Ack(ctx, d); err != nil {
				sink.add(fmt.Errorf("ack %s: %w", d.Message.ID, err))
				continue
			}
			mu.Lock()
			settled[d.Message.ID]++
			mu.Unlock()
			atomic.AddInt64(&settledN, 1)
		}
		time.Sleep(5 * time.Millisecond)
	}
	sink.fatalIfAny(t)

	mu.Lock()
	defer mu.Unlock()
	assertExactlyOnceSettled(t, expected, settled, seen)
}

// TestStress_Redis_DelayedMigrationConcurrentAcks reproduces the race where
// two concurrent Dequeue calls both observe the same matured delayed member:
// a delayed message that is migrated, delivered and Acked must never be
// re-added to the ready set by an overlapping migration (which would produce
// a duplicate delivery followed by a poison "missing payload"). Messages are
// written exclusively in the split wire format.
func TestStress_Redis_DelayedMigrationConcurrentAcks(t *testing.T) {
	const (
		readyTotal   = 600
		delayedTotal = 200
		total        = readyTotal + delayedTotal
	)
	driver, _, _ := newStressDriver(t, 60*time.Second)
	ctx := context.Background()
	sink := &stressErr{}

	payload := make([]byte, 512)
	for i := range payload {
		payload[i] = byte(i*7 + 3)
	}

	var (
		mu       sync.Mutex
		expected = make(map[string]struct{}, total)
		seen     = make(map[string]int, total)
		settled  = make(map[string]int, total)
	)
	var settledN int64

	var wg sync.WaitGroup
	for p := 0; p < 4; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < readyTotal/4; j++ {
				m := queue.NewMessage("redis.split.ready", payload)
				mu.Lock()
				expected[m.ID] = struct{}{}
				mu.Unlock()
				if err := driver.Enqueue(ctx, m); err != nil {
					sink.add(fmt.Errorf("enqueue %s: %w", m.ID, err))
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < delayedTotal; j++ {
			m := queue.NewMessage("redis.split.delayed", payload)
			mu.Lock()
			expected[m.ID] = struct{}{}
			mu.Unlock()
			if err := driver.EnqueueAt(ctx, m, time.Now().Add(50*time.Millisecond)); err != nil {
				sink.add(fmt.Errorf("enqueueAt %s: %w", m.ID, err))
				return
			}
		}
	}()

	stop := make(chan struct{})
	for c := 0; c < 4; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ds, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 64})
				if err != nil {
					sink.add(fmt.Errorf("dequeue: %w", err))
					return
				}
				for _, d := range ds {
					mu.Lock()
					seen[d.Message.ID]++
					mu.Unlock()
					if !bytes.Equal(d.Message.Payload, payload) {
						sink.add(fmt.Errorf("payload corrupted for %s: topic=%q gotLen=%d wantLen=%d",
							d.Message.ID, d.Message.Topic, len(d.Message.Payload), len(payload)))
						continue
					}
					if err := driver.Ack(ctx, d); err != nil {
						sink.add(fmt.Errorf("ack %s: %w", d.Message.ID, err))
						continue
					}
					mu.Lock()
					settled[d.Message.ID]++
					mu.Unlock()
					atomic.AddInt64(&settledN, 1)
				}
				if len(ds) == 0 {
					time.Sleep(2 * time.Millisecond)
				}
			}
		}()
	}

	deadline := time.Now().Add(90 * time.Second)
	for atomic.LoadInt64(&settledN) < total {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d/%d deliveries settled", atomic.LoadInt64(&settledN), total)
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	sink.fatalIfAny(t)

	mu.Lock()
	defer mu.Unlock()
	assertExactlyOnceSettled(t, expected, settled, seen)
}
