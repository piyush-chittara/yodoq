package solution

import (
	"fmt"
	"sync"
	"testing"
)

type fakeClock struct{ now int64 }

func (c *fakeClock) Now() int64         { return c.now }
func (c *fakeClock) advance(ms int64)   { c.now += ms }

func TestEnqueueLeaseAck(t *testing.T) {
	clock := &fakeClock{}
	q := New(clock)

	id, err := q.Enqueue([]byte("hello"))
	if err != nil || id == "" {
		t.Fatalf("Enqueue: id=%q err=%v", id, err)
	}

	job, err := q.Lease("w1", 1000)
	if err != nil || job == nil || job.ID != id || string(job.Payload) != "hello" {
		t.Fatalf("Lease: job=%v err=%v", job, err)
	}

	if err := q.Ack("w1", id); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if j, _ := q.Lease("w1", 1000); j != nil {
		t.Fatalf("acked job should not re-lease, got %v", j)
	}

	s := q.Stats()
	if s.Pending != 0 || s.Leased != 0 || s.Completed != 1 {
		t.Fatalf("Stats = %+v", s)
	}
}

func TestFailReleasesImmediately(t *testing.T) {
	clock := &fakeClock{}
	q := New(clock)

	id, _ := q.Enqueue([]byte("x"))
	if _, err := q.Lease("w1", 1000); err != nil {
		t.Fatal(err)
	}
	if err := q.Fail("w1", id); err != nil {
		t.Fatal(err)
	}
	job, err := q.Lease("w2", 1000)
	if err != nil || job == nil || job.ID != id {
		t.Fatalf("expected immediate re-lease, job=%v err=%v", job, err)
	}
}

func TestExpiryReclaimAndStaleAck(t *testing.T) {
	clock := &fakeClock{}
	q := New(clock)

	id, _ := q.Enqueue([]byte("stale"))
	j1, _ := q.Lease("w1", 1000)
	if j1 == nil {
		t.Fatal("expected lease")
	}
	clock.advance(2000)

	j2, err := q.Lease("w2", 5000)
	if err != nil || j2 == nil || j2.ID != id {
		t.Fatalf("expected reclaim lease, job=%v err=%v", j2, err)
	}
	if err := q.Ack("w1", id); err == nil {
		t.Fatal("expected stale ack rejection")
	}
	if err := q.Ack("w2", id); err != nil {
		t.Fatalf("holder ack: %v", err)
	}

	m := q.Metrics()
	if m.ExpiredReclaims < 1 || m.StaleAckRejected < 1 {
		t.Fatalf("metrics = %+v", m)
	}
}

func TestMaxAttemptsDeadLetter(t *testing.T) {
	clock := &fakeClock{}
	q := NewWithConfig(clock, Config{
		MaxAttempts:       2,
		BackoffBaseMS:     0,
		BackoffMultiplier: 2,
		BackoffCapMS:      0,
	})

	id, _ := q.Enqueue([]byte("poison"))

	// Attempt 1
	if j, _ := q.Lease("w1", 100); j == nil {
		t.Fatal("lease 1")
	}
	clock.advance(200)
	// Expiry reclaim → still pending (attempts=1 < 2)

	// Attempt 2
	if j, _ := q.Lease("w1", 100); j == nil {
		t.Fatal("lease 2")
	}
	clock.advance(200)
	// Expiry with attempts=2 → dead letter

	j, err := q.Lease("w1", 100)
	if err != nil {
		t.Fatal(err)
	}
	if j != nil {
		t.Fatalf("expected no lease after DLQ, got %v", j)
	}

	dl := q.DeadLetter()
	if len(dl) != 1 || dl[0].ID != id || dl[0].Attempts != 2 {
		t.Fatalf("DeadLetter = %+v", dl)
	}
	s := q.Stats()
	if s.Pending != 0 || s.Leased != 0 {
		t.Fatalf("Stats after DLQ = %+v", s)
	}
}

func TestFailDeadLetterAtMaxAttempts(t *testing.T) {
	clock := &fakeClock{}
	q := NewWithConfig(clock, Config{MaxAttempts: 1, BackoffBaseMS: 0})

	id, _ := q.Enqueue([]byte("x"))
	if _, err := q.Lease("w1", 1000); err != nil {
		t.Fatal(err)
	}
	if err := q.Fail("w1", id); err != nil {
		t.Fatal(err)
	}
	if j, _ := q.Lease("w2", 1000); j != nil {
		t.Fatalf("expected DLQ, got lease %v", j)
	}
	if len(q.DeadLetter()) != 1 {
		t.Fatalf("want 1 dead job")
	}
}

func TestBackoffDelaysAvailability(t *testing.T) {
	clock := &fakeClock{}
	q := NewWithConfig(clock, Config{
		MaxAttempts:           10,
		BackoffBaseMS:         500,
		BackoffMultiplier:     1, // constant 500ms
		BackoffCapMS:          500,
		BackoffJitterFraction: 0, // deterministic
	})

	id, _ := q.Enqueue([]byte("b"))
	if _, err := q.Lease("w1", 100); err != nil {
		t.Fatal(err)
	}
	// Expire at t=100; backoff → availableAt = 100+500 = 600
	clock.advance(100)
	if j, _ := q.Lease("w2", 100); j != nil {
		t.Fatalf("should still be in backoff, got %v", j)
	}
	clock.advance(500) // now = 600
	j, err := q.Lease("w2", 100)
	if err != nil || j == nil || j.ID != id {
		t.Fatalf("expected lease after backoff, job=%v err=%v", j, err)
	}
}

func TestPayloadIsolatedFromCaller(t *testing.T) {
	clock := &fakeClock{}
	q := New(clock)
	payload := []byte("data")
	id, _ := q.Enqueue(payload)
	payload[0] = 'X'
	j, _ := q.Lease("w1", 1000)
	if j == nil || string(j.Payload) != "data" {
		t.Fatalf("queue stored caller slice: %q", j.Payload)
	}
	j.Payload[0] = 'Y'
	_ = q.Fail("w1", id)
	j2, _ := q.Lease("w1", 1000)
	if string(j2.Payload) != "data" {
		t.Fatalf("mutated leased payload leaked: %q", j2.Payload)
	}
}

func TestConcurrentLeaseAck(t *testing.T) {
	clock := &fakeClock{}
	q := New(clock)
	const n = 100
	for i := 0; i < n; i++ {
		_, _ = q.Enqueue([]byte("x"))
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for {
				j, err := q.Lease(id, 10_000)
				if err != nil {
					errCh <- err
					return
				}
				if j == nil {
					return
				}
				if err := q.Ack(id, j.ID); err != nil {
					errCh <- err
					return
				}
			}
		}(fmt.Sprintf("w-%d", w))
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("worker error: %v", err)
	}

	s := q.Stats()
	if s.Completed != n || s.Pending != 0 || s.Leased != 0 {
		t.Fatalf("Stats = %+v want completed=%d", s, n)
	}
}

func TestEnqueueWithPriorityOrder(t *testing.T) {
	clock := &fakeClock{}
	q := NewWithConfig(clock, Config{
		MaxAttempts:             32,
		PriorityAgeBoostEveryMS: 0, // base priority only
	})

	low, err := q.EnqueueWithPriority([]byte("low"), 1)
	if err != nil {
		t.Fatal(err)
	}
	high, err := q.EnqueueWithPriority([]byte("high"), 10)
	if err != nil {
		t.Fatal(err)
	}

	j, err := q.Lease("w1", 1000)
	if err != nil || j == nil || j.ID != high {
		t.Fatalf("expected high-priority first, got %+v (low=%s)", j, low)
	}
	_ = q.Ack("w1", j.ID)

	j2, err := q.Lease("w1", 1000)
	if err != nil || j2 == nil || j2.ID != low {
		t.Fatalf("expected low next, got %+v", j2)
	}
}

func TestEnqueueDefaultPriorityFIFO(t *testing.T) {
	clock := &fakeClock{}
	q := NewWithConfig(clock, Config{
		MaxAttempts:             32,
		PriorityAgeBoostEveryMS: 0,
	})

	a, _ := q.Enqueue([]byte("a"))
	b, _ := q.Enqueue([]byte("b"))
	high, _ := q.EnqueueWithPriority([]byte("high"), 5)

	j, _ := q.Lease("w1", 1000)
	if j == nil || j.ID != high {
		t.Fatalf("high should win over default, got %+v", j)
	}
	_ = q.Ack("w1", j.ID)

	j2, _ := q.Lease("w1", 1000)
	if j2 == nil || j2.ID != a {
		t.Fatalf("FIFO among priority 0: want %s, got %+v", a, j2)
	}
	_ = q.Ack("w1", j2.ID)

	j3, _ := q.Lease("w1", 1000)
	if j3 == nil || j3.ID != b {
		t.Fatalf("FIFO among priority 0: want %s, got %+v", b, j3)
	}
}

func TestPriorityAging(t *testing.T) {
	clock := &fakeClock{}
	q := NewWithConfig(clock, Config{
		MaxAttempts:             32,
		PriorityAgeBoostEveryMS: 1000,
		BackoffBaseMS:           0,
	})

	low, _ := q.EnqueueWithPriority([]byte("low"), 0)
	clock.advance(3000) // effective = 0 + 3 = 3

	high, _ := q.EnqueueWithPriority([]byte("high"), 2) // effective = 2

	j, err := q.Lease("w1", 1000)
	if err != nil || j == nil || j.ID != low {
		t.Fatalf("aged low should beat fresh high=2, got %+v (low=%s high=%s)", j, low, high)
	}
}
