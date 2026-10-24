package solution

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
)

// LeasedJob is what a worker receives when it successfully leases a job.
type LeasedJob struct {
	ID             string
	Payload        []byte
	LeaseExpiresAt int64 // milliseconds, in the same frame as Clock.Now()
}

// Clock is the queue's view of time. The test harness supplies a fake clock
// so it can fast-forward past lease expiries without sleeping.
type Clock interface {
	Now() int64 // milliseconds
}

// Stats reports the current size of each job state.
type Stats struct {
	Pending   int // never leased, or failed and waiting
	Leased    int // currently held by a worker (may have expired)
	Completed int // acked successfully
}

var (
	ErrNotFound    = errors.New("job not found")
	ErrNotLeased   = errors.New("job is not leased")
	ErrNotHolder   = errors.New("worker does not hold the lease")
	ErrAlreadyDone = errors.New("job already completed")
	ErrDeadLetter  = errors.New("job is in dead letter")
)

type jobStatus int

const (
	statusPending jobStatus = iota
	statusLeased
	statusCompleted
	statusDead
)

type job struct {
	id             string
	payload        []byte
	status         jobStatus
	holder         string
	leaseExpiresAt int64
	availableAt    int64
	attempts       int
	priority       int   // base priority; higher leases first
	waitingSince   int64 // clock ms when job last entered pending (for aging)
}

// JobQueue is a single-process, in-memory lease queue.
//
// Guarantees at-least-once delivery via time-bounded leases. Safe for
// concurrent callers. Production retry policy (max attempts → dead letter,
// expiry backoff) is always on; see Config / DefaultConfig.
type JobQueue struct {
	clock Clock
	cfg   Config

	mu      sync.Mutex
	nextID  uint64
	jobs    map[string]*job
	pending []string // FIFO of job IDs in pending (may include not-yet-available)
	dead    []DeadJob
	metrics Metrics
	rng     *rand.Rand
}

// New constructs an empty queue with production DefaultConfig.
func New(clock Clock) *JobQueue {
	return NewWithConfig(clock, DefaultConfig())
}

// NewWithConfig constructs a queue with explicit knobs (useful in unit tests).
func NewWithConfig(clock Clock, cfg Config) *JobQueue {
	cfg = cfg.normalized()
	return &JobQueue{
		clock:  clock,
		cfg:    cfg,
		jobs:   make(map[string]*job),
		rng:    rand.New(rand.NewSource(1)),
	}
}

// Enqueue stores a new job at default priority 0 and returns its unique ID.
func (q *JobQueue) Enqueue(payload []byte) (string, error) {
	return q.EnqueueWithPriority(payload, 0)
}

// EnqueueWithPriority stores a new job with the given base priority.
// Higher priority is leased before lower among currently available jobs;
// ties break FIFO. Waiting jobs gain effective priority via aging
// (see Config.PriorityAgeBoostEveryMS).
func (q *JobQueue) EnqueueWithPriority(payload []byte, priority int) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.enqueueLocked(payload, priority)
}

func (q *JobQueue) enqueueLocked(payload []byte, priority int) (string, error) {
	q.nextID++
	id := fmt.Sprintf("%d", q.nextID)

	cp := make([]byte, len(payload))
	copy(cp, payload)

	now := q.clock.Now()
	j := &job{
		id:           id,
		payload:      cp,
		status:       statusPending,
		priority:     priority,
		waitingSince: now,
	}
	q.jobs[id] = j
	q.pending = append(q.pending, id)
	q.metrics.Enqueued++
	return id, nil
}

// Lease atomically reserves the next available job for workerID.
// Returns (nil, nil) when no job is available.
func (q *JobQueue) Lease(workerID string, leaseMS int64) (*LeasedJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.clock.Now()
	q.reclaimExpired(now)

	idx := q.findAvailablePending(now)
	if idx < 0 {
		return nil, nil
	}

	id := q.pending[idx]
	q.pending = append(q.pending[:idx], q.pending[idx+1:]...)

	j := q.jobs[id]
	j.status = statusLeased
	j.holder = workerID
	j.leaseExpiresAt = now + leaseMS
	j.availableAt = 0
	j.attempts++
	q.metrics.Leased++
	if j.attempts > q.metrics.MaxAttemptsObserved {
		q.metrics.MaxAttemptsObserved = j.attempts
	}

	payload := make([]byte, len(j.payload))
	copy(payload, j.payload)

	return &LeasedJob{
		ID:             j.id,
		Payload:        payload,
		LeaseExpiresAt: j.leaseExpiresAt,
	}, nil
}

// Ack marks a job as completed. Rejects stale acks from non-holders.
func (q *JobQueue) Ack(workerID, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	j, err := q.requireHolder(workerID, jobID, true)
	if err != nil {
		return err
	}

	j.status = statusCompleted
	j.holder = ""
	j.leaseExpiresAt = 0
	q.metrics.Acked++
	return nil
}

// Fail releases the lease without completing. The job becomes immediately
// re-leasable (no backoff), or dead-lettered if MaxAttempts is exhausted.
func (q *JobQueue) Fail(workerID, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	j, err := q.requireHolder(workerID, jobID, false)
	if err != nil {
		return err
	}

	q.metrics.Failed++
	j.holder = ""
	j.leaseExpiresAt = 0

	if j.attempts >= q.cfg.MaxAttempts {
		q.moveToDead(j, "max_attempts_after_fail")
		return nil
	}

	j.status = statusPending
	j.availableAt = 0 // immediately re-leasable per API contract
	j.waitingSince = q.clock.Now()
	q.pending = append(q.pending, j.id)
	return nil
}

// Stats returns counts of jobs by state. Dead-letter jobs are excluded from
// Pending/Leased/Completed (they are terminal for the drain loop).
func (q *JobQueue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()

	var s Stats
	for _, j := range q.jobs {
		switch j.status {
		case statusPending:
			s.Pending++
		case statusLeased:
			s.Leased++
		case statusCompleted:
			s.Completed++
		}
	}
	return s
}

// Metrics returns a snapshot of operator counters.
func (q *JobQueue) Metrics() Metrics {
	q.mu.Lock()
	defer q.mu.Unlock()
	m := q.metrics
	m.DeadLetterCount = len(q.dead)
	return m
}

// DeadLetter returns a copy of jobs awaiting human intervention.
func (q *JobQueue) DeadLetter() []DeadJob {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]DeadJob, len(q.dead))
	copy(out, q.dead)
	for i := range out {
		out[i].Payload = append([]byte(nil), out[i].Payload...)
	}
	return out
}

func (q *JobQueue) requireHolder(workerID, jobID string, isAck bool) (*job, error) {
	j, ok := q.jobs[jobID]
	if !ok {
		return nil, ErrNotFound
	}
	switch j.status {
	case statusCompleted:
		return nil, ErrAlreadyDone
	case statusDead:
		return nil, ErrDeadLetter
	case statusPending:
		if isAck {
			q.metrics.StaleAckRejected++
		} else {
			q.metrics.StaleFailRejected++
		}
		return nil, ErrNotLeased
	case statusLeased:
		if j.holder != workerID {
			if isAck {
				q.metrics.StaleAckRejected++
			} else {
				q.metrics.StaleFailRejected++
			}
			return nil, ErrNotHolder
		}
		// Expired lease: still "leased" in Stats until reclaim, but the
		// holder is no longer valid for Ack/Fail once time has passed —
		// actually the L1 test re-leases to worker-2 first, then worker-1
		// acks. After re-lease, holder is worker-2, so worker-1 gets
		// ErrNotHolder. If someone Acks after expiry but before reclaim,
		// should we reject? Yes — lease is expired, worker no longer holds
		// a valid lease. Check expiry.
		if q.clock.Now() >= j.leaseExpiresAt {
			if isAck {
				q.metrics.StaleAckRejected++
			} else {
				q.metrics.StaleFailRejected++
			}
			return nil, ErrNotHolder
		}
		return j, nil
	}
	return nil, ErrNotLeased
}

func (q *JobQueue) reclaimExpired(now int64) {
	for id, j := range q.jobs {
		if j.status != statusLeased {
			continue
		}
		if now < j.leaseExpiresAt {
			continue
		}
		q.metrics.ExpiredReclaims++
		expiredAt := j.leaseExpiresAt
		j.holder = ""
		j.leaseExpiresAt = 0

		if j.attempts >= q.cfg.MaxAttempts {
			q.moveToDead(j, "max_attempts_after_expiry")
			continue
		}

		j.status = statusPending
		// Backoff is anchored at lease expiry time (not reclaim time) so a
		// Lease call after the harness advances past expiry+backoff can
		// reclaim and re-lease in one step.
		j.availableAt = expiredAt + q.backoffDelay(j.attempts)
		j.waitingSince = now
		q.pending = append(q.pending, id)
	}
}

func (q *JobQueue) moveToDead(j *job, reason string) {
	j.status = statusDead
	j.holder = ""
	j.leaseExpiresAt = 0
	j.availableAt = 0
	payload := append([]byte(nil), j.payload...)
	q.dead = append(q.dead, DeadJob{
		ID:       j.id,
		Payload:  payload,
		Attempts: j.attempts,
		Reason:   reason,
	})
	q.metrics.DeadLetterCount = len(q.dead)
}

func (q *JobQueue) findAvailablePending(now int64) int {
	bestIdx := -1
	bestEff := int64(math.MinInt64)
	for i, id := range q.pending {
		j, ok := q.jobs[id]
		if !ok || j.status != statusPending {
			continue
		}
		if now < j.availableAt {
			continue
		}
		eff := q.effectivePriority(j, now)
		// Higher effective priority wins; FIFO (lower index) on ties.
		if bestIdx < 0 || eff > bestEff {
			bestIdx = i
			bestEff = eff
		}
	}
	return bestIdx
}

func (q *JobQueue) effectivePriority(j *job, now int64) int64 {
	eff := int64(j.priority)
	every := q.cfg.PriorityAgeBoostEveryMS
	if every > 0 && now > j.waitingSince {
		eff += (now - j.waitingSince) / every
	}
	return eff
}

func (q *JobQueue) backoffDelay(attempts int) int64 {
	if q.cfg.BackoffBaseMS == 0 || attempts <= 0 {
		return 0
	}
	// delay = base * multiplier^(attempts-1)
	delay := float64(q.cfg.BackoffBaseMS) * math.Pow(q.cfg.BackoffMultiplier, float64(attempts-1))
	if delay > float64(q.cfg.BackoffCapMS) {
		delay = float64(q.cfg.BackoffCapMS)
	}
	frac := q.cfg.BackoffJitterFraction
	if frac > 0 && q.rng != nil {
		// Fixed-seed rng keeps unit tests stable.
		j := (q.rng.Float64()*2 - 1) * frac // [-frac, +frac]
		delay = delay * (1 + j)
	}
	if delay < 0 {
		delay = 0
	}
	if delay > float64(q.cfg.BackoffCapMS) {
		delay = float64(q.cfg.BackoffCapMS)
	}
	return int64(delay)
}
