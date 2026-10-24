# YODOQ design

In-memory job queue with lease-based delivery. Workers call `Lease` → do work → `Ack` (or `Fail`). The queue does not run jobs itself; it only tracks ownership and eligibility.

## Delivery model

**At-least-once.** A job stays leaseable until some worker’s `Ack` is accepted. If a worker dies mid-job or an `Ack` never arrives, the lease expires and another worker can take it.

That implies duplicates when acks are dropped: the worker may have finished the work already, but the queue still believes the lease is open. Suppressing those duplicates inside the queue would require knowing that side effects already happened. That knowledge belongs with the consumer (idempotent handlers keyed by job id / business id), not with enqueue-time dedup.

`Fail` is different from expiry: it means “give this back now.” The API requires the job to be immediately leaseable again, so `Fail` skips backoff.

## Core mechanics

| Mechanism | Choice |
|-----------|--------|
| Ownership | Soft lease: `(holder, LeaseExpiresAt)`. Lazy reclaim on the next `Lease` — no background sweeper. |
| Time | Injected `Clock`. Wall time is never read, so tests can fast-forward past leases. |
| Ready selection | Among jobs with `now >= availableAt`, highest **effective priority** wins; ties break FIFO. |
| Completion | `Ack` from the current, unexpired holder only. Otherwise rejected (`ErrNotHolder` / stale). |
| Concurrency | `sync.Mutex` on every public method. The harness is single-threaded; real callers won’t be. |
| Payloads | Copied on enqueue and on lease so callers can’t mutate queue storage. |

States: `pending` → `leased` → `completed`, or `pending` ↔ `leased` via `Fail` / expiry, or → `dead` when attempts are exhausted.

## Priority and aging

Two enqueue entry points:

- `Enqueue(payload)` — base priority **0** (what the harness uses).
- `EnqueueWithPriority(payload, priority)` — explicit base priority; higher is preferred.

On each `Lease`, effective priority is:

```text
effective = priority + (now - waitingSince) / PriorityAgeBoostEveryMS
```

`waitingSince` is set whenever a job enters pending (enqueue, `Fail` requeue, expiry reclaim). Default `PriorityAgeBoostEveryMS` is 1000 (one priority step per simulated second of wait). Set it to 0 to disable aging.

Without aging, a steady stream of high-priority work would starve low-priority jobs. Aging is the anti-starvation policy: wait long enough and a low base priority can outrank a fresh higher one.

## Retry and dead letter

Defaults live in `DefaultConfig()` and are what `New(clock)` uses. `NewWithConfig` exists so tests can set a low attempt ceiling without forking behavior.

| Knob | Default | Rationale |
|------|---------|-----------|
| `MaxAttempts` | 32 | Cap poison messages. After this many leases without an `Ack`, the job goes to the dead-letter list instead of spinning forever. 32 is high enough that under the harness’s ≤50% crash rate and 200 jobs, DLQ is effectively unused, but the path is real and covered by unit tests. |
| Expiry backoff | base 50ms, ×2, cap 2000ms, ±20% jitter | After an expired lease is reclaimed, `availableAt = expiredAt + backoff(attempts)`. Anchoring at **expiry time** (not reclaim time) keeps “advance clock, then `Lease`” correct. |
| `Fail` backoff | none | Required by the assignment: immediately re-leasable. |
| `PriorityAgeBoostEveryMS` | 1000 | Aging step for effective priority; 0 disables. |

Dead-letter jobs are excluded from `Stats` pending/leased/completed so a drain loop that only watches those counters can finish. Inspect them with `DeadLetter()` / `Metrics()`.

## Observability

- `Stats()` — sizes the harness already understands.
- `Metrics()` — counters useful when debugging: expired reclaim, stale ack/fail, dead-letter count, max attempts seen.
- `DeadLetter()` — id, payload copy, attempt count, reason string.

## Assumptions

1. Single process, memory only. Restart loses queue state.
2. Workers pick their own `leaseMS`. The queue does not extend leases; long work must use a lease long enough to finish (or fail and retry).
3. Job ids are opaque monotonic strings. The graded harness only calls `Enqueue` (priority 0), so Levels 1–3 still behave as FIFO among equal priority.
4. “Lost work” means a job never successfully executed. The queue’s job is to keep leasing until `Ack` or dead-letter; the harness ledger is the ground truth for executions.

## Not built (and why)

| Idea | Why not |
|------|---------|
| Idempotency keys on enqueue | Solves producer double-submit. Does not stop re-lease after ack-drop. Handlers should be idempotent instead. |
| Persistence / WAL | Out of scope for the take-home. A minimal design would log enqueue/lease/ack/fail/dead and rebuild maps on boot. |
| `ExtendLease` / heartbeat | Not in the provided API. Needed only when work regularly outlives a fixed lease. |
| Payload size limits, rate limits, multi-tenant fair share | Operational hardening; no signal in the assignment. |

## Layout

```
solution/queue.go    # state machine + API
solution/config.go   # Config / defaults
solution/metrics.go  # Metrics, DeadJob
solution/queue_test.go
```

```bash
GOTOOLCHAIN=local go run .
GOTOOLCHAIN=local go test ./solution
```
