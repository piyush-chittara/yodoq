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

## Fairness

**Jobs:** FIFO among equal effective priority, aging so low priority is not starved, `MaxAttempts` so one poison job cannot retry forever, expiry backoff so reclaim storms are spaced.

**Workers:** `MaxInFlightPerWorker` (default 8) caps how many unexpired leases one `workerID` may hold. Further `Lease` calls return `(nil, nil)` until some complete (`LeaseSkippedInFlightCap` metric). A greedy worker cannot vacuum every pending job while others get nothing. The harness ticks one lease per worker per round, so default 8 does not affect graded Levels 1–3.

## Retry and dead letter

Defaults live in `DefaultConfig()` and are what `New(clock)` uses. `NewWithConfig` exists so tests can set a low attempt ceiling without forking behavior.

| Knob | Default | Rationale |
|------|---------|-----------|
| `MaxAttempts` | 32 | Cap poison messages. After this many leases without an `Ack`, the job goes to the dead-letter list instead of spinning forever. |
| Expiry backoff | base 50ms, ×2, cap 2000ms, ±20% jitter | `availableAt = expiredAt + backoff(attempts)`. Anchored at expiry time. |
| `Fail` backoff | none | Immediately re-leasable per API. |
| `PriorityAgeBoostEveryMS` | 1000 | Aging step; 0 disables. |
| `MaxInFlightPerWorker` | 8 | Concurrent lease cap per worker. |

Dead-letter jobs are counted in `Stats.Dead` only (not Pending/Leased/Completed), so the harness drain loop that watches Pending/Leased still exits. Inspect details with `DeadLetter()` / `Metrics()`.

## Persistence (minimum on-disk footprint)

Minimum footprint is unfinished work plus dead letters — not an append-only history of every lease/ack.

| Event | Disk |
|-------|------|
| Enqueue | Write `jobs/<id>.json` (id, payload, priority, attempts) |
| Lease / Fail / expiry | No durable ownership. Attempts are rewritten on lease so `MaxAttempts` survives restart. Holder/expiry are not stored. |
| Ack | **Delete** `jobs/<id>.json` — completed work leaves nothing on disk |
| Dead letter | Write `dead/<id>.json` (includes reason), remove from `jobs/` — survives restart for intervention, not reloaded as pending |

On restart (`NewPersistent`):

- `jobs/` → pending/available (leases are not restored)
- `dead/` → dead-letter list + `Stats.Dead`

A WAL of every lease/ack would grow without bound. Live + dead sets stay proportional to open and poisoned work.

- `New(clock)` / `NewWithConfig` — memory only (what the harness uses).
- `NewPersistent(clock, dir, cfg)` — store under `dir/jobs/` and `dir/dead/`.

## Observability

- `Stats()` — Pending, Leased, Completed, **Dead**.
- `Metrics()` — expired reclaim, stale ack/fail, dead-letter count, max attempts seen, in-flight cap skips.
- `DeadLetter()` — id, payload copy, attempt count, reason string.

## Assumptions

1. Default `New` is memory-only. Persistent mode survives process restart via the live-set files above.
2. Workers pick their own `leaseMS`. The queue does not extend leases.
3. Job ids are opaque monotonic strings. The graded harness only calls `Enqueue` (priority 0).
4. “Lost work” means a job never successfully executed. The harness ledger is ground truth for executions.

## Not built (and why)

| Idea | Why not |
|------|---------|
| Idempotency keys on enqueue | Solves producer double-submit, not ack-drop re-delivery. Handlers should be idempotent. |
| `ExtendLease` / heartbeat | Not in the provided API. |

## Layout

```
solution/queue.go       # state machine + API
solution/config.go      # Config / defaults
solution/metrics.go     # Metrics, DeadJob
solution/store.go       # live-set file store for NewPersistent
solution/queue_test.go
```

```bash
GOTOOLCHAIN=local go run .
GOTOOLCHAIN=local go test ./solution
```
