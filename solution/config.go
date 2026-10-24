package solution

// Config holds production retry / intervention knobs.
// New(clock) uses DefaultConfig(). Use NewWithConfig in tests to tighten MaxAttempts.
type Config struct {
	// MaxAttempts is the maximum number of successful Lease calls a job may
	// receive. After a lease ends without Ack (Fail or expiry) and
	// attempts >= MaxAttempts, the job moves to the dead-letter queue.
	MaxAttempts int

	// BackoffBaseMS is the base delay applied after lease expiry reclaim
	// before the job is leaseable again. Fail() does not apply backoff
	// (API requires immediately re-leasable).
	BackoffBaseMS int64

	// BackoffMultiplier scales the delay as attempts grow (exponential).
	BackoffMultiplier float64

	// BackoffCapMS caps the expiry-reclaim backoff.
	BackoffCapMS int64

	// BackoffJitterFraction is applied as ±fraction of the computed backoff
	// (e.g. 0.2 = ±20%). Clamped so delay stays in [0, BackoffCapMS].
	BackoffJitterFraction float64

	// PriorityAgeBoostEveryMS adds +1 to effective priority for each full
	// interval a job has spent waiting in pending since waitingSince.
	// Zero disables aging (base priority only).
	PriorityAgeBoostEveryMS int64
}

// DefaultConfig returns production defaults safe for the YODOQ harness
// (L3 at 50% crash with 200 jobs still sees effectively zero DLQ losses).
func DefaultConfig() Config {
	return Config{
		MaxAttempts:             32,
		BackoffBaseMS:           50,
		BackoffMultiplier:       2,
		BackoffCapMS:            2000,
		BackoffJitterFraction:   0.2,
		PriorityAgeBoostEveryMS: 1000,
	}
}

func (c Config) normalized() Config {
	d := DefaultConfig()
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = d.MaxAttempts
	}
	if c.BackoffBaseMS < 0 {
		c.BackoffBaseMS = d.BackoffBaseMS
	}
	if c.BackoffMultiplier < 1 {
		c.BackoffMultiplier = d.BackoffMultiplier
	}
	if c.BackoffCapMS < 0 {
		c.BackoffCapMS = d.BackoffCapMS
	}
	if c.BackoffJitterFraction < 0 {
		c.BackoffJitterFraction = d.BackoffJitterFraction
	}
	if c.PriorityAgeBoostEveryMS < 0 {
		c.PriorityAgeBoostEveryMS = d.PriorityAgeBoostEveryMS
	}
	return c
}
