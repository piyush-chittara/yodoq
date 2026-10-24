package solution

// Metrics are operator-facing counters for debugging a stuck or noisy queue.
type Metrics struct {
	Enqueued                int
	Leased                  int
	Acked                   int
	Failed                  int
	ExpiredReclaims         int
	StaleAckRejected        int
	StaleFailRejected       int
	DeadLetterCount         int
	MaxAttemptsObserved     int
	LeaseSkippedInFlightCap int
}

// DeadJob is a job moved to the dead-letter queue for human intervention.
type DeadJob struct {
	ID       string
	Payload  []byte
	Attempts int
	Reason   string
}
