package solution

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// jobRecord is the on-disk entry for unfinished or dead-letter jobs.
type jobRecord struct {
	ID       string `json:"id"`
	Payload  []byte `json:"payload"`
	Priority int    `json:"priority"`
	Attempts int    `json:"attempts"`
	Reason   string `json:"reason,omitempty"`
}

type fileStore struct {
	root    string
	jobsDir string
	deadDir string
}

func newFileStore(dir string) (*fileStore, error) {
	jobsDir := filepath.Join(dir, "jobs")
	deadDir := filepath.Join(dir, "dead")
	if err := os.MkdirAll(jobsDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(deadDir, 0o755); err != nil {
		return nil, err
	}
	return &fileStore{root: dir, jobsDir: jobsDir, deadDir: deadDir}, nil
}

func (s *fileStore) jobPath(id string) string {
	return filepath.Join(s.jobsDir, id+".json")
}

func (s *fileStore) deadPath(id string) string {
	return filepath.Join(s.deadDir, id+".json")
}

func writeRecord(path string, rec jobRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (s *fileStore) save(j *job) error {
	return writeRecord(s.jobPath(j.id), jobRecord{
		ID:       j.id,
		Payload:  j.payload,
		Priority: j.priority,
		Attempts: j.attempts,
	})
}

func (s *fileStore) delete(id string) error {
	err := os.Remove(s.jobPath(id))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// saveDead writes the job under dead/ and removes it from jobs/.
func (s *fileStore) saveDead(j *job, reason string) error {
	rec := jobRecord{
		ID:       j.id,
		Payload:  j.payload,
		Priority: j.priority,
		Attempts: j.attempts,
		Reason:   reason,
	}
	if err := writeRecord(s.deadPath(j.id), rec); err != nil {
		return err
	}
	return s.delete(j.id)
}

func loadDir(dir string) ([]jobRecord, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make([]jobRecord, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		var rec jobRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			return nil, fmt.Errorf("load %s: %w", e.Name(), err)
		}
		out = append(out, rec)
	}
	return out, nil
}

func (s *fileStore) loadJobs() ([]jobRecord, error) {
	return loadDir(s.jobsDir)
}

func (s *fileStore) loadDead() ([]jobRecord, error) {
	return loadDir(s.deadDir)
}

func maxNumericID(recs ...[]jobRecord) uint64 {
	var max uint64
	for _, list := range recs {
		for _, r := range list {
			n, err := strconv.ParseUint(r.ID, 10, 64)
			if err == nil && n > max {
				max = n
			}
		}
	}
	return max
}
