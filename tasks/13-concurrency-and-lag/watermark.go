package main

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

// offsetTracker knows, for one partition, which dispatched records are still
// in flight and which offset is safe to commit: one past the longest prefix
// of finished records. Records finish in any order; the commit only moves
// when the oldest in-flight record finishes. Not safe for concurrent use.
type offsetTracker struct {
	pending  []pendingOffset // dispatched and not yet committable, ascending
	inflight int
	commit   kgo.EpochOffset // Offset -1 until the first record finishes
}

type pendingOffset struct {
	offset int64
	epoch  int32
	done   bool
}

func newOffsetTracker() *offsetTracker {
	return &offsetTracker{commit: kgo.EpochOffset{Epoch: -1, Offset: -1}}
}

// add registers a record handed to a worker. Offsets must increase but may
// have gaps (transaction markers, compaction).
func (t *offsetTracker) add(offset int64, epoch int32) {
	// TODO(13.1): append to t.pending and count it in t.inflight.
}

// done marks a record finished and reports whether the committable offset
// moved. Unknown or already finished offsets are ignored.
func (t *offsetTracker) done(offset int64) bool {
	// TODO(13.1): find the offset in t.pending (it is sorted), mark it done.
	// If it was the oldest one, drop the finished prefix and set t.commit to
	// one past its last offset, with that record's epoch.
	return false
}

// committable returns the offset to commit, if any record has finished.
func (t *offsetTracker) committable() (kgo.EpochOffset, bool) {
	return t.commit, t.commit.Offset >= 0
}

// inFlight is the number of dispatched records that have not finished.
func (t *offsetTracker) inFlight() int { return t.inflight }
