package main

import (
	"cmp"
	"slices"

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
	t.pending = append(t.pending, pendingOffset{offset: offset, epoch: epoch})
	t.inflight++
}

// done marks a record finished and reports whether the committable offset
// moved. Unknown or already finished offsets are ignored.
func (t *offsetTracker) done(offset int64) bool {
	i, ok := slices.BinarySearchFunc(t.pending, offset, func(p pendingOffset, o int64) int {
		return cmp.Compare(p.offset, o)
	})
	if !ok || t.pending[i].done {
		return false
	}
	t.pending[i].done = true
	t.inflight--
	if i > 0 {
		return false
	}

	n := 0
	for n < len(t.pending) && t.pending[n].done {
		n++
	}
	last := t.pending[n-1]
	// The epoch of the last finished record: the next owner uses it to detect
	// log truncation after an unclean leader change (KIP-320).
	t.commit = kgo.EpochOffset{Epoch: last.epoch, Offset: last.offset + 1}
	t.pending = t.pending[n:]
	return true
}

// committable returns the offset to commit, if any record has finished.
func (t *offsetTracker) committable() (kgo.EpochOffset, bool) {
	return t.commit, t.commit.Offset >= 0
}

// inFlight is the number of dispatched records that have not finished.
func (t *offsetTracker) inFlight() int { return t.inflight }
