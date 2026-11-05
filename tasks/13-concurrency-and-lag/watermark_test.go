package main

import (
	"math/rand/v2"
	"testing"
)

func addAll(t *offsetTracker, offsets ...int64) {
	for _, o := range offsets {
		t.add(o, 7)
	}
}

func wantCommit(t *testing.T, tr *offsetTracker, want int64) {
	t.Helper()
	got, ok := tr.committable()
	if want < 0 {
		if ok {
			t.Fatalf("committable = %d, want nothing yet", got.Offset)
		}
		return
	}
	if !ok || got.Offset != want {
		t.Fatalf("committable = %d (ok=%v), want %d", got.Offset, ok, want)
	}
}

func TestInOrder(t *testing.T) {
	tr := newOffsetTracker()
	addAll(tr, 10, 11, 12)
	wantCommit(t, tr, -1)
	for _, o := range []int64{10, 11, 12} {
		if !tr.done(o) {
			t.Fatalf("done(%d) did not move the commit", o)
		}
		wantCommit(t, tr, o+1)
	}
	if got, _ := tr.committable(); got.Epoch != 7 {
		t.Fatalf("epoch = %d, want the last record's epoch 7", got.Epoch)
	}
	if tr.inFlight() != 0 {
		t.Fatalf("inFlight = %d, want 0", tr.inFlight())
	}
}

func TestOutOfOrderWaitsForOldest(t *testing.T) {
	tr := newOffsetTracker()
	addAll(tr, 10, 11, 12, 13)
	tr.done(12)
	tr.done(13)
	tr.done(11)
	wantCommit(t, tr, -1) // 10 is still running: committing 14 would lose it on a crash
	if tr.inFlight() != 1 {
		t.Fatalf("inFlight = %d, want 1", tr.inFlight())
	}
	if !tr.done(10) {
		t.Fatal("done(10) did not move the commit")
	}
	wantCommit(t, tr, 14)
}

func TestGapsInOffsets(t *testing.T) {
	tr := newOffsetTracker()
	addAll(tr, 10, 11, 15, 16) // 12-14: transaction markers or compacted away
	tr.done(15)
	tr.done(10)
	wantCommit(t, tr, 11)
	tr.done(11)
	wantCommit(t, tr, 16)
	tr.done(16)
	wantCommit(t, tr, 17)
}

func TestUnknownAndRepeatedDoneAreIgnored(t *testing.T) {
	tr := newOffsetTracker()
	addAll(tr, 10, 11)
	if tr.done(99) || tr.done(9) {
		t.Fatal("unknown offset moved the commit")
	}
	tr.done(11)
	if tr.done(11) {
		t.Fatal("repeated done moved the commit")
	}
	if tr.inFlight() != 1 {
		t.Fatalf("inFlight = %d, want 1", tr.inFlight())
	}
	tr.done(10)
	wantCommit(t, tr, 12)
	if tr.done(10) {
		t.Fatal("done on an already committable offset moved the commit")
	}
	wantCommit(t, tr, 12)
}

// A revoke with records still queued or running: the owner commits what the
// tracker says and drops the rest. The next owner starts there, so nothing
// unfinished is skipped; finished records past it are redelivered.
func TestRevokeMidFlight(t *testing.T) {
	tr := newOffsetTracker()
	addAll(tr, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	for _, o := range []int64{0, 1, 2, 5, 6, 9} {
		tr.done(o)
	}
	wantCommit(t, tr, 3)

	// Same partition assigned back later: a fresh tracker, records from 3.
	next := newOffsetTracker()
	addAll(next, 3, 4, 5)
	wantCommit(t, next, -1)
	next.done(4)
	next.done(3)
	wantCommit(t, next, 5)
}

// For every random completion order, the commit never passes an unfinished
// record, never goes backwards, and ends one past the last offset.
func TestRandomCompletionNeverSkips(t *testing.T) {
	for round := range 200 {
		r := rand.New(rand.NewPCG(uint64(round), 42))
		tr := newOffsetTracker()
		var offsets []int64
		next := int64(r.IntN(1000))
		for range 1 + r.IntN(300) {
			offsets = append(offsets, next)
			tr.add(next, 0)
			next += 1 + int64(r.IntN(3)) // gaps
		}

		finished := map[int64]bool{}
		prev := int64(-1)
		for _, i := range r.Perm(len(offsets)) {
			tr.done(offsets[i])
			finished[offsets[i]] = true

			got, ok := tr.committable()
			if !ok {
				got.Offset = -1
			}
			if got.Offset < prev {
				t.Fatalf("round %d: commit went backwards %d -> %d", round, prev, got.Offset)
			}
			prev = got.Offset
			for _, o := range offsets {
				if o < got.Offset && !finished[o] {
					t.Fatalf("round %d: commit %d skips unfinished offset %d", round, got.Offset, o)
				}
			}
		}
		wantCommit(t, tr, offsets[len(offsets)-1]+1)
		if tr.inFlight() != 0 {
			t.Fatalf("round %d: inFlight = %d after all done", round, tr.inFlight())
		}
	}
}
