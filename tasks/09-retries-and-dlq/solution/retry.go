package main

import (
	"context"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"idiomaticGo/kafka/internal/kit"
)

// retryWorker consumes the retry topic. A record that isn't due yet is not
// slept on: its partition is rewound to it and paused until it is due, so the
// poll loop keeps serving other partitions and rebalances are never held up.
func retryWorker(ctx context.Context, app *kit.App) error {
	flush, err := startTracing(ctx, app)
	if err != nil {
		return err
	}
	defer flush()

	cl, err := app.NewClient(
		kgo.ConsumeTopics(retryTopic),
		kgo.ConsumerGroup(retryGroup),
		// Records stay in flight across polls, so only what we mark is committed.
		kgo.AutoCommitMarks(),
		// SetOffsets and marks must not race a revoke.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
			if err := cl.CommitMarkedOffsets(ctx); err != nil {
				app.Log.Error("commit on revoke failed", "err", err)
			}
			// A pause outlives the assignment; a partition we get back later must not stay paused.
			cl.ResumeFetchPartitions(revoked)
		}),
		kgo.OnPartitionsLost(func(_ context.Context, cl *kgo.Client, lost map[string][]int32) {
			cl.ResumeFetchPartitions(lost)
		}),
	)
	if err != nil {
		return err
	}
	defer cl.CloseAllowingRebalance()
	if err := ensureTopics(ctx, app, cl); err != nil {
		return err
	}

	w := &worker{app: app, cl: cl, cfg: loadConfig()}
	due := map[int32]time.Time{} // paused partition -> when its first record is due
	for {
		resumeDue(cl, due)
		pctx, cancel := pollContext(ctx, due)
		fetches := cl.PollFetches(pctx)
		cancel()
		if ctx.Err() != nil || fetches.IsClientClosed() {
			break
		}
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) {
				app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
			}
		})

		var err error
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			for _, rec := range p.Records {
				if err != nil {
					return
				}
				if at := retryAt(rec); time.Now().Before(at) {
					err = w.pauseUntil(ctx, rec)
					due[rec.Partition] = at
					return
				}
				if err = w.handleRetry(ctx, rec); err == nil {
					cl.MarkCommitRecords(rec)
				}
			}
		})
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return err
		}
		cl.AllowRebalance()
	}

	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return cl.CommitMarkedOffsets(cctx)
}

// handleRetry runs one more attempt and decides what's next for the record.
func (w *worker) handleRetry(ctx context.Context, rec *kgo.Record) error {
	attempt := headerInt(rec, hdrAttempt)
	ctx, span := startProcess(ctx, rec, retryGroup, attempt)
	defer span.End()

	err := w.process(ctx, rec, attempt)
	switch {
	case err == nil:
		return nil
	case classify(err) == permanent || attempt >= w.cfg.maxAttempts:
		failSpan(span, err)
		return w.park(ctx, toDLQ(rec, err, attempt))
	default:
		failSpan(span, err)
		return w.park(ctx, toRetry(rec, err, attempt+1, w.cfg.backoff(attempt+1)))
	}
}

// pauseUntil stops fetching rec's partition and rewinds it to rec, so rec and
// everything behind it is fetched again once the partition is resumed.
func (w *worker) pauseUntil(ctx context.Context, rec *kgo.Record) error {
	w.cl.PauseFetchPartitions(map[string][]int32{rec.Topic: {rec.Partition}})
	// SetOffsets also resets what the client believes is committed, so
	// checkpoint the marked progress first or a crash would redo it.
	if err := w.cl.CommitMarkedOffsets(ctx); err != nil {
		return err
	}
	w.cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		rec.Topic: {rec.Partition: {Epoch: rec.LeaderEpoch, Offset: rec.Offset}},
	})
	return nil
}

func resumeDue(cl *kgo.Client, due map[int32]time.Time) {
	now := time.Now()
	for p, at := range due {
		if !at.After(now) {
			cl.ResumeFetchPartitions(map[string][]int32{retryTopic: {p}})
			delete(due, p)
		}
	}
}

// pollContext bounds the next poll by the earliest due time, so a paused
// partition is resumed on time even when no other partition has records.
func pollContext(ctx context.Context, due map[int32]time.Time) (context.Context, context.CancelFunc) {
	if len(due) == 0 {
		return context.WithCancel(ctx)
	}
	var next time.Time
	for _, at := range due {
		if next.IsZero() || at.Before(next) {
			next = at
		}
	}
	return context.WithDeadline(ctx, next)
}

func retryAt(rec *kgo.Record) time.Time {
	at, err := time.Parse(time.RFC3339Nano, header(rec, hdrRetryAt))
	if err != nil {
		return time.Time{} // missing or garbled: due now
	}
	return at
}
