package main

import (
	"context"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"idiomaticGo/kafka/internal/kit"
)

// retryWorker consumes the retry topic and gives every record one more attempt.
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
	for {
		// TODO(09.4): resume partitions whose first record is due now, and bound
		// this poll by the earliest due time (context.WithDeadline), so a paused
		// partition comes back on time even if nothing else arrives.
		fetches := cl.PollFetches(ctx)
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
				// TODO(09.4): if retryAt(rec) is in the future, don't process it and
				// don't sleep: pause the partition, commit what's marked, rewind the
				// partition to rec with SetOffsets, remember when it's due, return.
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

func retryAt(rec *kgo.Record) time.Time {
	at, err := time.Parse(time.RFC3339Nano, header(rec, hdrRetryAt))
	if err != nil {
		return time.Time{} // missing or garbled: due now
	}
	return at
}
