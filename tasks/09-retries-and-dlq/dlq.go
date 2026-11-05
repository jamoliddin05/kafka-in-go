package main

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"idiomaticGo/kafka/internal/kit"
)

// listDLQ prints every DLQ record with its headers, and the DLQ depth.
func listDLQ(ctx context.Context, app *kit.App) error {
	// TODO(09.5): read the DLQ from its start to its current end (bounds +
	// readRange) and print per record: partition@offset, key, the bookkeeping
	// headers, traceparent and the value. Records at or above the t09.redrive
	// group's committed offset (committed) wait for a redrive: count them.
	return errTODO
}

// redrive re-publishes DLQ records to the main topic, after the fix is deployed.
func redrive(ctx context.Context, app *kit.App) error {
	// TODO(09.6): read the DLQ from t09.redrive's committed offsets (or the log
	// start) to the current end. Produce a copy of each record to t09.orders:
	// same key, value and headers, minus retry-attempt, retry-at, error and
	// error-class, with redrive-count + 1; skip records already at MAX_REDRIVES.
	// Once everything is acked, commit the end offsets for t09.redrive with
	// kadm.CommitAllOffsets. DRY_RUN=true: produce nothing, commit nothing.
	return errTODO
}

// bounds returns the log start and end offsets of every partition of topic.
func bounds(ctx context.Context, adm *kadm.Client, topic string) (starts, ends map[int32]int64, err error) {
	s, err := adm.ListStartOffsets(ctx, topic)
	if err == nil {
		err = s.Error()
	}
	if err != nil {
		return nil, nil, err
	}
	e, err := adm.ListEndOffsets(ctx, topic)
	if err == nil {
		err = e.Error()
	}
	if err != nil {
		return nil, nil, err
	}
	starts, ends = map[int32]int64{}, map[int32]int64{}
	s.Each(func(o kadm.ListedOffset) { starts[o.Partition] = o.Offset })
	e.Each(func(o kadm.ListedOffset) { ends[o.Partition] = o.Offset })
	return starts, ends, nil
}

// committed returns group's committed offsets for topic; partitions without a commit are absent (0).
func committed(ctx context.Context, adm *kadm.Client, group, topic string) (map[int32]int64, error) {
	resps, err := adm.FetchOffsetsForTopics(ctx, group, topic)
	if err != nil {
		return nil, err
	}
	out := map[int32]int64{}
	resps.Each(func(r kadm.OffsetResponse) {
		if r.Err == nil && r.At >= 0 {
			out[r.Partition] = r.At
		}
	})
	return out, nil
}

// readRange reads [from[p], to[p]) of every partition of topic without a group
// and calls fn for each record in offset order per partition.
func readRange(ctx context.Context, app *kit.App, topic string, from, to map[int32]int64, fn func(*kgo.Record) error) error {
	todo := map[int32]kgo.Offset{}
	for p, end := range to {
		if from[p] < end {
			todo[p] = kgo.NewOffset().At(from[p])
		}
	}
	if len(todo) == 0 {
		return nil
	}

	cl, err := app.NewClient(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: todo}))
	if err != nil {
		return err
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(ctx, kit.EnvDuration("READ_TIMEOUT", 30*time.Second))
	defer cancel()
	for len(todo) > 0 {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("read %s: %w", topic, err)
		}
		if err := fetches.Err0(); err != nil {
			return err
		}
		for rec := range fetches.RecordsAll() {
			if _, ok := todo[rec.Partition]; !ok || rec.Offset >= to[rec.Partition] {
				continue
			}
			if err := fn(rec); err != nil {
				return err
			}
			if rec.Offset+1 >= to[rec.Partition] {
				delete(todo, rec.Partition)
			}
		}
	}
	return nil
}
