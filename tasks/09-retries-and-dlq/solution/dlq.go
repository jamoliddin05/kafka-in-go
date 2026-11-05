package main

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"idiomaticGo/kafka/internal/kit"
)

// listDLQ prints every DLQ record with its headers, and how many still wait
// for a redrive: that number is the DLQ depth you alert on.
func listDLQ(ctx context.Context, app *kit.App) error {
	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	starts, ends, err := bounds(ctx, adm, dlqTopic)
	if err != nil {
		return err
	}
	redriven, err := committed(ctx, adm, redriveGroup, dlqTopic)
	if err != nil {
		return err
	}

	total, pending := 0, 0
	byError := map[string]int{}
	err = readRange(ctx, app, dlqTopic, starts, ends, func(rec *kgo.Record) error {
		total++
		state := "redriven"
		if rec.Offset >= redriven[rec.Partition] {
			state = "PENDING"
			pending++
		}
		byError[header(rec, hdrErrorClass)+": "+header(rec, hdrError)]++
		fmt.Printf("%s/%d@%d key=%s class=%s attempt=%s redrives=%s from=%s/%s@%s first-failure=%s %s\n",
			rec.Topic, rec.Partition, rec.Offset, rec.Key,
			header(rec, hdrErrorClass), header(rec, hdrAttempt), cmpOr(header(rec, hdrRedriveCount), "0"),
			header(rec, hdrOriginalTopic), header(rec, hdrOriginalPartition), header(rec, hdrOriginalOffset),
			header(rec, hdrFirstFailure), state)
		fmt.Printf("  error: %s\n  traceparent: %s\n  value: %s\n", header(rec, hdrError), header(rec, "traceparent"), rec.Value)
		return nil
	})
	if err != nil {
		return err
	}

	fmt.Printf("\n%d records in %s, %d pending redrive\n", total, dlqTopic, pending)
	for _, e := range slices.Sorted(maps.Keys(byError)) {
		fmt.Printf("%5d  %s\n", byError[e], e)
	}
	return nil
}

// redrive re-publishes DLQ records that arrived since the last redrive to the
// main topic, then commits its position in the DLQ under its own group. Run it
// after the fix is deployed. A crash between produce and commit only means
// some records are redriven twice; the consumer must cope with that anyway.
func redrive(ctx context.Context, app *kit.App) error {
	dryRun := kit.EnvBool("DRY_RUN", false)
	maxRedrives := kit.EnvInt("MAX_REDRIVES", 3)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	starts, ends, err := bounds(ctx, adm, dlqTopic)
	if err != nil {
		return err
	}
	from, err := committed(ctx, adm, redriveGroup, dlqTopic)
	if err != nil {
		return err
	}
	for p, start := range starts {
		from[p] = max(from[p], start)
	}

	var out []*kgo.Record
	skipped := 0
	err = readRange(ctx, app, dlqTopic, from, ends, func(rec *kgo.Record) error {
		n := headerInt(rec, hdrRedriveCount)
		if n >= maxRedrives {
			skipped++
			app.Log.Warn("not redriving, it failed after every redrive so far", "key", string(rec.Key),
				"dlq_offset", rec.Offset, "redrives", n, "error", header(rec, hdrError))
			return nil
		}
		out = append(out, toMain(rec, n+1))
		return nil
	})
	if err != nil {
		return err
	}
	if dryRun {
		app.Log.Info("dry run: nothing produced, nothing committed", "would_redrive", len(out), "would_skip", skipped)
		return nil
	}

	if len(out) > 0 {
		if err := cl.ProduceSync(ctx, out...).FirstErr(); err != nil {
			return fmt.Errorf("redrive produce (nothing committed, safe to rerun): %w", err)
		}
	}
	var offsets kadm.Offsets
	for p, end := range ends {
		if end > from[p] {
			offsets.Add(kadm.Offset{Topic: dlqTopic, Partition: p, At: end, LeaderEpoch: -1})
		}
	}
	if err := adm.CommitAllOffsets(ctx, redriveGroup, offsets); err != nil {
		return fmt.Errorf("commit redrive position: %w", err)
	}
	app.Log.Info("redrive done", "redriven", len(out), "skipped", skipped, "to", ordersTopic)
	return nil
}

// toMain is the copy that goes back to the main topic: same key, value and
// trace headers, retry bookkeeping cleared, redrive counted.
func toMain(rec *kgo.Record, redrives int) *kgo.Record {
	out := &kgo.Record{
		Topic:   ordersTopic,
		Key:     rec.Key,
		Value:   rec.Value,
		Headers: slices.Clone(rec.Headers),
	}
	delHeader(out, hdrAttempt, hdrRetryAt, hdrError, hdrErrorClass)
	setHeader(out, hdrRedriveCount, strconv.Itoa(redrives))
	return out
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

func cmpOr(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
