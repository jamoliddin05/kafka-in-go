// Task 16: share groups. Read README.md first; the TODOs follow its "Your task" steps.
package main

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"idiomaticGo/kafka/internal/kit"
)

const (
	jobsTopic = "t16.jobs"
	// GROUP overrides it, so you can watch a brand-new share group start at "latest".
	defaultGroup = "t16.workers"
)

var errTODO = errors.New("not implemented yet, see README.md")

func main() {
	kit.Main(map[string]kit.Role{
		"producer": produce,
		"consumer": consume,
		"config":   configure,
		"inspect":  inspect,
	})
}

// Job is one unit of work. Kind decides how the worker behaves, so every
// outcome of a share-group delivery can be triggered on purpose.
type Job struct {
	ID   string    `json:"id"`
	Kind string    `json:"kind"`           // ok | flaky | poison
	Fail int32     `json:"fail,omitempty"` // flaky: fails on deliveries 1..Fail
	At   time.Time `json:"at"`
}

func ensureTopic(ctx context.Context, app *kit.App, cl *kgo.Client) error {
	// Two partitions on purpose: the lesson is that six workers can share them.
	return app.EnsureTopic(ctx, cl, kit.Topic{Name: jobsTopic, Partitions: 2})
}

// ---------------------------------------------------------------- producer

func produce(ctx context.Context, app *kit.App) error {
	interval := kit.EnvDuration("INTERVAL", 100*time.Millisecond)
	count := kit.EnvInt("COUNT", 0)
	poisonPct := kit.EnvInt("POISON_PCT", 2)
	transientPct := kit.EnvInt("TRANSIENT_PCT", 10)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := ensureTopic(ctx, app, cl); err != nil {
		return err
	}

	tick := time.NewTicker(interval)
	defer tick.Stop()
	for i := 1; count == 0 || i <= count; i++ {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}

		job := Job{ID: "job-" + crand.Text()[:8], Kind: "ok", At: time.Now()}
		var value []byte
		switch n := rand.IntN(100); {
		case n < poisonPct:
			job.Kind = "poison"
			value = []byte(`{"id":"` + job.ID + `","kind":`) // truncated JSON: no retry will ever fix it
		case n < poisonPct+transientPct:
			job.Kind, job.Fail = "flaky", 1+rand.Int32N(6)
		}
		if value == nil {
			if value, err = json.Marshal(job); err != nil {
				return err
			}
		}

		rec := &kgo.Record{Topic: jobsTopic, Key: []byte(job.ID), Value: value}
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			app.Log.Error("produce failed", "err", err)
			continue
		}
		app.Log.Info("job queued", "id", job.ID, "kind", job.Kind, "fail", job.Fail, "partition", rec.Partition, "offset", rec.Offset)
	}
	return nil
}

// ---------------------------------------------------------------- consumer

var (
	errTransient = errors.New("downstream unavailable")
	errPoison    = errors.New("cannot decode job")
)

func consume(ctx context.Context, app *kit.App) error {
	work := kit.EnvDuration("WORK", 200*time.Millisecond)
	maxRecords := kit.EnvInt("MAX_RECORDS", 5)
	crashPct := kit.EnvInt("CRASH_PCT", 0)
	stallPct := kit.EnvInt("STALL_PCT", 0)
	stall := kit.EnvDuration("STALL", 45*time.Second)
	group := kit.Env("GROUP", defaultGroup)

	cl, err := app.NewClient(
		kgo.ShareGroup(group),
		kgo.ConsumeTopics(jobsTopic),
		// Every record handed to us is locked for us until we ack it or the
		// lock expires, so take only what we can finish well within the lock.
		kgo.ShareMaxRecords(int32(maxRecords)),
		// TODO(16.3): add kgo.ShareAckCallback and log every result whose Err
		// is not nil. Without it, failed acks are invisible.
	)
	if err != nil {
		return err
	}
	defer cl.Close() // leaves the group and releases anything polled but not acked
	if err := ensureTopic(ctx, app, cl); err != nil {
		return err
	}

	app.Log.Info("consuming", "group", group, "topic", jobsTopic, "work", work, "max_records", maxRecords)
	for {
		fetches := cl.PollRecords(ctx, maxRecords)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})

		recs := fetches.Records()
		app.Log.Debug("polled", "records", held(recs))
		for i, r := range recs {
			if ctx.Err() != nil {
				return nil // the rest is still pending: Close releases it
			}
			switch n := rand.IntN(100); {
			case n < crashPct:
				app.Log.Error("simulated crash: exiting without acking or leaving", "holding", held(recs[i:]))
				os.Exit(3)
			case n < crashPct+stallPct:
				app.Log.Warn("simulated stall: holding records without acking", "holding", held(recs[i:]), "for", stall)
				if !sleep(ctx, stall) {
					return nil
				}
			}

			err := handle(ctx, r, work)
			if ctx.Err() != nil {
				return nil // interrupted mid-job: Close releases it with the rest
			}
			// TODO(16.2): tell the broker what happened: r.Ack with AckAccept,
			// AckRelease or AckReject depending on err. Until you do, the next
			// poll silently accepts every record, failures included.
			report(app, r, err, "none")
		}
	}
}

func report(app *kit.App, r *kgo.Record, err error, ack string) {
	attrs := []any{"key", string(r.Key), "partition", r.Partition, "offset", r.Offset,
		"delivery", r.DeliveryCount(), "ack", ack}
	if err != nil {
		app.Log.Warn("job failed", append(attrs, "err", err)...)
		return
	}
	app.Log.Info("job done", attrs...)
}

// held lists records as partition/offset, so you can follow them to their next delivery.
func held(rs []*kgo.Record) string {
	var b strings.Builder
	for _, r := range rs {
		fmt.Fprintf(&b, "%d/%d ", r.Partition, r.Offset)
	}
	return strings.TrimSpace(b.String())
}

// handle pretends to do the job. flaky jobs fail until their delivery count
// passes Fail; poison jobs can never succeed.
func handle(ctx context.Context, r *kgo.Record, work time.Duration) error {
	var job Job
	if err := json.Unmarshal(r.Value, &job); err != nil {
		return fmt.Errorf("%w: %v", errPoison, err)
	}
	if !sleep(ctx, work) {
		return ctx.Err()
	}
	if job.Kind == "flaky" && r.DeliveryCount() <= job.Fail {
		return fmt.Errorf("%w (fails %d times)", errTransient, job.Fail)
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// ---------------------------------------------------------------- config

// configure sets group-level configs. Share groups keep their settings on the
// group itself (resource type GROUP), not in the client.
func configure(ctx context.Context, app *kit.App) error {
	group := kit.Env("GROUP", defaultGroup)
	set := map[string]string{"share.auto.offset.reset": kit.Env("RESET", "earliest")}
	if v := kit.Env("LOCK_MS", ""); v != "" {
		set["share.record.lock.duration.ms"] = v
	}
	if v := kit.Env("DELIVERY_LIMIT", ""); v != "" {
		set["share.delivery.count.limit"] = v
	}

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()

	// TODO(16.1): send a kmsg.IncrementalAlterConfigsRequest with one resource
	// of type kmsg.ConfigResourceTypeGroupConfig named group, one config entry
	// (op Set) per key in set. Check every resource's ErrorCode, then call
	// printGroupConfigs: it shows the DescribeConfigs side of the same API.
	_, _ = group, set
	return errTODO
}

func printGroupConfigs(ctx context.Context, cl *kgo.Client, group string) error {
	req := kmsg.NewPtrDescribeConfigsRequest()
	res := kmsg.NewDescribeConfigsRequestResource()
	res.ResourceType = kmsg.ConfigResourceTypeGroupConfig
	res.ResourceName = group
	req.Resources = append(req.Resources, res)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "GROUP CONFIG\tVALUE\tSOURCE\n")
	for _, r := range resp.Resources {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			return fmt.Errorf("describe group %s configs: %w", r.ResourceName, err)
		}
		for _, c := range r.Configs {
			if strings.HasPrefix(c.Name, "share.") {
				fmt.Fprintf(w, "%s\t%s\t%s\n", c.Name, deref(c.Value), c.Source)
			}
		}
	}
	return w.Flush()
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// ---------------------------------------------------------------- inspect

func inspect(ctx context.Context, app *kit.App) error {
	group := kit.Env("GROUP", defaultGroup)
	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	// TODO(16.4): fill g with adm.DescribeShareGroups (check g.Err) and
	// offsets with adm.DescribeShareGroupOffsets. Then run it with 1, 2 and 6
	// consumers and compare the ASSIGNED column.
	var g kadm.DescribedShareGroup
	var offsets kadm.DescribedShareGroupsOffsets
	ends, err := adm.ListEndOffsets(ctx, jobsTopic)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "share group %s  state=%s  epoch=%d  assignor=%s  coordinator=%d  members=%d\n\n",
		g.GroupID, g.GroupState, g.GroupEpoch, g.Assignor, g.Coordinator.NodeID, len(g.Members))
	fmt.Fprintf(w, "MEMBER\tCLIENT\tHOST\tASSIGNED\n")
	for _, m := range g.Members {
		var assigned []string
		for _, t := range m.Assignment.Sorted() {
			assigned = append(assigned, fmt.Sprintf("%s%v", t.Topic, t.Partitions))
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", m.MemberID, m.ClientID, m.ClientHost, strings.Join(assigned, " "))
	}
	fmt.Fprintf(w, "\nPARTITION\tSTART OFFSET (SPSO)\tEND\tLAG\n")
	for _, p := range slices.Sorted(maps.Keys(ends[jobsTopic])) {
		e, _ := ends.Lookup(jobsTopic, p)
		o, ok := offsets[group].Offsets.Lookup(jobsTopic, p)
		if !ok {
			fmt.Fprintf(w, "%d\t-\t%d\t-\n", p, e.Offset)
			continue
		}
		fmt.Fprintf(w, "%d\t%d\t%d\t%d\n", p, o.StartOffset, e.Offset, o.Lag)
	}
	return w.Flush()
}
