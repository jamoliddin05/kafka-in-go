// Reference solution for task 14. Try the task first: tasks/14-schema-evolution/README.md
package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"maps"
	mrand "math/rand/v2"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"idiomaticGo/kafka/internal/kit"
)

const (
	ordersTopic = "t14.orders"
	group       = "t14.billing"
)

func main() {
	kit.Main(map[string]kit.Role{
		"producer": produce,
		"consumer": consume,
		"replay":   replay,
	})
}

// releaseOf picks the release this process plays. One binary is all three
// releases, so VERSION is not optional here.
func releaseOf(app *kit.App) (release, error) {
	rel, ok := releases[app.Version]
	if !ok {
		return release{}, fmt.Errorf("VERSION=%q: set VERSION=v1|v2|v3, this binary plays all three releases", app.Version)
	}
	return rel, nil
}

func produce(ctx context.Context, app *kit.App) error {
	rel, err := releaseOf(app)
	if err != nil {
		return err
	}
	interval := kit.EnvDuration("INTERVAL", 500*time.Millisecond)

	cl, err := app.NewClient(kgo.DefaultProduceTopic(ordersTopic))
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := app.EnsureTopic(ctx, cl, kit.Topic{Name: ordersTopic, Partitions: 3}); err != nil {
		return err
	}

	env := Envelope{EventType: eventType, ContentType: contentType, SchemaVersion: rel.schemaVersion}
	run := strings.ToLower(rand.Text()[:4])
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for n := 0; ; n++ {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}

		o := newOrder(app.Version, fmt.Sprintf("%s-%05d", run, n))
		value, err := rel.write(o)
		if err != nil {
			return fmt.Errorf("encode as %s: %w", app.Version, err)
		}
		rec := &kgo.Record{Key: []byte(o.ID), Value: value, Headers: env.headers()}
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			app.Log.Error("produce failed", "err", err)
			continue
		}
		app.Log.Info("produced", "schema", rel.schemaVersion, "value", string(value))
	}
}

// newOrder only uses what the release knows about: v1 had no currencies
// and no refunds.
func newOrder(version, id string) Order {
	o := Order{ID: id, AmountMinor: 100 + mrand.Int64N(19900), Currency: "USD"}
	statuses := []Status{StatusPending, StatusPaid, StatusPaid}
	if version != "v1" {
		statuses = append(statuses, StatusRefunded)
		o.Currency = []string{"USD", "USD", "EUR", "GBP"}[mrand.IntN(4)]
	}
	o.Status = statuses[mrand.IntN(len(statuses))]
	return o
}

// stats is what a billing consumer would put on a dashboard.
type stats struct {
	bySchema   map[int]int
	errors     int
	unknown    int
	zeroAmount int
	revenue    map[string]int64
}

func newStats() *stats {
	return &stats{bySchema: map[int]int{}, revenue: map[string]int64{}}
}

func consume(ctx context.Context, app *kit.App) error {
	rel, err := releaseOf(app)
	if err != nil {
		return err
	}
	strict := kit.EnvBool("STRICT", false)
	every := kit.EnvDuration("SUMMARY_EVERY", 10*time.Second)

	cl, err := app.NewClient(kgo.ConsumerGroup(group), kgo.ConsumeTopics(ordersTopic))
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := app.EnsureTopic(ctx, cl, kit.Topic{Name: ordersTopic, Partitions: 3}); err != nil {
		return err
	}

	app.Log.Info("billing consumer started", "release", app.Version, "reads_up_to_schema", rel.schemaVersion, "strict", strict)
	st := newStats()
	summary := time.NewTicker(every)
	defer summary.Stop()
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		fetches.EachRecord(func(r *kgo.Record) { bill(app, rel, strict, st, r) })

		select {
		case <-summary.C:
			app.Log.Info("summary", "by_schema", st.bySchema, "decode_errors", st.errors,
				"unknown_status", st.unknown, "zero_amount", st.zeroAmount, "revenue_minor", st.revenue)
		default:
		}
	}
}

// bill is the business logic: the same in every release, only the reader differs.
func bill(app *kit.App, rel release, strict bool, st *stats, r *kgo.Record) {
	env := envelopeOf(r.Headers)
	st.bySchema[env.SchemaVersion]++
	lg := app.Log.With("partition", r.Partition, "offset", r.Offset, "schema", env.SchemaVersion)
	if env.SchemaVersion > rel.schemaVersion {
		lg = lg.With("newer_than_me", true)
	}

	o, err := rel.read(env, r.Value, strict)
	if err != nil {
		st.errors++
		lg.Error("cannot decode, skipping (in prod: DLQ, task 09)", "err", err, "value", string(r.Value))
		return
	}
	switch o.Status {
	case StatusPaid:
		st.revenue[o.Currency] += o.AmountMinor
	case StatusRefunded:
		st.revenue[o.Currency] -= o.AmountMinor
	case StatusPending:
	default:
		st.unknown++
		lg.Warn("unknown status, not billed", "order", o.ID, "status", o.Status)
		return
	}
	if o.AmountMinor == 0 {
		st.zeroAmount++
		lg.Warn("order with zero amount", "order", o.ID, "status", o.Status, "value", string(r.Value))
		return
	}
	lg.Info("billed", "order", o.ID, "status", o.Status, "amount", fmt.Sprintf("%d.%02d %s", o.AmountMinor/100, o.AmountMinor%100, o.Currency))
}

// replay runs every record still in the topic through this release's reader:
// the check to do before a consumer deploy, because retention keeps every
// version a producer ever wrote.
func replay(ctx context.Context, app *kit.App) error {
	rel, err := releaseOf(app)
	if err != nil {
		return err
	}
	strict := kit.EnvBool("STRICT", false)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	starts, err := adm.ListStartOffsets(ctx, ordersTopic)
	if err != nil {
		return err
	}
	ends, err := adm.ListEndOffsets(ctx, ordersTopic)
	if err != nil {
		return err
	}
	if err := ends.Error(); err != nil {
		return fmt.Errorf("%s (run the producer first?): %w", ordersTopic, err)
	}

	type row struct {
		records, errors, unknown, zero int
		sample                         string
	}
	rows := map[int]*row{}
	total := 0
	err = readAll(ctx, app, starts, ends, func(r *kgo.Record) {
		env := envelopeOf(r.Headers)
		rw := rows[env.SchemaVersion]
		if rw == nil {
			rw = &row{}
			rows[env.SchemaVersion] = rw
		}
		rw.records++
		total++
		o, err := rel.read(env, r.Value, strict)
		switch {
		case err != nil:
			rw.errors++
			if rw.sample == "" {
				rw.sample = err.Error()
			}
		case !o.Status.Known():
			rw.unknown++
		case o.AmountMinor == 0:
			rw.zero++
		}
	})
	if err != nil {
		return err
	}

	fmt.Printf("replay of %s through the %s reader (strict=%v): %d records\n\n", ordersTopic, app.Version, strict, total)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SCHEMA\tRECORDS\tDECODE ERRORS\tUNKNOWN STATUS\tZERO AMOUNT\tFIRST ERROR")
	for _, v := range slices.Sorted(maps.Keys(rows)) {
		rw := rows[v]
		fmt.Fprintf(w, "%d\t%d\t%d\t%d\t%d\t%s\n", v, rw.records, rw.errors, rw.unknown, rw.zero, rw.sample)
	}
	return w.Flush()
}

func readAll(ctx context.Context, app *kit.App, starts, ends kadm.ListedOffsets, fn func(*kgo.Record)) error {
	want := map[string]map[int32]kgo.Offset{}
	left := 0
	ends.Each(func(e kadm.ListedOffset) {
		s, _ := starts.Lookup(e.Topic, e.Partition)
		if s.Offset >= e.Offset {
			return
		}
		if want[e.Topic] == nil {
			want[e.Topic] = map[int32]kgo.Offset{}
		}
		want[e.Topic][e.Partition] = kgo.NewOffset().At(s.Offset)
		left++
	})
	if left == 0 {
		return nil
	}

	cl, err := app.NewClient(kgo.ConsumePartitions(want))
	if err != nil {
		return err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for left > 0 {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("reading %s: %w", ordersTopic, err)
		}
		fetches.EachError(func(t string, p int32, err error) {
			app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			e, _ := ends.Lookup(p.Topic, p.Partition)
			for _, r := range p.Records {
				if r.Offset >= e.Offset {
					continue
				}
				fn(r)
				if r.Offset == e.Offset-1 {
					left--
				}
			}
		})
	}
	return nil
}
