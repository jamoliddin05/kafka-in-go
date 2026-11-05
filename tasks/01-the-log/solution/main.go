// Reference solution for task 01. Try the task first: tasks/01-the-log/README.md
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"idiomaticGo/kafka/internal/kit"
)

const (
	eventsTopic    = "t01.events"
	retentionTopic = "t01.short-retention"
)

func main() {
	kit.Main(map[string]kit.Role{
		"producer": produce,
		"consumer": read,
		"inspect":  inspect,
	})
}

type Event struct {
	ID     string    `json:"id"`
	UserID string    `json:"user_id"`
	Type   string    `json:"type"`
	At     time.Time `json:"at"`
}

func produce(ctx context.Context, app *kit.App) error {
	topic := kit.Env("TOPIC", eventsTopic)
	interval := kit.EnvDuration("INTERVAL", 500*time.Millisecond)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()

	if err := ensureTopics(ctx, app, cl); err != nil {
		return err
	}

	tick := time.NewTicker(interval)
	defer tick.Stop()
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}

		ev := Event{
			ID:     fmt.Sprintf("%s-%d", app.Instance, i),
			UserID: fmt.Sprintf("user-%d", i%5),
			Type:   "page_view",
			At:     time.Now(),
		}
		value, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		rec := &kgo.Record{
			Topic: topic,
			Key:   []byte(ev.UserID),
			Value: value,
			Headers: []kgo.RecordHeader{
				{Key: "event-type", Value: []byte(ev.Type)},
				{Key: "producer", Value: []byte(app.Instance)},
			},
		}

		// ProduceSync waits for the broker's ack, so Partition and Offset are
		// filled in on return. One round trip per record: fine here, slow in prod.
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			app.Log.Error("produce failed", "err", err)
			continue
		}
		app.Log.Info("produced", "topic", rec.Topic, "key", string(rec.Key), "partition", rec.Partition, "offset", rec.Offset)
	}
}

func ensureTopics(ctx context.Context, app *kit.App, cl *kgo.Client) error {
	if err := app.EnsureTopic(ctx, cl, kit.Topic{Name: eventsTopic, Partitions: 3}); err != nil {
		return err
	}
	return app.EnsureTopic(ctx, cl, kit.Topic{
		Name:       retentionTopic,
		Partitions: 1,
		Configs: map[string]string{
			// Roll a new segment every 10s and drop segments older than 30s.
			// Retention deletes whole closed segments, never single records.
			"segment.ms":   "10000",
			"retention.ms": "30000",
		},
	})
}

// read consumes without a consumer group: no membership, no committed
// offsets, nothing stored on the broker about this reader. The position is
// whatever we ask for. This is Pulsar's Reader API in Kafka terms.
func read(ctx context.Context, app *kit.App) error {
	topic := kit.Env("TOPIC", eventsTopic)
	from := kit.Env("FROM", "start")
	partition := kit.EnvInt("PARTITION", -1)

	offset, err := parseFrom(from)
	if err != nil {
		return err
	}

	var opts []kgo.Opt
	if partition >= 0 {
		opts = append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {int32(partition): offset},
		}))
	} else {
		opts = append(opts, kgo.ConsumeTopics(topic), kgo.ConsumeStartOffset(offset))
	}

	cl, err := app.NewClient(opts...)
	if err != nil {
		return err
	}
	defer cl.Close()

	app.Log.Info("reading", "topic", topic, "from", from, "partition", partition)
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			app.Log.Info("record",
				"partition", r.Partition,
				"offset", r.Offset,
				"key", string(r.Key),
				"ts", r.Timestamp.Format("15:04:05.000"),
				"headers", len(r.Headers),
				"value", string(r.Value),
			)
		})
	}
}

// parseFrom understands: start | end | offset:<n> | ago:<duration>.
func parseFrom(s string) (kgo.Offset, error) {
	switch {
	case s == "start":
		return kgo.NewOffset().AtStart(), nil
	case s == "end":
		return kgo.NewOffset().AtEnd(), nil
	case strings.HasPrefix(s, "offset:"):
		n, err := strconv.ParseInt(strings.TrimPrefix(s, "offset:"), 10, 64)
		if err != nil {
			return kgo.Offset{}, fmt.Errorf("bad FROM %q: %w", s, err)
		}
		return kgo.NewOffset().At(n), nil
	case strings.HasPrefix(s, "ago:"):
		d, err := time.ParseDuration(strings.TrimPrefix(s, "ago:"))
		if err != nil {
			return kgo.Offset{}, fmt.Errorf("bad FROM %q: %w", s, err)
		}
		// The broker answers this with its time index: the first offset whose
		// timestamp is >= the given time.
		return kgo.NewOffset().AfterMilli(time.Now().Add(-d).UnixMilli()), nil
	}
	return kgo.Offset{}, fmt.Errorf("bad FROM %q, want start|end|offset:N|ago:5m", s)
}

// inspect prints, per partition: where the log starts (retention moves it),
// where it ends (the high watermark), which offset was current AGO ago,
// and who leads the partition / which replicas are in sync.
func inspect(ctx context.Context, app *kit.App) error {
	topic := kit.Env("TOPIC", eventsTopic)
	ago := kit.EnvDuration("AGO", time.Minute)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	details, err := adm.ListTopics(ctx, topic)
	if err != nil {
		return err
	}
	if err := details.Error(); err != nil {
		return err
	}
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return err
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return err
	}
	atTime, err := adm.ListOffsetsAfterMilli(ctx, time.Now().Add(-ago).UnixMilli(), topic)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "PARTITION\tSTART\tEND (HW)\tRECORDS\tOFFSET %s AGO\tLEADER\tREPLICAS\tISR\n", ago)
	partitions := details[topic].Partitions
	for _, p := range slices.Sorted(maps.Keys(partitions)) {
		s, _ := starts.Lookup(topic, p)
		e, _ := ends.Lookup(topic, p)
		t, _ := atTime.Lookup(topic, p)
		pd := partitions[p]
		fmt.Fprintf(w, "%d\t%d\t%d\t%d\t%d\t%d\t%v\t%v\n",
			p, s.Offset, e.Offset, e.Offset-s.Offset, t.Offset, pd.Leader, pd.Replicas, pd.ISR)
	}
	return w.Flush()
}
