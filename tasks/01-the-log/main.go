// Task 01: the log. Read README.md first; the TODOs follow its "Your task" steps.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
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
	eventsTopic    = "t01.events"
	retentionTopic = "t01.short-retention"
)

var errTODO = errors.New("not implemented yet, see README.md")

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
		_, _ = topic, value

		// TODO(01.1): build a *kgo.Record for `topic` with key = ev.UserID,
		// value = value and two headers: "event-type" and "producer" (app.Instance).
		// Send it with cl.ProduceSync and log the partition and offset the broker
		// assigned. On error: return nil if ctx is done, otherwise log and continue.
		return errTODO
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
			"segment.ms":   "10000",
			"retention.ms": "30000",
		},
	})
}

// read consumes without a consumer group, like Pulsar's Reader API.
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

	// TODO(01.3): loop on cl.PollFetches(ctx) until ctx is done or the client is
	// closed. Log fetch errors (fetches.EachError) and every record with its
	// partition, offset, key, timestamp and value.
	return errTODO
}

// parseFrom understands: start | end | offset:<n> | ago:<duration>.
func parseFrom(s string) (kgo.Offset, error) {
	switch {
	case s == "start":
		return kgo.NewOffset().AtStart(), nil
	case s == "end":
		return kgo.NewOffset().AtEnd(), nil
	case strings.HasPrefix(s, "offset:"):
		// TODO(01.2): parse the number and return kgo.NewOffset().At(n).
		return kgo.Offset{}, errTODO
	case strings.HasPrefix(s, "ago:"):
		// TODO(01.2): parse the duration and return the offset of the first
		// record at or after now-d. Hint: kgo.NewOffset().AfterMilli(...).
		return kgo.Offset{}, errTODO
	}
	return kgo.Offset{}, fmt.Errorf("bad FROM %q, want start|end|offset:N|ago:5m", s)
}

// inspect prints the shape of the log per partition.
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

	// TODO(01.4): fill these three with adm.ListStartOffsets, adm.ListEndOffsets
	// and adm.ListOffsetsAfterMilli (now - ago). Then run the inspect role and
	// explain every column to yourself.
	var starts, ends, atTime kadm.ListedOffsets
	_ = ago

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
