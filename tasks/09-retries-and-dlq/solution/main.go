// Reference solution for task 09. Try the task first: tasks/09-retries-and-dlq/README.md
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"idiomaticGo/kafka/internal/kit"
)

const (
	ordersTopic  = "t09.orders"
	retryTopic   = "t09.orders.retry"
	dlqTopic     = "t09.orders.dlq"
	ordersGroup  = "t09.orders"
	retryGroup   = "t09.orders.retry"
	redriveGroup = "t09.redrive"
)

func main() {
	kit.Main(map[string]kit.Role{
		"producer":  produce,
		"consumer":  consume,
		"processor": retryWorker,
		"dlq":       listDLQ,
		"redrive":   redrive,
		"lag":       showLag,
	})
}

type Order struct {
	ID       string    `json:"id"`
	Customer string    `json:"customer"`
	Amount   int64     `json:"amount_cents"`
	At       time.Time `json:"at"`
	// Simulation only: the payment gateway answers 503 for this order until then.
	GatewayDownUntil time.Time `json:"gateway_down_until,omitzero"`
}

// orderV2 is what an upstream team started sending: the amount as a decimal string.
type orderV2 struct {
	ID       string    `json:"id"`
	Customer string    `json:"customer"`
	Amount   string    `json:"amount_cents"`
	At       time.Time `json:"at"`
}

type config struct {
	localRetries int           // in-place retries in the main consumer before rerouting
	localBackoff time.Duration // pause between in-place retries
	maxAttempts  int           // trips through the retry topic before the DLQ
	retryBackoff time.Duration // delay before retry attempt 1, doubled for every next one
	maxBackoff   time.Duration
	lenient      bool // the hotfix: accept "19.99" amounts from v2 producers
}

func loadConfig() config {
	return config{
		localRetries: kit.EnvInt("LOCAL_RETRIES", 2),
		localBackoff: kit.EnvDuration("LOCAL_BACKOFF", 200*time.Millisecond),
		maxAttempts:  kit.EnvInt("MAX_ATTEMPTS", 4),
		retryBackoff: kit.EnvDuration("RETRY_BACKOFF", time.Second),
		maxBackoff:   kit.EnvDuration("MAX_BACKOFF", 30*time.Second),
		lenient:      kit.EnvBool("LENIENT_AMOUNT", false),
	}
}

// worker is what the consumer and the retry worker share.
type worker struct {
	app *kit.App
	cl  *kgo.Client
	cfg config
}

func ensureTopics(ctx context.Context, app *kit.App, cl *kgo.Client) error {
	for _, t := range []kit.Topic{
		{Name: ordersTopic, Partitions: 3},
		{Name: retryTopic, Partitions: 3},
		{Name: dlqTopic, Partitions: 1},
	} {
		if err := app.EnsureTopic(ctx, cl, t); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------- producer

func produce(ctx context.Context, app *kit.App) error {
	interval := kit.EnvDuration("INTERVAL", 200*time.Millisecond)
	poisonPct := kit.EnvInt("POISON_PCT", 1)
	transientPct := kit.EnvInt("TRANSIENT_PCT", 10)
	transientFor := kit.EnvDuration("TRANSIENT_FOR", 3*time.Second)
	customers := kit.EnvInt("CUSTOMERS", 12)

	flush, err := startTracing(ctx, app)
	if err != nil {
		return err
	}
	defer flush()

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
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}

		o := Order{
			ID:       "o-" + strings.ToLower(rand.Text()[:10]),
			Customer: fmt.Sprintf("c-%d", mrand.IntN(customers)),
			Amount:   int64(500 + mrand.IntN(20000)),
			At:       time.Now().UTC(),
		}
		kind := "ok"
		var value []byte
		switch roll := mrand.IntN(100); {
		case roll < poisonPct:
			kind = "poison"
			value, err = json.Marshal(orderV2{
				ID: o.ID, Customer: o.Customer, At: o.At,
				Amount: fmt.Sprintf("%d.%02d", o.Amount/100, o.Amount%100),
			})
		case roll < poisonPct+transientPct:
			kind = "transient"
			o.GatewayDownUntil = o.At.Add(transientFor)
			value, err = json.Marshal(o)
		default:
			value, err = json.Marshal(o)
		}
		if err != nil {
			return err
		}

		rec := &kgo.Record{Topic: ordersTopic, Key: []byte(o.Customer), Value: value}
		if err := sendTraced(ctx, cl, rec); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			app.Log.Error("produce failed", "err", err)
			continue
		}
		level := slog.LevelDebug
		if kind != "ok" {
			level = slog.LevelInfo
		}
		app.Log.Log(ctx, level, "produced", "kind", kind, "order_id", o.ID, "key", o.Customer,
			"partition", rec.Partition, "offset", rec.Offset)
	}
}

// ---------------------------------------------------------------- consumer

func consume(ctx context.Context, app *kit.App) error {
	flush, err := startTracing(ctx, app)
	if err != nil {
		return err
	}
	defer flush()

	cl, err := app.NewClient(
		kgo.ConsumeTopics(ordersTopic),
		kgo.ConsumerGroup(ordersGroup),
		kgo.DisableAutoCommit(),
		// Commit and rebalance never interleave: we only commit partitions we still own.
		kgo.BlockRebalanceOnPoll(),
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
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		for rec := range fetches.RecordsAll() {
			// An error means the record is neither done nor parked: exit without
			// committing, and it is read again after the restart.
			if err := w.handleMain(ctx, rec); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
		if err := cl.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			app.Log.Error("commit failed", "err", err)
		}
		cl.AllowRebalance()
	}
}

// handleMain retries transient errors a few times in place, which keeps order
// and absorbs blips, then moves the record out of the way so the partition
// keeps flowing: to the retry topic, or straight to the DLQ if retrying is pointless.
func (w *worker) handleMain(ctx context.Context, rec *kgo.Record) error {
	ctx, span := startProcess(ctx, rec, ordersGroup, 0)
	defer span.End()

	var err error
	for i := 0; ; i++ {
		if err = w.process(ctx, rec, 0); err == nil {
			return nil
		}
		if classify(err) == permanent || i >= w.cfg.localRetries {
			break
		}
		if err := sleep(ctx, w.cfg.localBackoff); err != nil {
			return err
		}
	}
	failSpan(span, err)
	if classify(err) == permanent {
		return w.park(ctx, toDLQ(rec, err, 0))
	}
	return w.park(ctx, toRetry(rec, err, 1, w.cfg.backoff(1)))
}

// ---------------------------------------------------------------- lag

// showLag prints committed offset, end offset and lag per partition of GROUP.
func showLag(ctx context.Context, app *kit.App) error {
	group := kit.Env("GROUP", ordersGroup)
	every := kit.EnvDuration("WATCH", 0)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	for {
		if err := printLag(ctx, adm, group); err != nil {
			return err
		}
		if every == 0 {
			return nil
		}
		if err := sleep(ctx, every); err != nil {
			return nil
		}
	}
}

func printLag(ctx context.Context, adm *kadm.Client, group string) error {
	lags, err := adm.Lag(ctx, group)
	if err != nil {
		return err
	}
	l := lags[group]
	now := time.Now().Format("15:04:05")
	if err := l.Error(); errors.Is(err, kerr.GroupIDNotFound) {
		fmt.Printf("%s  group %s does not exist yet: no member joined, nothing committed\n", now, group)
		return nil
	} else if err != nil {
		return err
	}

	fmt.Printf("%s  group %s (%s)\n", now, group, l.State)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TOPIC\tPARTITION\tCOMMITTED\tEND\tLAG\tMEMBER")
	for _, r := range l.Lag.Sorted() {
		member := "-"
		if r.Member != nil {
			member = r.Member.MemberID[max(0, len(r.Member.MemberID)-8):]
		}
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%s\n", r.Topic, r.Partition, r.Commit.At, r.End.Offset, r.Lag, member)
	}
	return w.Flush()
}

// ---------------------------------------------------------------- helpers

// sleep waits for d or until ctx is done, whichever comes first.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
