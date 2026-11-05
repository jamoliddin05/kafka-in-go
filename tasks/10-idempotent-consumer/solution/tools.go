package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"idiomaticGo/kafka/internal/kit"
)

const schema = `
CREATE TABLE IF NOT EXISTS t10_accounts (
    account_id TEXT PRIMARY KEY,
    balance    BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS t10_inbox (
    event_id     TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS t10_offsets (
    consumer_group  TEXT   NOT NULL,
    topic           TEXT   NOT NULL,
    kafka_partition INT    NOT NULL,
    next_offset     BIGINT NOT NULL,
    PRIMARY KEY (consumer_group, topic, kafka_partition)
);`

// ensureSchema is the "migration on startup" most services start with.
func ensureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		// Replicas starting together race on CREATE TABLE IF NOT EXISTS
		// (a duplicate key error on pg_type); the advisory lock serializes them.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(10)`); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, schema)
		return err
	})
}

// ensureTopic creates the topic with cl, or with a short-lived client if cl is nil.
func ensureTopic(ctx context.Context, app *kit.App, cl *kgo.Client) error {
	if cl == nil {
		tmp, err := app.NewClient()
		if err != nil {
			return err
		}
		defer tmp.Close()
		cl = tmp
	}
	return app.EnsureTopic(ctx, cl, kit.Topic{Name: depositsTopic, Partitions: 3})
}

// check recomputes what every balance should be from the log itself: the sum
// of each event_id once. The topic is the source of truth, the table a projection.
func check(ctx context.Context, app *kit.App) error {
	recs, ends, err := readAll(ctx, app, depositsTopic)
	if err != nil {
		return err
	}
	unique := make(map[string]Deposit, len(recs))
	var everyRecord int64
	for _, r := range recs {
		var d Deposit
		if err := json.Unmarshal(r.Value, &d); err != nil {
			return fmt.Errorf("decode p%d@%d: %w", r.Partition, r.Offset, err)
		}
		everyRecord += d.Amount
		unique[d.EventID] = d
	}
	expected := make(map[string]int64)
	for _, d := range unique {
		expected[d.AccountID] += d.Amount
	}

	pool, err := app.ConnectDB(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := ensureSchema(ctx, pool); err != nil {
		return err
	}
	actual, err := balances(ctx, pool)
	if err != nil {
		return err
	}
	var inboxRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM t10_inbox`).Scan(&inboxRows); err != nil {
		return err
	}

	fmt.Printf("%s: %d records, %d unique event_ids, %d re-sent copies; t10_inbox has %d rows\n",
		depositsTopic, len(recs), len(unique), len(recs)-len(unique), inboxRows)
	kafkaLag := kafkaLag(ctx, app)
	pgLag, err := postgresLag(ctx, pool, ends)
	if err != nil {
		return err
	}
	fmt.Printf("lag of %s: %s by Kafka commits, %s by Postgres offsets\n\n", ledgerGroup, kafkaLag, pgLag)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(w, "ACCOUNT\tEXPECTED\tACTUAL\tDIFF\t")
	var expTotal, actTotal int64
	wrong := 0
	all := maps.Clone(actual)
	maps.Copy(all, expected)
	ids := slices.Sorted(maps.Keys(all))
	for _, id := range ids {
		e, a := expected[id], actual[id]
		expTotal += e
		actTotal += a
		if e != a {
			wrong++
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t\n", id, money(e), money(a), signed(a-e))
	}
	fmt.Fprintf(w, "TOTAL\t%s\t%s\t%s\t\n", money(expTotal), money(actTotal), signed(actTotal-expTotal))
	if err := w.Flush(); err != nil {
		return err
	}

	fmt.Printf("\nsum of every record on the log, duplicates included: %s\n", money(everyRecord))
	if wrong == 0 {
		fmt.Println("OK: every balance equals the sum of its unique deposits")
	} else {
		fmt.Printf("WRONG: %d of %d accounts differ, total off by %s\n", wrong, len(ids), signed(actTotal-expTotal))
	}
	return nil
}

func balances(ctx context.Context, pool *pgxpool.Pool) (map[string]int64, error) {
	rows, err := pool.Query(ctx, `SELECT account_id, balance FROM t10_accounts`)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64)
	var id string
	var balance int64
	_, err = pgx.ForEachRow(rows, []any{&id, &balance}, func() error {
		out[id] = balance
		return nil
	})
	return out, err
}

func kafkaLag(ctx context.Context, app *kit.App) string {
	cl, err := app.NewClient()
	if err != nil {
		return err.Error()
	}
	defer cl.Close()
	lags, err := kadm.NewClient(cl).Lag(ctx, ledgerGroup)
	if err != nil {
		return err.Error()
	}
	l := lags[ledgerGroup]
	if err := l.Error(); err != nil {
		return err.Error()
	}
	return fmt.Sprintf("%d", l.Lag.Total())
}

func postgresLag(ctx context.Context, pool *pgxpool.Pool, ends map[int32]int64) (string, error) {
	rows, err := pool.Query(ctx, `SELECT kafka_partition, next_offset FROM t10_offsets WHERE consumer_group = $1 AND topic = $2`,
		ledgerGroup, depositsTopic)
	if err != nil {
		return "", err
	}
	var p int32
	var next, lag int64
	n := 0
	_, err = pgx.ForEachRow(rows, []any{&p, &next}, func() error {
		lag += max(ends[p]-next, 0)
		n++
		return nil
	})
	if err != nil || n == 0 {
		return "n/a", err
	}
	return fmt.Sprintf("%d", lag), nil
}

// readAll reads a topic from the log start up to the end offsets captured
// when it starts, without a group. Records produced meanwhile are ignored.
func readAll(ctx context.Context, app *kit.App, topic string) ([]*kgo.Record, map[int32]int64, error) {
	cl, err := app.NewClient(kgo.ConsumeTopics(topic))
	if err != nil {
		return nil, nil, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	starts, err := adm.ListStartOffsets(ctx, topic)
	if err == nil {
		err = starts.Error()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("list offsets of %s (run the producer first?): %w", topic, err)
	}
	listed, err := adm.ListEndOffsets(ctx, topic)
	if err == nil {
		err = listed.Error()
	}
	if err != nil {
		return nil, nil, err
	}
	ends := make(map[int32]int64)
	remaining := make(map[int32]int64)
	listed.Each(func(o kadm.ListedOffset) {
		ends[o.Partition] = o.Offset
		if s, _ := starts.Lookup(topic, o.Partition); o.Offset > s.Offset {
			remaining[o.Partition] = o.Offset
		}
	})

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(remaining) > 0 {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil, nil, fmt.Errorf("read %s: %w, still waiting for %v", topic, ctx.Err(), remaining)
		}
		if err := fetches.Err(); err != nil {
			return nil, nil, err
		}
		for r := range fetches.RecordsAll() {
			end, ok := remaining[r.Partition]
			if !ok || r.Offset >= end {
				continue
			}
			out = append(out, r)
			if r.Offset+1 >= end {
				delete(remaining, r.Partition)
			}
		}
	}
	return out, ends, nil
}

// reset empties the tables and moves the log start of the topic to its end,
// so check starts from a clean slate. Stop the producer first.
func reset(ctx context.Context, app *kit.App) error {
	pool, err := app.ConnectDB(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := ensureSchema(ctx, pool); err != nil {
		return err
	}
	if _, err := pool.Exec(ctx, `TRUNCATE t10_accounts, t10_inbox, t10_offsets`); err != nil {
		return err
	}

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ends, err := adm.ListEndOffsets(ctx, depositsTopic)
	if err == nil {
		err = ends.Error()
	}
	if errors.Is(err, kerr.UnknownTopicOrPartition) {
		app.Log.Info("reset: tables truncated, topic does not exist yet", "topic", depositsTopic)
		return nil
	}
	if err != nil {
		return err
	}
	// DeleteRecords moves the log start offset: what kafka-delete-records.sh does.
	deleted, err := adm.DeleteRecords(ctx, ends.Offsets())
	if err == nil {
		err = deleted.Error()
	}
	if err != nil {
		return err
	}
	newStart := make(map[int32]int64)
	ends.Each(func(o kadm.ListedOffset) { newStart[o.Partition] = o.Offset })
	app.Log.Info("reset: tables truncated, records deleted", "topic", depositsTopic, "new_log_start", newStart)
	return nil
}

func money(cents int64) string {
	sign := ""
	if cents < 0 {
		sign, cents = "-", -cents
	}
	return fmt.Sprintf("%s%d.%02d", sign, cents/100, cents%100)
}

func signed(cents int64) string {
	if cents > 0 {
		return "+" + money(cents)
	}
	return money(cents)
}
