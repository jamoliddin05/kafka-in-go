package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"idiomaticGo/kafka/internal/kit"
)

const schema = `
CREATE TABLE IF NOT EXISTS t11_orders (
    id         TEXT PRIMARY KEY,
    customer   TEXT NOT NULL,
    amount     BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS t11_outbox (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id      TEXT NOT NULL UNIQUE,
    topic         TEXT NOT NULL,
    record_key    TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    payload       JSONB NOT NULL,
    trace_context JSONB NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS t11_outbox_unpublished ON t11_outbox (id) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS t11_inbox (
    event_id     TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);`

// ensureSchema is the "migration on startup" most services start with. The
// partial index keeps the relay's "unpublished rows" query cheap however big
// the table gets.
func ensureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		// Replicas starting together race on CREATE TABLE IF NOT EXISTS
		// (a duplicate key error on pg_type); the advisory lock serializes them.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(11)`); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, schema)
		return err
	})
}

func ensureTopic(ctx context.Context, app *kit.App, cl *kgo.Client) error {
	return app.EnsureTopic(ctx, cl, kit.Topic{Name: eventsTopic, Partitions: 3})
}

// execer is what *pgxpool.Pool and pgx.Tx have in common.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// setupTracing wraps app.SetupTracing with a bounded flush for the defer.
func setupTracing(ctx context.Context, app *kit.App) (flush func(), err error) {
	shutdown, err := app.SetupTracing(ctx)
	if err != nil {
		return nil, err
	}
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdown(ctx); err != nil {
			app.Log.Warn("flushing spans failed", "err", err)
		}
	}, nil
}

// headerCarrier lets the OTel propagator read and write Kafka record headers (task 08).
type headerCarrier struct{ r *kgo.Record }

func (c headerCarrier) Get(key string) string {
	for _, h := range c.r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, value string) {
	c.r.Headers = slices.DeleteFunc(c.r.Headers, func(h kgo.RecordHeader) bool { return h.Key == key })
	c.r.Headers = append(c.r.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c headerCarrier) Keys() []string {
	keys := make([]string, len(c.r.Headers))
	for i, h := range c.r.Headers {
		keys[i] = h.Key
	}
	return keys
}

// startSend starts a PRODUCER span as a child of ctx and writes its context
// into the record's headers, so the consumer's span becomes its child. The
// span rides in r.Context until endSend.
func startSend(ctx context.Context, r *kgo.Record) {
	ctx, _ = tracer.Start(ctx, "send "+r.Topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation.type", "send"),
			attribute.String("messaging.destination.name", r.Topic),
			attribute.String("messaging.kafka.message.key", string(r.Key)),
		))
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{r})
	r.Context = ctx
}

// endSend records where the broker put the record, or why it failed.
func endSend(r *kgo.Record, err error) {
	span := trace.SpanFromContext(r.Context)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetAttributes(
			attribute.Int("messaging.destination.partition.id", int(r.Partition)),
			attribute.Int64("messaging.kafka.offset", r.Offset),
		)
	}
	span.End()
}

func eventRecord(ev OrderEvent) (*kgo.Record, error) {
	value, err := json.Marshal(ev)
	if err != nil {
		return nil, err
	}
	return &kgo.Record{
		Topic: eventsTopic,
		// Keyed by order: all events of one order stay in order on one partition.
		Key:   []byte(ev.OrderID),
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "event-id", Value: []byte(ev.EventID)},
			{Key: "event-type", Value: []byte(ev.Type)},
		},
	}, nil
}

// load fires ORDERS POST /orders requests at the api with CONCURRENCY workers.
func load(ctx context.Context, app *kit.App) error {
	url := kit.Env("API_URL", "http://localhost:8080") + "/orders"
	orders := kit.EnvInt("ORDERS", 200)
	concurrency := max(kit.EnvInt("CONCURRENCY", 8), 1)

	client := &http.Client{Timeout: 15 * time.Second}
	var mu sync.Mutex
	statuses := make(map[string]int)
	var lastTrace string
	jobs := make(chan struct{})
	var wg sync.WaitGroup
	for range concurrency {
		wg.Go(func() {
			for range jobs {
				status, traceID := postOrder(ctx, client, url)
				mu.Lock()
				statuses[status]++
				if traceID != "" {
					lastTrace = traceID
				}
				mu.Unlock()
			}
		})
	}

	started := time.Now()
send:
	for range orders {
		select {
		case jobs <- struct{}{}:
		case <-ctx.Done():
			break send
		}
	}
	close(jobs)
	wg.Wait()
	app.Log.Info("load done", "requests", orders, "took", time.Since(started).Round(time.Millisecond), "statuses", statuses)
	if lastTrace != "" {
		app.Log.Info("open one trace in Jaeger", "url", "http://localhost:16686/trace/"+lastTrace)
	}
	return nil
}

func postOrder(ctx context.Context, client *http.Client, url string) (status, traceID string) {
	body, _ := json.Marshal(map[string]any{
		"customer": fmt.Sprintf("cust-%03d", rand.IntN(500)),
		"amount":   100 * (1 + rand.Int64N(500)),
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "error", ""
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "error", ""
	}
	defer resp.Body.Close()
	var out struct {
		TraceID string `json:"trace_id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.Status, out.TraceID
}

// check compares the orders in Postgres with the events on the log. Lost and
// phantom events are bugs; duplicates are expected and the consumer's problem.
func check(ctx context.Context, app *kit.App) error {
	recs, err := readAll(ctx, app, eventsTopic)
	if err != nil {
		return err
	}
	eventsPerOrder := make(map[string]int)
	eventIDs := make(map[string]struct{})
	for _, r := range recs {
		var ev OrderEvent
		if err := json.Unmarshal(r.Value, &ev); err != nil {
			return fmt.Errorf("decode p%d@%d: %w", r.Partition, r.Offset, err)
		}
		eventsPerOrder[ev.OrderID]++
		eventIDs[ev.EventID] = struct{}{}
	}

	pool, err := app.ConnectDB(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := ensureSchema(ctx, pool); err != nil {
		return err
	}
	orders, err := column(ctx, pool, `SELECT id FROM t11_orders`)
	if err != nil {
		return err
	}
	unpublished, err := column(ctx, pool, `SELECT record_key FROM t11_outbox WHERE published_at IS NULL`)
	if err != nil {
		return err
	}

	inFlight := make(map[string]bool, len(unpublished))
	for _, id := range unpublished {
		inFlight[id] = true
	}
	inOrders := make(map[string]bool, len(orders))
	var lost []string
	for _, id := range orders {
		inOrders[id] = true
		if eventsPerOrder[id] == 0 && !inFlight[id] {
			lost = append(lost, id)
		}
	}
	var phantom []string
	for id := range eventsPerOrder {
		if !inOrders[id] {
			phantom = append(phantom, id)
		}
	}

	fmt.Printf("orders in t11_orders:               %d\n", len(orders))
	fmt.Printf("records in %s:       %d (%d distinct event_ids, %d duplicates)\n",
		eventsTopic, len(recs), len(eventIDs), len(recs)-len(eventIDs))
	fmt.Printf("outbox rows not published yet:      %d\n", len(unpublished))
	fmt.Printf("orders without an event (lost):     %d %s\n", len(lost), sample(lost))
	fmt.Printf("events without an order (phantom):  %d %s\n", len(phantom), sample(phantom))
	switch {
	case len(lost) > 0 || len(phantom) > 0:
		fmt.Println("WRONG: Postgres and Kafka disagree")
	case len(unpublished) > 0:
		fmt.Printf("OK so far: %d events are still waiting in the outbox\n", len(unpublished))
	default:
		fmt.Println("OK: every order has its event and every event has its order")
	}
	return nil
}

func column(ctx context.Context, pool *pgxpool.Pool, sql string) ([]string, error) {
	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func sample(ids []string) string {
	if len(ids) == 0 {
		return ""
	}
	slices.Sort(ids)
	return fmt.Sprintf("e.g. %v", ids[:min(len(ids), 3)])
}

// readAll reads a topic from the log start up to the end offsets captured
// when it starts, without a group. Records produced meanwhile are ignored.
func readAll(ctx context.Context, app *kit.App, topic string) ([]*kgo.Record, error) {
	cl, err := app.NewClient(kgo.ConsumeTopics(topic))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	starts, err := adm.ListStartOffsets(ctx, topic)
	if err == nil {
		err = starts.Error()
	}
	if err != nil {
		return nil, fmt.Errorf("list offsets of %s (start the api first?): %w", topic, err)
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return nil, err
	}
	remaining := make(map[int32]int64)
	ends.Each(func(o kadm.ListedOffset) {
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
			return nil, fmt.Errorf("read %s: %w, still waiting for %v", topic, ctx.Err(), remaining)
		}
		if err := fetches.Err(); err != nil {
			return nil, err
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
	return out, nil
}

// reset empties the tables and moves the topic's log start to its end, so
// check starts from a clean slate. Stop the api and the relay first.
func reset(ctx context.Context, app *kit.App) error {
	pool, err := app.ConnectDB(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := ensureSchema(ctx, pool); err != nil {
		return err
	}
	if _, err := pool.Exec(ctx, `TRUNCATE t11_orders, t11_outbox, t11_inbox RESTART IDENTITY`); err != nil {
		return err
	}

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ends, err := adm.ListEndOffsets(ctx, eventsTopic)
	if err == nil {
		err = ends.Error()
	}
	if errors.Is(err, kerr.UnknownTopicOrPartition) {
		app.Log.Info("reset: tables truncated, topic does not exist yet", "topic", eventsTopic)
		return nil
	}
	if err != nil {
		return err
	}
	deleted, err := adm.DeleteRecords(ctx, ends.Offsets())
	if err == nil {
		err = deleted.Error()
	}
	if err != nil {
		return err
	}
	app.Log.Info("reset: tables truncated, records deleted", "topic", eventsTopic)
	return nil
}

func newID() string { return crand.Text() }

func money(cents int64) string { return fmt.Sprintf("%d.%02d", cents/100, cents%100) }
