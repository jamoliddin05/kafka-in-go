// Task 11: transactional outbox. Read README.md first; the TODOs follow its "Your task" steps.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"idiomaticGo/kafka/internal/kit"
)

const (
	eventsTopic   = "t11.order-events"
	notifierGroup = "t11.notifier"
)

var (
	errTODO           = errors.New("not implemented yet, see README.md")
	errSimulatedCrash = errors.New("simulated crash")
	// The global provider is installed later by SetupTracing; otel's tracers delegate to it.
	tracer = otel.Tracer("idiomaticGo/kafka/t11")
)

func main() {
	kit.Main(map[string]kit.Role{
		"api":      serveAPI,
		"relay":    runRelay,
		"consumer": consume,
		"load":     load,
		"check":    check,
		"reset":    reset,
	})
}

type Order struct {
	ID       string
	Customer string
	Amount   int64 // cents
}

// OrderEvent is what downstream services see. EventID is minted together
// with the order and survives every republish, so consumers can dedupe.
type OrderEvent struct {
	EventID  string    `json:"event_id"`
	Type     string    `json:"type"`
	OrderID  string    `json:"order_id"`
	Customer string    `json:"customer"`
	Amount   int64     `json:"amount"`
	At       time.Time `json:"at"`
}

type api struct {
	app     *kit.App
	pool    *pgxpool.Pool
	cl      *kgo.Client
	mode    string
	failPct int
}

func serveAPI(ctx context.Context, app *kit.App) error {
	flush, err := setupTracing(ctx, app)
	if err != nil {
		return err
	}
	defer flush()
	pool, err := app.ConnectDB(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := ensureSchema(ctx, pool); err != nil {
		return err
	}
	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := ensureTopic(ctx, app, cl); err != nil {
		return err
	}

	a := &api{app: app, pool: pool, cl: cl, mode: kit.Env("WRITE_MODE", "db-first"), failPct: kit.EnvInt("FAIL_PCT", 10)}
	switch a.mode {
	case "db-first", "kafka-first", "outbox":
	default:
		return fmt.Errorf("bad WRITE_MODE %q, want db-first|kafka-first|outbox", a.mode)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", a.createOrder)
	srv := &http.Server{Addr: kit.Env("HTTP_ADDR", ":8080"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	app.Log.Info("listening", "addr", srv.Addr, "write_mode", a.mode, "fail_pct", a.failPct)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	// Let in-flight requests finish their writes before the pool and client close.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func (a *api) createOrder(w http.ResponseWriter, r *http.Request) {
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := tracer.Start(ctx, "POST /orders", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	var req struct {
		Customer string `json:"customer"`
		Amount   int64  `json:"amount"`
	}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	o := Order{ID: newID(), Customer: req.Customer, Amount: req.Amount}
	if o.Customer == "" {
		o.Customer = fmt.Sprintf("cust-%03d", rand.IntN(500))
	}
	if o.Amount <= 0 {
		o.Amount = 100 * (1 + rand.Int64N(500))
	}
	ev := OrderEvent{EventID: newID(), Type: "order.created", OrderID: o.ID, Customer: o.Customer, Amount: o.Amount, At: time.Now()}
	span.SetAttributes(attribute.String("order.id", o.ID))

	var err error
	switch a.mode {
	case "db-first":
		err = a.dbFirst(ctx, o, ev)
	case "kafka-first":
		err = a.kafkaFirst(ctx, o, ev)
	case "outbox":
		err = a.withOutbox(ctx, o, ev)
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		a.app.Log.ErrorContext(ctx, "create order failed", "order_id", o.ID, "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	a.app.Log.InfoContext(ctx, "order created", "order_id", o.ID, "event_id", ev.EventID, "write_mode", a.mode)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"order_id": o.ID,
		"event_id": ev.EventID,
		"trace_id": span.SpanContext().TraceID().String(),
	})
}

// crash decides whether this request "dies" between its two writes:
// OOM-kill, deploy, broker unreachable, DB timeout. The cause doesn't matter.
func (a *api) crash() bool { return rand.IntN(100) < a.failPct }

// dbFirst is the dual write most services start with: commit, then publish.
func (a *api) dbFirst(ctx context.Context, o Order, ev OrderEvent) error {
	if err := insertOrder(ctx, a.pool, o); err != nil {
		return err
	}
	if a.crash() {
		return fmt.Errorf("after the DB commit, before the produce: %w", errSimulatedCrash)
	}
	return a.publish(ctx, ev)
}

// kafkaFirst is the "fix" people try next: publish, then commit.
func (a *api) kafkaFirst(ctx context.Context, o Order, ev OrderEvent) error {
	if err := a.publish(ctx, ev); err != nil {
		return err
	}
	if a.crash() {
		return fmt.Errorf("after the produce, before the DB commit: %w", errSimulatedCrash)
	}
	return insertOrder(ctx, a.pool, o)
}

func (a *api) publish(ctx context.Context, ev OrderEvent) error {
	rec, err := eventRecord(ev)
	if err != nil {
		return err
	}
	startSend(ctx, rec)
	err = a.cl.ProduceSync(ctx, rec).FirstErr()
	endSend(rec, err)
	return err
}

// withOutbox writes the order and its event in one transaction. Publishing
// is the relay's job; the trace context goes into the row so the trace
// survives the trip through the table.
func (a *api) withOutbox(ctx context.Context, o Order, ev OrderEvent) error {
	// TODO(11.1): WRITE_MODE=outbox. Put the current trace context into a
	// propagation.MapCarrier (otel.GetTextMapPropagator().Inject), then in ONE
	// transaction (pgx.BeginFunc): insertOrder, `if a.crash()` return an error
	// wrapping errSimulatedCrash, insertOutbox. Optional: wrap it all in a
	// "create t11.order-events" PRODUCER span, as the README explains.
	return errTODO
}

func insertOrder(ctx context.Context, db execer, o Order) error {
	_, err := db.Exec(ctx, `INSERT INTO t11_orders (id, customer, amount) VALUES ($1, $2, $3)`, o.ID, o.Customer, o.Amount)
	return err
}

func insertOutbox(ctx context.Context, tx pgx.Tx, ev OrderEvent, traceContext propagation.MapCarrier) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
        INSERT INTO t11_outbox (event_id, topic, record_key, event_type, payload, trace_context)
        VALUES ($1, $2, $3, $4, $5, $6)`,
		ev.EventID, eventsTopic, ev.OrderID, ev.Type, payload, map[string]string(traceContext))
	return err
}

type relay struct {
	app            *kit.App
	pool           *pgxpool.Pool
	cl             *kgo.Client
	batch          int
	crashEvery     int
	produceTimeout time.Duration

	batches int
}

type outboxRow struct {
	ID           int64
	EventID      string
	Topic        string
	RecordKey    string
	EventType    string
	Payload      []byte
	TraceContext map[string]string
}

func runRelay(ctx context.Context, app *kit.App) error {
	flush, err := setupTracing(ctx, app)
	if err != nil {
		return err
	}
	defer flush()
	pool, err := app.ConnectDB(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := ensureSchema(ctx, pool); err != nil {
		return err
	}
	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := ensureTopic(ctx, app, cl); err != nil {
		return err
	}

	rl := &relay{
		app:            app,
		pool:           pool,
		cl:             cl,
		batch:          max(kit.EnvInt("BATCH", 100), 1),
		crashEvery:     kit.EnvInt("CRASH_BEFORE_MARK", 0),
		produceTimeout: kit.EnvDuration("PRODUCE_TIMEOUT", 10*time.Second),
	}
	pollInterval := kit.EnvDuration("POLL_INTERVAL", 500*time.Millisecond)
	restartDelay := kit.EnvDuration("RESTART_DELAY", 2*time.Second)
	app.Log.Info("relaying", "batch", rl.batch, "poll_interval", pollInterval, "crash_before_mark", rl.crashEvery)

	published := 0
	for {
		n, err := rl.publishBatch(ctx)
		wait := pollInterval
		switch {
		case ctx.Err() != nil:
			app.Log.Info("relay stopped", "published", published, "batches", rl.batches)
			return nil
		case errors.Is(err, errTODO):
			return err
		case errors.Is(err, errSimulatedCrash):
			app.Log.Warn("simulated crash after the produce, before marking the rows: they will be published again", "rows", n)
			wait = restartDelay
		case err != nil:
			app.Log.Error("batch failed, its rows stay in the outbox", "err", err)
		case n > 0:
			published += n
			app.Log.Info("published", "rows", n, "total", published)
			if n == rl.batch {
				wait = 0 // a full batch means there is a backlog
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
	}
}

// publishBatch moves up to rl.batch rows from the outbox to Kafka and marks
// them. The rows stay locked for the whole round trip, so a second relay
// skips them instead of publishing them too.
func (rl *relay) publishBatch(ctx context.Context) (int, error) {
	// TODO(11.2): in one transaction:
	//   1. SELECT up to rl.batch unpublished rows ORDER BY id ... FOR UPDATE SKIP LOCKED
	//      (pgx.CollectRows with pgx.RowToStructByName[outboxRow] reads them)
	//   2. per row: a *kgo.Record (key = record_key, value = payload, event-id and
	//      event-type headers), and startSend(parent, rec) where parent is the
	//      context extracted from row.TraceContext with a propagation.MapCarrier
	//   3. ProduceSync all of them under a PRODUCE_TIMEOUT deadline, endSend each
	//      result; on any error return it (the deferred rollback keeps the rows)
	//   4. if rl.crashBeforeMark(), return len(rows), errSimulatedCrash
	//   5. UPDATE ... SET published_at = now() WHERE id = ANY($1), commit
	return 0, errTODO
}

// crashBeforeMark counts published batches and says whether this one "dies"
// before its rows are marked: every CRASH_BEFORE_MARK-th batch.
func (rl *relay) crashBeforeMark() bool {
	rl.batches++
	return rl.crashEvery > 0 && rl.batches%rl.crashEvery == 0
}

type notifier struct {
	app  *kit.App
	pool *pgxpool.Pool
}

func consume(ctx context.Context, app *kit.App) error {
	flush, err := setupTracing(ctx, app)
	if err != nil {
		return err
	}
	defer flush()
	pool, err := app.ConnectDB(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := ensureSchema(ctx, pool); err != nil {
		return err
	}
	cl, err := app.NewClient(kgo.ConsumerGroup(notifierGroup), kgo.ConsumeTopics(eventsTopic))
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := ensureTopic(ctx, app, cl); err != nil {
		return err
	}

	n := &notifier{app: app, pool: pool}
	app.Log.Info("consuming", "topic", eventsTopic, "group", notifierGroup)
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		for r := range fetches.RecordsAll() {
			if err := n.handle(ctx, r); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("handle p%d@%d: %w", r.Partition, r.Offset, err)
			}
		}
	}
}

func (n *notifier) handle(ctx context.Context, r *kgo.Record) error {
	ctx = otel.GetTextMapPropagator().Extract(ctx, headerCarrier{r})
	ctx, span := tracer.Start(ctx, "process "+r.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation.type", "process"),
			attribute.String("messaging.destination.name", r.Topic),
			attribute.String("messaging.consumer.group.name", notifierGroup),
			attribute.Int("messaging.destination.partition.id", int(r.Partition)),
			attribute.Int64("messaging.kafka.offset", r.Offset),
		))
	defer span.End()

	var ev OrderEvent
	if err := json.Unmarshal(r.Value, &ev); err != nil {
		span.RecordError(err)
		return err
	}
	first, err := n.firstDelivery(ctx, ev.EventID)
	if err != nil {
		span.RecordError(err)
		return err
	}
	if !first {
		span.SetAttributes(attribute.Bool("duplicate", true))
		n.app.Log.InfoContext(ctx, "duplicate event skipped", "event_id", ev.EventID, "order_id", ev.OrderID,
			"partition", r.Partition, "offset", r.Offset)
		return nil
	}
	n.app.Log.InfoContext(ctx, "notify: order created", "order_id", ev.OrderID, "customer", ev.Customer,
		"amount", money(ev.Amount), "partition", r.Partition, "offset", r.Offset,
		"event_age", time.Since(ev.At).Round(time.Millisecond))
	return nil
}

// firstDelivery claims eventID in the consumer's inbox (task 10). The
// "notification" is a log line and can't share the transaction: claiming
// first makes it at-most-once if the process dies right here.
func (n *notifier) firstDelivery(ctx context.Context, eventID string) (bool, error) {
	// TODO(11.3): claim eventID in t11_inbox (INSERT ... ON CONFLICT DO NOTHING)
	// and return whether a row was inserted. Until then every redelivery
	// notifies the customer again.
	_ = eventID
	return true, nil
}
