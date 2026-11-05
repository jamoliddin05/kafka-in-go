// Task 08: tracing. Read README.md first; the TODOs follow its "Your task" steps.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"idiomaticGo/kafka/internal/kit"
)

const (
	ordersTopic   = "t08.orders"
	paymentsTopic = "t08.payments"
	billingGroup  = "t08.billing"
	notifierGroup = "t08.notifier"
)

// The global provider is a delegate, so this works although SetupTracing runs later.
var tracer = otel.Tracer("idiomaticGo/kafka/tasks/08-tracing")

func main() {
	kit.Main(map[string]kit.Role{
		"api":       serveAPI,
		"processor": charge,
		"consumer":  notify,
	})
}

type Order struct {
	ID       string    `json:"id"`
	Customer string    `json:"customer"`
	Amount   int64     `json:"amount_cents"`
	At       time.Time `json:"at"`
}

type Payment struct {
	OrderID   string    `json:"order_id"`
	Customer  string    `json:"customer"`
	Amount    int64     `json:"amount_cents"`
	ChargeID  string    `json:"charge_id"`
	OrderedAt time.Time `json:"ordered_at"`
}

// pipe is one role's Kafka client plus the tracing choices that apply to it.
type pipe struct {
	app   *kit.App
	cl    *kgo.Client
	group string
	link  bool
}

func newPipe(ctx context.Context, app *kit.App, group string, opts ...kgo.Opt) (*pipe, error) {
	p := &pipe{app: app, group: group, link: kit.EnvBool("LINK", false)}
	// TODO(08.5) (optional): with TRACER=kotel, add the kotel plugin's hooks to
	// opts and make send/startProcess below use it instead of tracing.go.
	cl, err := app.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	if err := ensureTopics(ctx, app, cl); err != nil {
		cl.Close()
		return nil, err
	}
	p.cl = cl
	app.Log.Info("tracing", "service", app.ServiceName(), "tracer", kit.Env("TRACER", "manual"), "link", p.link)
	return p, nil
}

func (p *pipe) send(ctx context.Context, rec *kgo.Record) error {
	return produceTraced(ctx, p.cl, rec)
}

func (p *pipe) startProcess(ctx context.Context, rec *kgo.Record) (context.Context, trace.Span) {
	return startProcessSpan(ctx, rec, p.group, p.link)
}

// consume polls until ctx is done and hands every record to handle.
func (p *pipe) consume(ctx context.Context, handle func(context.Context, *kgo.Record)) error {
	for {
		fetches := p.cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, part int32, err error) {
			p.app.Log.Error("fetch error", "topic", t, "partition", part, "err", err)
		})
		fetches.EachRecord(func(rec *kgo.Record) { handle(ctx, rec) })
	}
}

func ensureTopics(ctx context.Context, app *kit.App, cl *kgo.Client) error {
	for _, t := range []string{ordersTopic, paymentsTopic} {
		if err := app.EnsureTopic(ctx, cl, kit.Topic{Name: t, Partitions: 3}); err != nil {
			return err
		}
	}
	return nil
}

// startTracing installs the tracer provider and returns the flush to defer.
func startTracing(ctx context.Context, app *kit.App) (flush func(), err error) {
	shutdown, err := app.SetupTracing(ctx)
	if err != nil {
		return nil, err
	}
	return func() {
		// ctx is already cancelled at this point; the last batch still has to go out.
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdown(sctx); err != nil {
			app.Log.Warn("trace flush failed", "err", err)
		}
	}, nil
}

// ---------------------------------------------------------------- api

func serveAPI(ctx context.Context, app *kit.App) error {
	flush, err := startTracing(ctx, app)
	if err != nil {
		return err
	}
	defer flush()

	p, err := newPipe(ctx, app, "")
	if err != nil {
		return err
	}
	defer p.cl.Close()

	mux := http.NewServeMux()
	mux.Handle("POST /orders", serverSpan("/orders", p.createOrder))
	srv := &http.Server{
		Addr:              kit.Env("HTTP_ADDR", ":8080"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	app.Log.Info("listening", "addr", srv.Addr)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(sctx)
}

type orderRequest struct {
	Customer string `json:"customer"`
	Amount   int64  `json:"amount_cents"`
}

func (p *pipe) createOrder(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req := orderRequest{Customer: "c-1", Amount: 1999}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	order := Order{
		ID:       "o-" + strings.ToLower(rand.Text()[:10]),
		Customer: req.Customer,
		Amount:   req.Amount,
		At:       time.Now().UTC(),
	}
	value, err := json.Marshal(order)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rec := &kgo.Record{Topic: ordersTopic, Key: []byte(order.ID), Value: value}
	if err := p.send(ctx, rec); err != nil {
		p.app.Log.ErrorContext(ctx, "produce failed", "order_id", order.ID, "err", err)
		http.Error(w, "order not accepted, retry", http.StatusServiceUnavailable)
		return
	}
	p.app.Log.InfoContext(ctx, "order accepted", "order_id", order.ID, "partition", rec.Partition, "offset", rec.Offset)

	traceID := trace.SpanContextFromContext(ctx).TraceID().String()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Trace-Id", traceID)
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"order_id": order.ID, "trace_id": traceID})
}

// serverSpan is the minimal hand-made version of otelhttp: continue the
// caller's trace if it sent a traceparent, otherwise start a new one.
func serverSpan(route string, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := tracer.Start(ctx, r.Method+" "+route,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				semconv.HTTPRequestMethodKey.String(r.Method),
				semconv.HTTPRoute(route),
				semconv.URLPath(r.URL.Path),
			),
		)
		defer span.End()

		rw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next(rw, r.WithContext(ctx))

		span.SetAttributes(semconv.HTTPResponseStatusCode(rw.status))
		if rw.status >= 500 {
			span.SetStatus(codes.Error, http.StatusText(rw.status))
		}
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// ---------------------------------------------------------------- processor

func charge(ctx context.Context, app *kit.App) error {
	flush, err := startTracing(ctx, app)
	if err != nil {
		return err
	}
	defer flush()

	p, err := newPipe(ctx, app, billingGroup, kgo.ConsumeTopics(ordersTopic), kgo.ConsumerGroup(billingGroup))
	if err != nil {
		return err
	}
	defer p.cl.Close()
	return p.consume(ctx, p.chargeOrder)
}

func (p *pipe) chargeOrder(ctx context.Context, rec *kgo.Record) {
	ctx, span := p.startProcess(ctx, rec)
	defer span.End()

	var o Order
	if err := json.Unmarshal(rec.Value, &o); err != nil {
		p.fail(ctx, span, "bad order payload", err)
		return
	}
	chargeID, err := callGateway(ctx, o)
	if err != nil {
		p.fail(ctx, span, "charge failed", err)
		return
	}
	value, err := json.Marshal(Payment{
		OrderID:   o.ID,
		Customer:  o.Customer,
		Amount:    o.Amount,
		ChargeID:  chargeID,
		OrderedAt: o.At,
	})
	if err != nil {
		p.fail(ctx, span, "encode payment", err)
		return
	}
	out := &kgo.Record{Topic: paymentsTopic, Key: rec.Key, Value: value}
	if err := p.send(ctx, out); err != nil {
		p.fail(ctx, span, "produce payment failed", err)
		return
	}
	p.app.Log.InfoContext(ctx, "order charged", "order_id", o.ID, "charge_id", chargeID,
		"from", rec.Topic, "partition", rec.Partition, "offset", rec.Offset)
}

// callGateway stands in for the HTTP call to a payment provider.
func callGateway(ctx context.Context, o Order) (string, error) {
	ctx, span := tracer.Start(ctx, "payment-gateway charge", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	select {
	case <-time.After(time.Duration(20+mrand.IntN(60)) * time.Millisecond):
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return "ch-" + strings.ToLower(o.ID[2:]), nil
}

func (p *pipe) fail(ctx context.Context, span trace.Span, msg string, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, msg)
	p.app.Log.ErrorContext(ctx, msg, "err", err)
}

// ---------------------------------------------------------------- consumer

func notify(ctx context.Context, app *kit.App) error {
	flush, err := startTracing(ctx, app)
	if err != nil {
		return err
	}
	defer flush()

	p, err := newPipe(ctx, app, notifierGroup, kgo.ConsumeTopics(paymentsTopic), kgo.ConsumerGroup(notifierGroup))
	if err != nil {
		return err
	}
	defer p.cl.Close()
	return p.consume(ctx, p.notifyCustomer)
}

func (p *pipe) notifyCustomer(ctx context.Context, rec *kgo.Record) {
	ctx, span := p.startProcess(ctx, rec)
	defer span.End()

	var pay Payment
	if err := json.Unmarshal(rec.Value, &pay); err != nil {
		p.fail(ctx, span, "bad payment payload", err)
		return
	}
	// Baggage travels in the same headers as traceparent; the processor never touched it.
	tenant := baggage.FromContext(ctx).Member("tenant").Value()
	p.app.Log.InfoContext(ctx, "customer notified", "order_id", pay.OrderID, "customer", pay.Customer,
		"tenant", tenant, "e2e", time.Since(pay.OrderedAt).Round(time.Millisecond))
}
