package main

import (
	"context"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"idiomaticGo/kafka/internal/kit"
)

// Task 08 built this by hand; here kotel's RecordCarrier is the carrier.
var tracer = otel.Tracer("idiomaticGo/kafka/tasks/09-retries-and-dlq")

// sendTraced produces rec inside a PRODUCER span whose context goes into the headers.
func sendTraced(ctx context.Context, cl *kgo.Client, rec *kgo.Record) error {
	ctx, span := tracer.Start(ctx, "send "+rec.Topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingOperationTypeSend,
			semconv.MessagingDestinationName(rec.Topic),
			semconv.MessagingKafkaMessageKey(string(rec.Key)),
		),
	)
	defer span.End()
	otel.GetTextMapPropagator().Inject(ctx, kotel.NewRecordCarrier(rec))
	err := cl.ProduceSync(ctx, rec).FirstErr()
	if err != nil {
		failSpan(span, err)
	}
	return err
}

// startProcess continues the trace found in rec's headers. Rerouted records
// keep the original traceparent, so every attempt is a child of the original send.
func startProcess(ctx context.Context, rec *kgo.Record, group string, attempt int) (context.Context, trace.Span) {
	ctx = otel.GetTextMapPropagator().Extract(ctx, kotel.NewRecordCarrier(rec))
	return tracer.Start(ctx, "process "+rec.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingOperationTypeProcess,
			semconv.MessagingDestinationName(rec.Topic),
			semconv.MessagingDestinationPartitionID(strconv.Itoa(int(rec.Partition))),
			semconv.MessagingKafkaOffset(int(rec.Offset)),
			semconv.MessagingKafkaMessageKey(string(rec.Key)),
			semconv.MessagingConsumerGroupName(group),
			attribute.Int("retry.attempt", attempt),
		),
	)
}

func failSpan(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, classify(err).String())
}

// startTracing installs the tracer provider and returns the flush to defer.
func startTracing(ctx context.Context, app *kit.App) (flush func(), err error) {
	shutdown, err := app.SetupTracing(ctx)
	if err != nil {
		return nil, err
	}
	return func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdown(sctx); err != nil {
			app.Log.Warn("trace flush failed", "err", err)
		}
	}, nil
}
