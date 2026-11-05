package main

import (
	"context"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// headerCarrier lets the W3C propagators read and write Kafka record headers.
// Use it as (*headerCarrier)(&rec.Headers).
type headerCarrier []kgo.RecordHeader

var _ propagation.TextMapCarrier = (*headerCarrier)(nil)

func (c *headerCarrier) Get(key string) string {
	for _, h := range *c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set replaces an existing header: a forwarded record that already carries a
// traceparent must not end up with two, because Get returns the first one.
func (c *headerCarrier) Set(key, value string) {
	for i, h := range *c {
		if h.Key == key {
			(*c)[i].Value = []byte(value)
			return
		}
	}
	*c = append(*c, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c *headerCarrier) Keys() []string {
	keys := make([]string, len(*c))
	for i, h := range *c {
		keys[i] = h.Key
	}
	return keys
}

// produceTraced sends rec inside a PRODUCER span and puts that span's context
// into the headers, so whoever consumes the record can continue the trace.
func produceTraced(ctx context.Context, cl *kgo.Client, rec *kgo.Record) error {
	ctx, span := tracer.Start(ctx, "send "+rec.Topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingOperationTypeSend,
			semconv.MessagingOperationName("send"),
			semconv.MessagingDestinationName(rec.Topic),
			semconv.MessagingKafkaMessageKey(string(rec.Key)),
		),
	)
	defer span.End()

	otel.GetTextMapPropagator().Inject(ctx, (*headerCarrier)(&rec.Headers))

	if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "produce failed")
		return err
	}
	// Partition and offset exist only after the broker acked.
	span.SetAttributes(
		semconv.MessagingDestinationPartitionID(strconv.Itoa(int(rec.Partition))),
		semconv.MessagingKafkaOffset(int(rec.Offset)),
	)
	return nil
}

// startProcessSpan extracts the producer's context from the headers and starts
// the CONSUMER span for one record: a child of the producer span, or with
// link=true the root of a new trace that links back to it.
func startProcessSpan(ctx context.Context, rec *kgo.Record, group string, link bool) (context.Context, trace.Span) {
	ctx = otel.GetTextMapPropagator().Extract(ctx, (*headerCarrier)(&rec.Headers))

	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingOperationTypeProcess,
			semconv.MessagingOperationName("process"),
			semconv.MessagingDestinationName(rec.Topic),
			semconv.MessagingDestinationPartitionID(strconv.Itoa(int(rec.Partition))),
			semconv.MessagingKafkaOffset(int(rec.Offset)),
			semconv.MessagingKafkaMessageKey(string(rec.Key)),
			semconv.MessagingConsumerGroupName(group),
		),
	}
	if link {
		// WithNewRoot drops the remote parent but keeps the baggage in ctx.
		opts = append(opts, trace.WithNewRoot(), trace.WithLinks(trace.LinkFromContext(ctx)))
	}
	return tracer.Start(ctx, "process "+rec.Topic, opts...)
}
