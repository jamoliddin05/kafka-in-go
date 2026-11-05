package main

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// headerCarrier lets the W3C propagators read and write Kafka record headers.
// Use it as (*headerCarrier)(&rec.Headers).
type headerCarrier []kgo.RecordHeader

var _ propagation.TextMapCarrier = (*headerCarrier)(nil)

// TODO(08.1): return the value of the first header named key, or "".
func (c *headerCarrier) Get(key string) string { return "" }

// TODO(08.1): set the header, replacing an existing one with the same key.
func (c *headerCarrier) Set(key, value string) {}

// TODO(08.1): return the keys of all headers.
func (c *headerCarrier) Keys() []string { return nil }

// produceTraced sends rec and waits for the broker's ack.
func produceTraced(ctx context.Context, cl *kgo.Client, rec *kgo.Record) error {
	// TODO(08.2): start a PRODUCER span "send <topic>" with the messaging
	// attributes from the README, inject its context into rec.Headers with
	// otel.GetTextMapPropagator(), produce, then put the partition and offset
	// (or the error) on the span.
	return cl.ProduceSync(ctx, rec).FirstErr()
}

// startProcessSpan starts the CONSUMER span for one record.
func startProcessSpan(ctx context.Context, rec *kgo.Record, group string, link bool) (context.Context, trace.Span) {
	// TODO(08.3): extract the producer's context from rec.Headers and start a
	// CONSUMER span "process <topic>" as its child, with messaging attributes.
	// TODO(08.4): with link=true, start a new root span that links to it instead.
	return ctx, trace.SpanFromContext(ctx)
}
