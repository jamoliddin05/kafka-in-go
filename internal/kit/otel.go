package kit

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// ServiceName is what Jaeger shows in its service dropdown, e.g. "t08-api".
func (a *App) ServiceName() string {
	if name := Env("SERVICE_NAME", ""); name != "" {
		return name
	}
	prefix := a.Task
	if len(prefix) >= 2 {
		prefix = "t" + prefix[:2]
	}
	return prefix + "-" + a.Role
}

// SetupTracing installs the global tracer provider (OTLP/gRPC to Jaeger,
// endpoint from OTEL_EXPORTER_OTLP_ENDPOINT, default plaintext localhost:4317) and the
// W3C propagator. It only sets up the pipe: putting the trace context into
// Kafka headers and taking it back out is the job of task 08.
// Call the returned func before exit, or the last batch of spans is lost.
func (a *App) SetupTracing(ctx context.Context) (shutdown func(context.Context) error, err error) {
	var opts []otlptracegrpc.Option
	if Env("OTEL_EXPORTER_OTLP_ENDPOINT", "") == "" {
		// The exporter's own default is TLS to localhost:4317; Jaeger here is plaintext.
		opts = append(opts, otlptracegrpc.WithEndpoint("localhost:4317"), otlptracegrpc.WithInsecure())
	}
	exp, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}
	res := resource.NewSchemaless(
		attribute.String("service.name", a.ServiceName()),
		attribute.String("service.version", a.Version),
		attribute.String("service.instance.id", a.Instance),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return tp.Shutdown, nil
}
