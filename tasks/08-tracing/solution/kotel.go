package main

import "github.com/twmb/franz-go/plugin/kotel"

// newKotelTracer builds franz-go's hook-based tracer. Without TracerProvider or
// TracerPropagator options it uses the globals that app.SetupTracing installed.
func newKotelTracer(group string, link bool) *kotel.Tracer {
	var opts []kotel.TracerOpt
	if group != "" {
		opts = append(opts, kotel.ConsumerGroup(group))
	}
	if link {
		opts = append(opts, kotel.LinkSpans())
	}
	return kotel.NewTracer(opts...)
}
