package kit

import (
	"context"
	"io"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// NewLogger builds the process logger. Every record logged with a context
// that carries an OTel span gets trace_id and span_id attached, which is what
// lets you jump from a log line in one service to the whole trace in Jaeger.
func NewLogger(cfg Config, level slog.Level) *slog.Logger {
	return slog.New(newHandler(os.Stdout, cfg.LogFormat, level)).With(
		"role", cfg.Role,
		"inst", cfg.Instance,
		"ver", cfg.Version,
	)
}

func newHandler(w io.Writer, format string, level slog.Level) slog.Handler {
	opts := &slog.HandlerOptions{Level: level}
	if format == "json" {
		return traceHandler{slog.NewJSONHandler(w, opts)}
	}
	opts.ReplaceAttr = func(groups []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey && len(groups) == 0 {
			return slog.String(slog.TimeKey, a.Value.Time().Format("15:04:05.000"))
		}
		return a
	}
	return traceHandler{slog.NewTextHandler(w, opts)}
}

type traceHandler struct{ slog.Handler }

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceHandler{h.Handler.WithAttrs(attrs)}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{h.Handler.WithGroup(name)}
}
