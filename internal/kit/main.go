package kit

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"
)

// App is what every role receives: parsed config plus a ready logger.
type App struct {
	Config
	Log *slog.Logger
	// kafkaLog is a separate logger so franz-go internals can be quieter
	// (or louder) than the application itself.
	kafkaLog *slog.Logger
}

// Role is one process of a task: "producer", "consumer", "api", ...
// It must return when ctx is cancelled (SIGINT/SIGTERM).
type Role func(ctx context.Context, app *App) error

// Main is the entrypoint of every task binary. It picks the role from -role
// or $ROLE, cancels ctx on the first SIGINT/SIGTERM and lets a second signal
// kill the process immediately — the same contract Kubernetes gives you:
// SIGTERM, then SIGKILL after terminationGracePeriodSeconds.
func Main(roles map[string]Role) {
	names := slices.Sorted(maps.Keys(roles))
	roleFlag := flag.String("role", Env("ROLE", ""), "process to run: "+strings.Join(names, "|"))
	flag.Parse()

	cfg := LoadConfig()
	cfg.Role = *roleFlag
	if cfg.Role == "" && len(roles) == 1 {
		cfg.Role = names[0]
	}

	app := &App{Config: cfg}
	app.Log = NewLogger(cfg, cfg.LogLevel)
	app.kafkaLog = NewLogger(cfg, cfg.KafkaLogLevel).With("component", "franz-go")
	slog.SetDefault(app.Log)

	run, ok := roles[cfg.Role]
	if !ok {
		app.Log.Error("unknown role, set ROLE or -role", "got", cfg.Role, "available", names)
		os.Exit(2)
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		app.Log.Info("signal received, shutting down (send again to force)", "signal", sig.String())
		cancel(fmt.Errorf("received %s", sig))
		signal.Stop(sigs)
	}()

	started := time.Now()
	app.Log.Info("starting", "task", cfg.Task, "brokers", strings.Join(cfg.Brokers, ","))
	err := run(ctx, app)
	if err != nil && !errors.Is(err, context.Canceled) {
		app.Log.Error("exited with error", "err", err, "uptime", time.Since(started).Round(time.Millisecond))
		os.Exit(1)
	}
	app.Log.Info("stopped", "uptime", time.Since(started).Round(time.Millisecond))
}
