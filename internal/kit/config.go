// Package kit is the boring plumbing shared by every task: config, logging,
// signal handling, client construction. The Kafka logic you're here to learn
// lives in the tasks themselves.
package kit

import (
	"log/slog"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

// Config is read from the environment so the same binary runs from your IDE
// (defaults point at the brokers' host ports) and inside docker compose.
type Config struct {
	Task        string
	Role        string
	Instance    string
	Version     string
	Brokers     []string
	DatabaseURL string
	LogFormat   string
	LogLevel    slog.Level
	// franz-go's own logs. "info" shows joins, assignments and revokes,
	// which is exactly what you want to watch in the consumer-group tasks.
	KafkaLogLevel slog.Level
}

func LoadConfig() Config {
	host, _ := os.Hostname()
	return Config{
		Task:          Env("TASK", taskFromBuild()),
		Role:          Env("ROLE", ""),
		Instance:      Env("INSTANCE", host),
		Version:       Env("VERSION", "dev"),
		Brokers:       strings.Split(Env("KAFKA_BROKERS", "localhost:9092,localhost:9093,localhost:9094"), ","),
		DatabaseURL:   Env("DATABASE_URL", "postgres://course:course@localhost:5433/course?sslmode=disable"),
		LogFormat:     Env("LOG_FORMAT", "text"),
		LogLevel:      parseLevel(Env("LOG_LEVEL", "info")),
		KafkaLogLevel: parseLevel(Env("KAFKA_LOG_LEVEL", "warn")),
	}
}

// taskFromBuild names the task after the main package ("08-tracing",
// "08-tracing/solution"), so `go run` from the host needs no TASK env.
func taskFromBuild() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "local"
	}
	_, task, found := strings.Cut(bi.Path, "/tasks/")
	if !found {
		return "local"
	}
	return task
}

func Env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func EnvInt(key string, def int) int {
	v, err := strconv.Atoi(Env(key, ""))
	if err != nil {
		return def
	}
	return v
}

func EnvBool(key string, def bool) bool {
	v, err := strconv.ParseBool(Env(key, ""))
	if err != nil {
		return def
	}
	return v
}

func EnvDuration(key string, def time.Duration) time.Duration {
	v, err := time.ParseDuration(Env(key, ""))
	if err != nil {
		return def
	}
	return v
}

func parseLevel(s string) slog.Level {
	var l slog.Level
	if s == "none" || s == "off" {
		return slog.Level(100)
	}
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo
	}
	return l
}
