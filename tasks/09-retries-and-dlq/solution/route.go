package main

import (
	"context"
	"math/rand/v2"
	"slices"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Bookkeeping headers. Everything else on the record, including traceparent,
// travels unchanged from the main topic to the retry topic, the DLQ and back.
const (
	hdrAttempt           = "retry-attempt"    // which trip through the retry topic this is (1..MAX_ATTEMPTS)
	hdrRetryAt           = "retry-at"         // RFC 3339; the retry worker doesn't process the record before this
	hdrFirstFailure      = "first-failure-at" // set once, on the first failure
	hdrError             = "error"            // the last error
	hdrErrorClass        = "error-class"      // transient | permanent
	hdrOriginalTopic     = "original-topic"   // where the record first failed, set once
	hdrOriginalPartition = "original-partition"
	hdrOriginalOffset    = "original-offset"
	hdrRedriveCount      = "redrive-count" // set by the redrive tool
)

// toRetry builds the copy of rec that goes to the retry topic for the given attempt.
func toRetry(rec *kgo.Record, cause error, attempt int, delay time.Duration) *kgo.Record {
	out := reroute(rec, retryTopic, cause, attempt)
	setHeader(out, hdrRetryAt, time.Now().Add(delay).UTC().Format(time.RFC3339Nano))
	return out
}

// toDLQ builds the copy of rec that goes to the DLQ.
func toDLQ(rec *kgo.Record, cause error, attempt int) *kgo.Record {
	out := reroute(rec, dlqTopic, cause, attempt)
	delHeader(out, hdrRetryAt)
	return out
}

func reroute(rec *kgo.Record, topic string, cause error, attempt int) *kgo.Record {
	out := &kgo.Record{
		Topic:   topic,
		Key:     rec.Key, // same key: per-key ordering inside the retry topic, same partitioning everywhere
		Value:   rec.Value,
		Headers: slices.Clone(rec.Headers),
	}
	setHeaderOnce(out, hdrOriginalTopic, rec.Topic)
	setHeaderOnce(out, hdrOriginalPartition, strconv.Itoa(int(rec.Partition)))
	setHeaderOnce(out, hdrOriginalOffset, strconv.FormatInt(rec.Offset, 10))
	setHeaderOnce(out, hdrFirstFailure, time.Now().UTC().Format(time.RFC3339Nano))
	setHeader(out, hdrError, cause.Error())
	setHeader(out, hdrErrorClass, classify(cause).String())
	setHeader(out, hdrAttempt, strconv.Itoa(attempt))
	return out
}

// park writes a rerouted record and waits for the ack. Only after that may the
// source record be committed.
func (w *worker) park(ctx context.Context, out *kgo.Record) error {
	if err := w.cl.ProduceSync(ctx, out).FirstErr(); err != nil {
		return err
	}
	args := []any{"to", out.Topic, "key", string(out.Key), "attempt", header(out, hdrAttempt),
		"class", header(out, hdrErrorClass), "error", header(out, hdrError)}
	if at := header(out, hdrRetryAt); at != "" {
		args = append(args, "retry_at", at)
	}
	w.app.Log.WarnContext(ctx, "rerouted", args...)
	return nil
}

// backoff is the delay before retry attempt n: exponential, capped, with up
// to 20% jitter so records that failed together don't come back together.
func (c config) backoff(attempt int) time.Duration {
	d := min(c.retryBackoff<<(attempt-1), c.maxBackoff)
	return d + rand.N(d/5+1)
}

func header(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func headerInt(rec *kgo.Record, key string) int {
	n, _ := strconv.Atoi(header(rec, key))
	return n
}

func setHeader(rec *kgo.Record, key, value string) {
	for i, h := range rec.Headers {
		if h.Key == key {
			rec.Headers[i].Value = []byte(value)
			return
		}
	}
	rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func setHeaderOnce(rec *kgo.Record, key, value string) {
	if header(rec, key) == "" {
		setHeader(rec, key, value)
	}
}

func delHeader(rec *kgo.Record, keys ...string) {
	rec.Headers = slices.DeleteFunc(rec.Headers, func(h kgo.RecordHeader) bool {
		return slices.Contains(keys, h.Key)
	})
}
