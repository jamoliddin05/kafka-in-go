// Task 04: producer reliability. Read README.md first; the TODOs follow its "Your task" steps.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	mrand "math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"idiomaticGo/kafka/internal/kit"
)

const (
	eventsTopic = "t04.events"
	benchTopic  = "t04.bench"
)

var errTODO = errors.New("not implemented yet, see README.md")

func main() {
	kit.Main(map[string]kit.Role{
		"producer": produce,
		"bench":    bench,
		"count":    count,
	})
}

// Event is what the count tool needs to prove loss: which run sent it and
// a gapless per-run sequence number.
type Event struct {
	Run  string    `json:"run"`
	Seq  int64     `json:"seq"`
	At   time.Time `json:"at"`
	Note string    `json:"note"`
}

var words = strings.Fields("order paid shipped refund customer invoice cart checkout " +
	"warehouse courier address payment card wallet discount coupon tax total item sku")

// note builds compressible text, roughly like real JSON payloads: random
// bytes would not compress at all, a repeated byte would compress absurdly well.
func note(size int) string {
	var b strings.Builder
	b.Grow(size + 16)
	for b.Len() < size {
		b.WriteString(words[mrand.IntN(len(words))])
		b.WriteByte(' ')
	}
	return b.String()[:size]
}

func newRecord(topic, run string, seq int64, valueSize int) *kgo.Record {
	value, _ := json.Marshal(Event{Run: run, Seq: seq, At: time.Now(), Note: note(valueSize)})
	return &kgo.Record{Topic: topic, Key: fmt.Appendf(nil, "user-%d", seq%1000), Value: value}
}

// ---------------------------------------------------------------- client options

// producerOpts maps the task.env knobs to options. An unset knob keeps the
// franz-go default, so you can see what the defaults do.
func producerOpts() ([]kgo.Opt, error) {
	var opts []kgo.Opt
	if d := kit.EnvDuration("LINGER", -1); d >= 0 {
		opts = append(opts, kgo.ProducerLinger(d))
	}
	if n := kit.EnvInt("BATCH_MAX_BYTES", 0); n > 0 {
		opts = append(opts, kgo.ProducerBatchMaxBytes(int32(n)))
	}
	if n := kit.EnvInt("MAX_BUFFERED", 0); n > 0 {
		opts = append(opts, kgo.MaxBufferedRecords(n))
	}
	if d := kit.EnvDuration("DELIVERY_TIMEOUT", 0); d > 0 {
		opts = append(opts, kgo.RecordDeliveryTimeout(d))
	}
	switch c := kit.Env("COMPRESSION", ""); c {
	case "":
	case "none":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
	case "gzip":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
	case "snappy":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
	case "lz4":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
	case "zstd":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
	default:
		return nil, fmt.Errorf("bad COMPRESSION %q, want none|gzip|snappy|lz4|zstd", c)
	}
	return opts, nil
}

func classify(err error) string {
	if ke, ok := errors.AsType[*kerr.Error](err); ok {
		return ke.Message
	}
	switch {
	case errors.Is(err, kgo.ErrMaxBuffered):
		return "MAX_BUFFERED"
	case errors.Is(err, kgo.ErrRecordTimeout):
		return "RECORD_TIMEOUT"
	case errors.Is(err, kgo.ErrClientClosed):
		return "CLIENT_CLOSED"
	case errors.Is(err, context.Canceled):
		return "CANCELED"
	}
	return err.Error()
}

// tally counts promise outcomes. Promises run serially on franz-go's own
// goroutine, so they must stay cheap: no I/O, no blocking.
type tally struct {
	sent, acked atomic.Int64
	mu          sync.Mutex
	failed      map[string]int64
	firstErr    error
}

func newTally() *tally { return &tally{failed: map[string]int64{}} }

func (t *tally) promise(_ *kgo.Record, err error) {
	// TODO(04.1): count the outcome: t.acked on success, otherwise
	// t.failed[classify(err)] under t.mu, and keep the first error in t.firstErr.
	_ = err
}

func (t *tally) failures() (int64, string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var n int64
	var parts []string
	for _, k := range slices.Sorted(maps.Keys(t.failed)) {
		n += t.failed[k]
		parts = append(parts, fmt.Sprintf("%s=%d", k, t.failed[k]))
	}
	return n, strings.Join(parts, " ")
}

// settle waits briefly until every sent record has had its promise called:
// promises are delivered asynchronously, also the ones Close fails.
func (t *tally) settle(limit time.Duration) {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		failed, _ := t.failures()
		if t.acked.Load()+failed >= t.sent.Load() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ---------------------------------------------------------------- producer

func produce(ctx context.Context, app *kit.App) error {
	topic := kit.Env("TOPIC", eventsTopic)
	rate := kit.EnvInt("RATE", 2000)
	valueSize := kit.EnvInt("VALUE_SIZE", 200)
	flushOnExit := kit.EnvBool("FLUSH_ON_EXIT", true)
	shutdownTimeout := kit.EnvDuration("SHUTDOWN_TIMEOUT", 10*time.Second)
	backpressure := kit.Env("BACKPRESSURE", "block")
	if backpressure != "block" && backpressure != "shed" {
		return fmt.Errorf("bad BACKPRESSURE %q, want block|shed", backpressure)
	}

	opts, err := producerOpts()
	if err != nil {
		return err
	}
	cl, err := app.NewClient(opts...)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			cl.Close()
		}
	}()
	if err := app.EnsureTopic(ctx, cl, kit.Topic{Name: topic, Partitions: 3}); err != nil {
		return err
	}

	// TODO(04.2): records are produced with the signal ctx, and nothing is
	// flushed on exit (see the end of this function). README step 2.
	prodCtx := ctx
	_, _ = flushOnExit, shutdownTimeout

	run := rand.Text()[:6]
	t := newTally()
	app.Log.Info("producing", "topic", topic, "run", run, "rate", rate, "backpressure", backpressure, "flush_on_exit", flushOnExit)
	if backpressure == "shed" {
		// TODO(04.4): with BACKPRESSURE=shed use cl.TryProduce in the loop below.
		return errTODO
	}

	var wg sync.WaitGroup
	statsCtx, stopStats := context.WithCancel(ctx)
	wg.Go(func() {
		tick := time.NewTicker(kit.EnvDuration("STATS_EVERY", time.Second))
		defer tick.Stop()
		for {
			select {
			case <-statsCtx.Done():
				return
			case <-tick.C:
			}
			_, failed := t.failures()
			app.Log.Info("stats", "run", run, "sent", t.sent.Load(), "acked", t.acked.Load(),
				"failed", failed, "buffered", cl.BufferedProduceRecords())
		}
	})

	var seq int64
	pace(ctx, rate, func() {
		seq++
		rec := newRecord(topic, run, seq, valueSize)
		t.sent.Add(1)
		cl.Produce(prodCtx, rec, t.promise)
	})
	stopStats()
	wg.Wait()

	// TODO(04.2): with FLUSH_ON_EXIT, Flush with a deadline before Close.
	// Close fails whatever is still buffered or in flight with ErrClientClosed.
	cl.Close()
	closed = true
	t.settle(2 * time.Second)

	failedN, failed := t.failures()
	app.Log.Info("summary", "run", run, "sent", t.sent.Load(), "acked", t.acked.Load(),
		"failed", failedN, "by_error", failed)
	return nil
}

// pace calls fn rate times per second (rate <= 0: as fast as possible) until
// ctx is done. A blocked Produce slows it down, which is the backpressure.
func pace(ctx context.Context, rate int, fn func()) {
	if rate <= 0 {
		for ctx.Err() == nil {
			fn()
		}
		return
	}
	const tickEvery = 10 * time.Millisecond
	perTick := float64(rate) * tickEvery.Seconds()
	tick := time.NewTicker(tickEvery)
	defer tick.Stop()
	var owed float64
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		for owed += perTick; owed >= 1 && ctx.Err() == nil; owed-- {
			fn()
		}
	}
}

// ---------------------------------------------------------------- bench

// batchStats sees every batch the brokers acknowledged. franz-go calls the
// hook from its own goroutine, after the promises, so totals use atomics.
type batchStats struct {
	batches, records, uncompressed, compressed atomic.Int64
	codecs                                     [5]atomic.Int64 // by CompressionType
}

func (s *batchStats) OnProduceBatchWritten(_ kgo.BrokerMetadata, _ string, _ int32, m kgo.ProduceBatchMetrics) {
	// TODO(04.3): add m.NumRecords, m.UncompressedBytes and m.CompressedBytes
	// to the totals, count the batch, and count m.CompressionType in s.codecs.
	_ = m
}

var codecNames = [...]string{"none", "gzip", "snappy", "lz4", "zstd"}

func (s *batchStats) codec() string {
	var used []string
	for i := range s.codecs {
		if s.codecs[i].Load() > 0 {
			used = append(used, codecNames[i])
		}
	}
	return strings.Join(used, ",")
}

func bench(ctx context.Context, app *kit.App) error {
	n := kit.EnvInt("BENCH_N", 200_000)
	rate := kit.EnvInt("BENCH_RATE", 0)
	valueSize := kit.EnvInt("VALUE_SIZE", 200)

	opts, err := producerOpts()
	if err != nil {
		return err
	}
	stats := new(batchStats)
	cl, err := app.NewClient(append(opts, kgo.WithHooks(stats))...)
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := app.EnsureTopic(ctx, cl, kit.Topic{Name: benchTopic, Partitions: 3}); err != nil {
		return err
	}

	// Build records first so the timing is the client's, not json.Marshal's.
	run := rand.Text()[:6]
	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i] = newRecord(benchTopic, run, int64(i+1), valueSize)
	}

	var (
		mu        sync.Mutex
		latencies = make([]time.Duration, 0, n)
	)
	t := newTally()
	start := time.Now()
	sendCtx, done := context.WithCancel(ctx)
	next := 0
	pace(sendCtx, rate, func() {
		if next == len(recs) {
			done()
			return
		}
		r := recs[next]
		next++
		sentAt := time.Now()
		t.sent.Add(1)
		cl.Produce(ctx, r, func(r *kgo.Record, err error) {
			if err == nil {
				mu.Lock()
				latencies = append(latencies, time.Since(sentAt))
				mu.Unlock()
			}
			t.promise(r, err)
		})
	})
	done()
	if err := cl.Flush(ctx); err != nil {
		return err
	}
	elapsed := time.Since(start)

	// The hook runs asynchronously; give it a moment to see the last batches.
	for deadline := time.Now().Add(2 * time.Second); stats.records.Load() < t.acked.Load() && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}

	failedN, failed := t.failures()
	if failedN > 0 {
		app.Log.Warn("some records failed", "failed", failed, "first_err", t.firstErr)
	}
	if t.acked.Load() == 0 {
		return errTODO // TODO(04.1) not done yet, or every record failed
	}
	if stats.batches.Load() == 0 {
		return errTODO // TODO(04.3): the hook is not implemented
	}
	mu.Lock()
	slices.Sort(latencies)
	p50, p99 := pick(latencies, 0.5), pick(latencies, 0.99)
	mu.Unlock()

	acked := t.acked.Load()
	unc, comp, batches := stats.uncompressed.Load(), stats.compressed.Load(), stats.batches.Load()
	fmt.Printf("records=%d failed=%d in %s: %.0f rec/s, %.1f MB/s before compression\n",
		acked, failedN, elapsed.Round(time.Millisecond), float64(acked)/elapsed.Seconds(), float64(unc)/elapsed.Seconds()/1e6)
	fmt.Printf("batches=%d  avg %.1f records/batch  avg %.1f KB/batch on the wire  codec=%s  ratio=%.2fx\n",
		batches, float64(stats.records.Load())/float64(batches), float64(comp)/float64(batches)/1e3,
		stats.codec(), float64(unc)/float64(max(comp, 1)))
	fmt.Printf("ack latency p50=%s p99=%s\n", p50, p99)
	return nil
}

func pick(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[min(len(sorted)-1, int(q*float64(len(sorted))))].Round(100 * time.Microsecond)
}

// ---------------------------------------------------------------- count

// count reads the topic up to its current end and reports, per producer run,
// how many records actually made it. Compare with the producer's summary.
func count(ctx context.Context, app *kit.App) error {
	topic := kit.Env("TOPIC", eventsTopic)
	last := kit.EnvInt("LAST_RUNS", 10)

	adm, err := app.NewClient()
	if err != nil {
		return err
	}
	defer adm.Close()
	ends, err := kadm.NewClient(adm).ListEndOffsets(ctx, topic)
	if err != nil {
		return err
	}
	if err := ends.Error(); err != nil {
		return err
	}
	remaining := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			remaining[o.Partition] = o.Offset
		}
	})

	cl, err := app.NewClient(kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		return err
	}
	defer cl.Close()

	type runStats struct {
		records, maxSeq int64
		seqs            map[int64]struct{}
		first           time.Time
	}
	runs := map[string]*runStats{}
	for len(remaining) > 0 {
		pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		fetches := cl.PollFetches(pollCtx)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(fetches.Err0(), context.DeadlineExceeded) {
			return fmt.Errorf("stopped waiting for partitions %v", slices.Sorted(maps.Keys(remaining)))
		}
		fetches.EachError(func(t string, p int32, err error) {
			app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			end, ok := remaining[r.Partition]
			if !ok || r.Offset >= end {
				return
			}
			if r.Offset >= end-1 {
				delete(remaining, r.Partition)
			}
			var ev Event
			if json.Unmarshal(r.Value, &ev) != nil {
				return
			}
			rs := runs[ev.Run]
			if rs == nil {
				rs = &runStats{seqs: map[int64]struct{}{}, first: ev.At}
				runs[ev.Run] = rs
			}
			rs.records++
			rs.maxSeq = max(rs.maxSeq, ev.Seq)
			rs.seqs[ev.Seq] = struct{}{}
		})
	}

	ids := slices.SortedFunc(maps.Keys(runs), func(a, b string) int { return runs[a].first.Compare(runs[b].first) })
	ids = ids[max(0, len(ids)-last):]
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "RUN\tSTARTED\tRECORDS\tDISTINCT SEQS\tMAX SEQ\tMISSING BELOW MAX\tDUPLICATES")
	for _, id := range ids {
		rs := runs[id]
		distinct := int64(len(rs.seqs))
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t%d\t%d\n", id, rs.first.Format("15:04:05"),
			rs.records, distinct, rs.maxSeq, rs.maxSeq-distinct, rs.records-distinct)
	}
	return w.Flush()
}
