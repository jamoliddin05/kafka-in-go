// Task 13: concurrency and lag. Read README.md first; the TODOs follow its "Your task" steps.
package main

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"hash/maphash"
	"maps"
	"math/rand/v2"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"idiomaticGo/kafka/internal/kit"
)

const (
	eventsTopic  = "t13.events"
	defaultGroup = "t13.consumer"
)

var errTODO = errors.New("not implemented yet, see README.md")

func main() {
	kit.Main(map[string]kit.Role{
		"producer": produce,
		"consumer": consume,
		"lag":      lag,
	})
}

// Event carries a per-user sequence number so the consumer can check per-key
// order. Run changes with every producer start, because Seq restarts at 1.
type Event struct {
	User string `json:"user"`
	Run  string `json:"run"`
	Seq  int64  `json:"seq"`
}

func (e Event) key() string { return e.Run + "/" + e.User }

func ensureTopic(ctx context.Context, app *kit.App, cl *kgo.Client) error {
	return app.EnsureTopic(ctx, cl, kit.Topic{Name: eventsTopic, Partitions: 12})
}

func produce(ctx context.Context, app *kit.App) error {
	rate := kit.EnvInt("RATE", 1000)
	users := max(kit.EnvInt("USERS", 1000), 1)
	hotPct := kit.EnvInt("HOT_PCT", 0)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := ensureTopic(ctx, app, cl); err != nil {
		return err
	}

	run := crand.Text()[:6]
	seqs := make([]int64, users)
	var produced, failed atomic.Int64
	app.Log.Info("producing", "rate", rate, "users", users, "hot_pct", hotPct, "run", run)

	const tick = 10 * time.Millisecond
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	report := time.NewTicker(5 * time.Second)
	defer report.Stop()
	perTick, owed := float64(rate)*tick.Seconds(), 0.0
	last, lastAt := int64(0), time.Now()
	for {
		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			err := cl.Flush(flushCtx)
			app.Log.Info("producer stopped", "produced", produced.Load(), "failed", failed.Load(), "flush_err", err)
			return nil
		case <-report.C:
			n := produced.Load()
			app.Log.Info("produced", "total", n, "rate", fmt.Sprintf("%.0f/s", float64(n-last)/time.Since(lastAt).Seconds()), "failed", failed.Load())
			last, lastAt = n, time.Now()
			continue
		case <-ticker.C:
		}

		owed += perTick
		for ; owed >= 1; owed-- {
			u := rand.IntN(users)
			if hotPct > 0 && rand.IntN(100) < hotPct {
				u = 0
			}
			seqs[u]++
			ev := Event{User: fmt.Sprintf("user-%d", u), Run: run, Seq: seqs[u]}
			value, err := json.Marshal(ev)
			if err != nil {
				return err
			}
			cl.Produce(ctx, &kgo.Record{Topic: eventsTopic, Key: []byte(ev.User), Value: value}, func(_ *kgo.Record, err error) {
				if err != nil {
					failed.Add(1)
					return
				}
				produced.Add(1)
			})
		}
	}
}

type config struct {
	group       string
	workers     int
	processTime time.Duration
	maxPoll     int
	maxInflight int
	commitEvery time.Duration
	reportEvery time.Duration
}

func consume(ctx context.Context, app *kit.App) error {
	mode := kit.Env("MODE", "sequential")
	cfg := config{
		group:       kit.Env("GROUP", defaultGroup),
		workers:     max(kit.EnvInt("WORKERS", 64), 1),
		processTime: kit.EnvDuration("PROCESS_TIME", 20*time.Millisecond),
		maxPoll:     max(kit.EnvInt("MAX_POLL", 500), 1),
		maxInflight: max(kit.EnvInt("MAX_INFLIGHT", 500), 2),
		commitEvery: kit.EnvDuration("COMMIT_INTERVAL", time.Second),
		reportEvery: kit.EnvDuration("REPORT", 2*time.Second),
	}
	switch mode {
	case "sequential":
		return consumeSequential(ctx, app, cfg)
	case "per-partition", "per-key", "unordered":
		return consumeConcurrent(ctx, app, mode, cfg)
	}
	return fmt.Errorf("bad MODE %q, want sequential|per-partition|per-key|unordered", mode)
}

// stats is shared by all modes; the reporter turns it into rec/s.
type stats struct {
	processed  atomic.Int64
	violations atomic.Int64
}

// apply is the "side effect" of a record: it checks that a key's sequence
// numbers are applied in order. last is the caller's per-key state.
func (s *stats) apply(app *kit.App, r *kgo.Record, last map[string]int64) {
	var ev Event
	if err := json.Unmarshal(r.Value, &ev); err != nil {
		app.Log.Error("undecodable record", "partition", r.Partition, "offset", r.Offset)
		return
	}
	k := ev.key()
	if prev := last[k]; ev.Seq < prev {
		if n := s.violations.Add(1); n <= 5 {
			app.Log.Warn("ORDER VIOLATION: applied an older event after a newer one",
				"user", ev.User, "seq", ev.Seq, "after_seq", prev, "partition", r.Partition, "offset", r.Offset)
		}
		return
	}
	last[k] = ev.Seq
}

// simulate stands in for the I/O a real handler does (a DB write, an HTTP
// call): 0.5x to 1.5x PROCESS_TIME, so concurrent records finish out of order.
func simulate(d time.Duration) {
	if d > 0 {
		time.Sleep(d/2 + rand.N(d))
	}
}

func (s *stats) report(ctx context.Context, app *kit.App, mode string, every time.Duration, extra func() []any) {
	t := time.NewTicker(every)
	defer t.Stop()
	start, last, lastAt := time.Now(), int64(0), time.Now()
	for {
		select {
		case <-ctx.Done():
			n := s.processed.Load()
			app.Log.Info("consumer summary", "mode", mode, "processed", n,
				"avg_rate", fmt.Sprintf("%.0f/s", float64(n)/time.Since(start).Seconds()), "violations", s.violations.Load())
			return
		case <-t.C:
		}
		n := s.processed.Load()
		args := []any{"mode", mode, "rate", fmt.Sprintf("%.0f/s", float64(n-last)/time.Since(lastAt).Seconds()),
			"processed", n, "violations", s.violations.Load()}
		app.Log.Info("progress", append(args, extra()...)...)
		last, lastAt = n, time.Now()
	}
}

// consumeSequential processes every record inline, in the poll loop.
// Correct and simple; its throughput is 1/PROCESS_TIME no matter how many
// partitions this member owns.
func consumeSequential(ctx context.Context, app *kit.App, cfg config) error {
	cl, err := app.NewClient(
		kgo.ConsumerGroup(cfg.group),
		kgo.ConsumeTopics(eventsTopic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return err
	}
	defer cl.CloseAllowingRebalance()
	if err := ensureTopic(ctx, app, cl); err != nil {
		return err
	}

	st := &stats{}
	rctx, stopReport := context.WithCancel(context.WithoutCancel(ctx))
	var wg sync.WaitGroup
	wg.Go(func() { st.report(rctx, app, "sequential", cfg.reportEvery, func() []any { return nil }) })
	defer func() { stopReport(); wg.Wait() }()

	last := map[string]int64{}
	for {
		fetches := cl.PollRecords(ctx, cfg.maxPoll)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		var done []*kgo.Record
		for _, r := range fetches.Records() {
			if ctx.Err() != nil {
				break
			}
			simulate(cfg.processTime)
			st.apply(app, r, last)
			st.processed.Add(1)
			done = append(done, r)
		}
		// Commit what was processed, not what was polled: on shutdown the
		// tail of the batch was never handled.
		if err := cl.CommitRecords(context.WithoutCancel(ctx), done...); err != nil {
			app.Log.Error("commit failed", "err", err)
		}
		cl.AllowRebalance()
	}
}

// partition is one assigned partition in the concurrent modes. A new one is
// created on every assignment, so order state never leaks across owners.
type partition struct {
	cl    *kgo.Client
	topic string
	id    int32

	mu        sync.Mutex
	offsets   *offsetTracker
	last      map[string]int64
	paused    bool
	revoked   bool
	running   sync.WaitGroup
	started   bool  // first record dispatched since assignment
	committed int64 // last committed offset; only touched under engine.commitMu

	recs chan *kgo.Record // per-partition mode only
}

type job struct {
	part *partition
	rec  *kgo.Record
}

type engine struct {
	app  *kit.App
	mode string
	cfg  config
	st   *stats
	cl   *kgo.Client
	seed maphash.Seed

	mu    sync.Mutex
	parts map[int32]*partition

	// commitMu serializes every commit with revokes, so a periodic commit can
	// never land after the final commit of a partition we gave away.
	commitMu sync.Mutex

	jobs    []chan job
	workers sync.WaitGroup
}

func consumeConcurrent(ctx context.Context, app *kit.App, mode string, cfg config) error {
	e := &engine{app: app, mode: mode, cfg: cfg, st: &stats{}, seed: maphash.MakeSeed(), parts: map[int32]*partition{}}
	e.startPool()

	cl, err := app.NewClient(
		kgo.ConsumerGroup(cfg.group),
		kgo.ConsumeTopics(eventsTopic),
		kgo.DisableAutoCommit(),
		// Rebalance callbacks run only between AllowRebalance and the next
		// poll, never while we dispatch: parts and channels are ours there.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(e.assigned),
		kgo.OnPartitionsRevoked(e.revoked),
		kgo.OnPartitionsLost(e.lost),
	)
	if err != nil {
		return err
	}
	e.cl = cl
	if err := ensureTopic(ctx, app, cl); err != nil {
		cl.Close()
		return err
	}
	app.Log.Info("consuming", "mode", mode, "workers", cfg.workers, "process_time", cfg.processTime,
		"max_inflight_per_partition", cfg.maxInflight, "group", cfg.group)

	bg, stopBg := context.WithCancel(context.WithoutCancel(ctx))
	var bgWG sync.WaitGroup
	bgWG.Go(func() { e.commitLoop(bg) })
	bgWG.Go(func() { e.st.report(bg, app, mode, cfg.reportEvery, e.gauges) })

	for {
		fetches := cl.PollRecords(ctx, cfg.maxPoll)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			break
		}
		fetches.EachError(func(t string, p int32, err error) {
			app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		var derr error
		fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
			if derr == nil {
				derr = e.dispatch(ftp)
			}
		})
		cl.AllowRebalance()
		if derr != nil {
			err = derr
			break
		}
	}

	// Leaving the group revokes everything: release() waits for running
	// records and commits. Only then may the workers go away.
	cl.CloseAllowingRebalance()
	e.stopPool()
	stopBg()
	bgWG.Wait()
	return err
}

func (e *engine) startPool() {
	if e.mode == "per-partition" {
		return
	}
	// TODO(13.3): fill e.jobs with queues (capacity MAX_INFLIGHT+MAX_POLL):
	// one per worker for per-key, a single shared one for unordered. Start
	// WORKERS goroutines with e.workers.Go, each calling e.handle for every
	// job of its queue until the queue is closed.
}

func (e *engine) stopPool() {
	for _, q := range e.jobs {
		close(q)
	}
	e.workers.Wait()
}

func (e *engine) assigned(_ context.Context, cl *kgo.Client, assigned map[string][]int32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for topic, ps := range assigned {
		for _, p := range ps {
			part := &partition{cl: cl, topic: topic, id: p, offsets: newOffsetTracker(), last: map[string]int64{}, committed: -1}
			if e.mode == "per-partition" {
				// TODO(13.2): give the partition its own queue, part.recs with
				// capacity MAX_INFLIGHT+MAX_POLL, and a goroutine (e.workers.Go)
				// that calls e.handle for each record until the queue is closed.
			}
			e.parts[p] = part
		}
		slices.Sort(ps)
		e.app.Log.Info("assigned", "partitions", ps)
	}
}

// dispatch hands one partition's polled records to the workers. It runs in
// the poll loop while rebalances are blocked.
func (e *engine) dispatch(ftp kgo.FetchTopicPartition) error {
	e.mu.Lock()
	part := e.parts[ftp.Partition]
	e.mu.Unlock()
	if part == nil || len(ftp.Records) == 0 {
		return nil
	}

	part.mu.Lock()
	if !part.started {
		part.started = true
		e.app.Log.Info("first record after assignment", "partition", part.id, "offset", ftp.Records[0].Offset)
	}
	for _, r := range ftp.Records {
		part.offsets.add(r.Offset, r.LeaderEpoch)
	}
	// TODO(13.4): backpressure. If the partition is not paused and has
	// MAX_INFLIGHT or more records in flight, pause fetching it
	// (part.cl.PauseFetchPartitions) and set part.paused.
	part.mu.Unlock()

	for _, r := range ftp.Records {
		if err := e.route(part, r); err != nil {
			return err
		}
	}
	return nil
}

// route hands one record to whoever processes it in this mode. A full queue
// blocks the poll loop; pausing keeps that rare.
func (e *engine) route(part *partition, r *kgo.Record) error {
	switch e.mode {
	case "per-partition":
		// TODO(13.2): send r to the partition's queue and return nil.
		return errTODO
	case "per-key":
		// TODO(13.3): send job{part, r} to the queue of worker
		// maphash.Bytes(e.seed, r.Key) % len(e.jobs).
		return errTODO
	case "unordered":
		// TODO(13.3): send job{part, r} to the shared queue.
		return errTODO
	}
	return fmt.Errorf("unknown mode %q", e.mode)
}

func (e *engine) handle(part *partition, r *kgo.Record) {
	part.mu.Lock()
	if part.revoked {
		// Dropped, not lost: it is past the committed watermark, so the next
		// owner of the partition starts from it.
		part.mu.Unlock()
		return
	}
	part.running.Add(1)
	part.mu.Unlock()
	defer part.running.Done()

	simulate(e.cfg.processTime)

	part.mu.Lock()
	e.st.apply(e.app, r, part.last)
	part.offsets.done(r.Offset)
	// TODO(13.4): if the partition is paused (and not revoked) and its
	// in-flight count dropped to MAX_INFLIGHT/2, resume it.
	part.mu.Unlock()
	e.st.processed.Add(1)
}

func (e *engine) revoked(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
	e.release(ctx, cl, revoked[eventsTopic], true)
}

// lost means another member may already own these partitions: committing
// now could rewind its progress, so we only stop.
func (e *engine) lost(ctx context.Context, cl *kgo.Client, lost map[string][]int32) {
	e.release(ctx, cl, lost[eventsTopic], false)
}

// release runs before the partitions are handed to another member: stop
// starting their records, wait for the ones running, commit the watermark.
func (e *engine) release(ctx context.Context, cl *kgo.Client, ps []int32, commit bool) {
	if len(ps) == 0 {
		return
	}
	started := time.Now()
	e.commitMu.Lock()
	defer e.commitMu.Unlock()

	e.mu.Lock()
	var parts []*partition
	for _, p := range ps {
		if part, ok := e.parts[p]; ok {
			parts = append(parts, part)
			delete(e.parts, p)
		}
	}
	e.mu.Unlock()

	for _, part := range parts {
		part.mu.Lock()
		part.revoked = true
		if part.paused {
			// Pauses outlive assignments: a partition that comes back later would stay paused.
			part.paused = false
			cl.ResumeFetchPartitions(map[string][]int32{part.topic: {part.id}})
		}
		if part.recs != nil {
			close(part.recs)
		}
		part.mu.Unlock()
	}
	// TODO(13.5): wait for the records still running (part.running) of every
	// released partition. Then, unless the partitions were lost, commit their
	// watermark with e.commitParts(ctx, cl, parts) and log what was committed,
	// how many queued records were dropped (offsets.inFlight() after the wait)
	// and how long it took.
	slices.Sort(ps)
	e.app.Log.Warn("released partitions WITHOUT waiting or committing (TODO 13.5)", "partitions", ps,
		"took", time.Since(started).Round(time.Millisecond))
}

func (e *engine) commitLoop(ctx context.Context) {
	t := time.NewTicker(e.cfg.commitEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		e.commitMu.Lock()
		e.mu.Lock()
		parts := slices.Collect(maps.Values(e.parts))
		e.mu.Unlock()
		e.commitParts(ctx, e.cl, parts)
		e.commitMu.Unlock()
	}
}

// commitParts commits the watermark of each partition that moved since its
// last commit. It returns the committed offset of every partition that has
// one. Caller holds commitMu.
func (e *engine) commitParts(ctx context.Context, cl *kgo.Client, parts []*partition) map[int32]int64 {
	offsets := map[int32]kgo.EpochOffset{}
	for _, part := range parts {
		part.mu.Lock()
		o, ok := part.offsets.committable()
		part.mu.Unlock()
		if ok && o.Offset != part.committed {
			offsets[part.id] = o
		}
	}
	if len(offsets) > 0 {
		var cerr error
		cl.CommitOffsetsSync(ctx, map[string]map[int32]kgo.EpochOffset{eventsTopic: offsets},
			func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
				cerr = commitError(resp, err)
			})
		if cerr != nil {
			e.app.Log.Error("commit failed", "err", cerr)
			offsets = nil
		}
	}
	out := map[int32]int64{}
	for _, part := range parts {
		if o, ok := offsets[part.id]; ok {
			part.committed = o.Offset
		}
		if part.committed >= 0 {
			out[part.id] = part.committed
		}
	}
	return out
}

func commitError(resp *kmsg.OffsetCommitResponse, err error) error {
	if err != nil {
		return err
	}
	var errs []error
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				errs = append(errs, fmt.Errorf("partition %d: %w", p.Partition, err))
			}
		}
	}
	return errors.Join(errs...)
}

// gauges adds the in-process state to the progress line.
func (e *engine) gauges() []any {
	e.mu.Lock()
	parts := slices.Collect(maps.Values(e.parts))
	e.mu.Unlock()
	inflight, paused := 0, 0
	for _, part := range parts {
		part.mu.Lock()
		inflight += part.offsets.inFlight()
		if part.paused {
			paused++
		}
		part.mu.Unlock()
	}
	return []any{"inflight", inflight, "paused", fmt.Sprintf("%d/%d", paused, len(parts))}
}

// lag prints, per partition, how far the group is behind: in records and in
// time, the age of the oldest record nobody has committed yet.
func lag(ctx context.Context, app *kit.App) error {
	group := kit.Env("GROUP", defaultGroup)
	every := kit.EnvDuration("EVERY", time.Second)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	for {
		if err := printLag(ctx, app, adm, group); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if every <= 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(every):
		}
	}
}

func printLag(ctx context.Context, app *kit.App, adm *kadm.Client, group string) error {
	lags, err := adm.Lag(ctx, group)
	if err != nil {
		return err
	}
	gl := lags[group]
	if err := gl.Error(); err != nil {
		return err
	}
	parts := gl.Lag[eventsTopic]

	next := map[int32]kgo.Offset{}
	for p, l := range parts {
		if l.Lag > 0 {
			at := l.Commit.At
			if at < 0 {
				at = l.Start.Offset
			}
			next[p] = kgo.NewOffset().At(at)
		}
	}
	heads, err := headTimestamps(ctx, app, next)
	if err != nil {
		return err
	}

	fmt.Printf("%s  group %s  state %s  members %d\n", time.Now().Format("15:04:05"), group, gl.State, len(gl.Members))
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintf(w, "PARTITION\tCOMMITTED\tEND\tLAG\tLAG TIME\tMEMBER\t\n")
	var total int64
	var worst time.Duration
	unknown := false
	for _, p := range slices.Sorted(maps.Keys(parts)) {
		l := parts[p]
		age := "0s"
		if ts, ok := heads[p]; ok {
			d := time.Since(ts).Round(100 * time.Millisecond)
			worst = max(worst, d)
			age = d.String()
		} else if l.Lag > 0 {
			age, unknown = "?", true
		}
		member := "-"
		if l.Member != nil {
			member = l.Member.MemberID[max(0, len(l.Member.MemberID)-8):]
		}
		total += max(l.Lag, 0)
		fmt.Fprintf(w, "%d\t%d\t%d\t%d\t%s\t%s\t\n", p, l.Commit.At, l.End.Offset, l.Lag, age, member)
	}
	worstAge := worst.String()
	if unknown {
		worstAge = "?"
	}
	fmt.Fprintf(w, "TOTAL\t\t\t%d\t%s\t\t\n\n", total, worstAge)
	return w.Flush()
}

// headTimestamps reads the first record at each given offset. Its timestamp
// is when the oldest uncommitted record was produced: the lag in time.
func headTimestamps(ctx context.Context, app *kit.App, at map[int32]kgo.Offset) (map[int32]time.Time, error) {
	out := map[int32]time.Time{}
	if len(at) == 0 {
		return out, nil
	}
	// TODO(13.6): read one record per partition at exactly these offsets
	// with a group-less client (kgo.ConsumePartitions), give up after ~2s,
	// and return each partition's first record timestamp.
	return out, nil
}
