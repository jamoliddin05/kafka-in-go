// Task 06: offsets and delivery semantics. Read README.md first; the TODOs follow its "Your task" steps.
package main

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"idiomaticGo/kafka/internal/kit"
)

const (
	paymentsTopic = "t06.payments"
	groupName     = "t06.ledger"
	partitions    = 3
)

const (
	modeAutocommit  = "autocommit"
	modeGreedy      = "greedy"
	modeMarks       = "marks"
	modeAtMostOnce  = "at-most-once"
	modeAtLeastOnce = "at-least-once"
)

var errTODO = errors.New("not implemented yet, see README.md")

func main() {
	kit.Main(map[string]kit.Role{
		"producer": produce,
		"consumer": consume,
		"offsets":  offsets,
		"reset":    reset,
	})
}

type Payment struct {
	ID          string    `json:"id"`
	Seq         int64     `json:"seq"`
	Account     string    `json:"account"`
	AmountCents int64     `json:"amount_cents"`
	At          time.Time `json:"at"`
}

func ensureTopic(ctx context.Context, app *kit.App, cl *kgo.Client) error {
	return app.EnsureTopic(ctx, cl, kit.Topic{Name: paymentsTopic, Partitions: partitions})
}

func produce(ctx context.Context, app *kit.App) error {
	rate := max(kit.EnvInt("RATE", 100), 1)
	run := crand.Text()[:6] // payment IDs stay unique across producer restarts

	cl, err := app.NewClient(kgo.DefaultProduceTopic(paymentsTopic))
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := ensureTopic(ctx, app, cl); err != nil {
		return err
	}

	var produced, failed atomic.Int64
	tick := time.NewTicker(time.Second / time.Duration(rate))
	defer tick.Stop()
	report := time.NewTicker(5 * time.Second)
	defer report.Stop()

	app.Log.Info("producing", "topic", paymentsTopic, "rate_per_sec", rate, "run", run)
	for seq := int64(1); ; {
		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := cl.Flush(flushCtx)
			app.Log.Info("produced", "total", produced.Load(), "failed", failed.Load())
			return err
		case <-report.C:
			app.Log.Info("produced", "total", produced.Load(), "failed", failed.Load())
		case <-tick.C:
			p := Payment{
				ID:          fmt.Sprintf("pay-%s-%d", run, seq),
				Seq:         seq,
				Account:     fmt.Sprintf("acct-%d", rand.IntN(100)),
				AmountCents: 100 + rand.Int64N(10_000),
				At:          time.Now(),
			}
			seq++
			value, err := json.Marshal(p)
			if err != nil {
				return err
			}
			cl.Produce(ctx, &kgo.Record{Key: []byte(p.Account), Value: value}, func(_ *kgo.Record, err error) {
				if err != nil {
					failed.Add(1)
					return
				}
				produced.Add(1)
			})
		}
	}
}

type consumerConfig struct {
	group          string
	mode           string
	start          string
	crashEvery     int
	restartAfter   time.Duration
	sessionTimeout time.Duration
	commitEvery    time.Duration
	processTime    time.Duration
	maxPoll        int
	reportEvery    time.Duration
}

// consume simulates crashes in-process: a crash abandons the client and builds
// a new one, while the ledger (the payments DB) survives and counts the damage.
func consume(ctx context.Context, app *kit.App) error {
	cfg := consumerConfig{
		group:          kit.Env("GROUP", groupName),
		mode:           kit.Env("MODE", modeAutocommit),
		start:          kit.Env("START", "earliest"),
		crashEvery:     kit.EnvInt("CRASH_EVERY", 0),
		restartAfter:   kit.EnvDuration("RESTART_AFTER", time.Second),
		sessionTimeout: kit.EnvDuration("SESSION_TIMEOUT", 10*time.Second),
		commitEvery:    kit.EnvDuration("AUTOCOMMIT_INTERVAL", 5*time.Second),
		processTime:    kit.EnvDuration("PROCESS_TIME", 2*time.Millisecond),
		maxPoll:        kit.EnvInt("MAX_POLL_RECORDS", 100),
		reportEvery:    kit.EnvDuration("REPORT_EVERY", 5*time.Second),
	}
	led := newLedger(app.Log)
	go led.reportEvery(ctx, cfg.reportEvery)

	app.Log.Info("consuming", "group", cfg.group, "mode", cfg.mode, "start", cfg.start,
		"crash_every", cfg.crashEvery, "session_timeout", cfg.sessionTimeout)
	for incarnation := 1; ; incarnation++ {
		crashed, err := runIncarnation(ctx, app, cfg, led, incarnation)
		if err != nil {
			return err
		}
		if !crashed {
			led.report("final ledger")
			return reportExitCommits(context.Background(), app, cfg.group, led)
		}
		app.Log.Warn("restarting after simulated crash", "in", cfg.restartAfter)
		select {
		case <-ctx.Done():
			led.report("final ledger")
			return nil
		case <-time.After(cfg.restartAfter):
		}
	}
}

// runIncarnation is one process lifetime. crashed=true means it was killed
// on purpose: no commit, no LeaveGroup, exactly what kill -9 leaves behind.
func runIncarnation(ctx context.Context, app *kit.App, cfg consumerConfig, led *ledger, incarnation int) (crashed bool, err error) {
	start, err := startOffset(cfg.start)
	if err != nil {
		return false, err
	}
	modeOpts, err := modeOptions(cfg.mode, app.Log)
	if err != nil {
		return false, err
	}

	// Cancelling the client's own context fails every request it would still send,
	// final commit and LeaveGroup included. Commits use it, so Ctrl+C can't cut one.
	life, die := context.WithCancel(context.Background())
	defer die()
	opts := append([]kgo.Opt{
		kgo.WithContext(life),
		kgo.ConsumerGroup(cfg.group),
		kgo.ConsumeTopics(paymentsTopic),
		kgo.ConsumeStartOffset(start),
		// A crashed member keeps its partitions until this expires (task 07).
		kgo.SessionTimeout(cfg.sessionTimeout),
		kgo.AutoCommitInterval(cfg.commitEvery),
		kgo.ClientID(fmt.Sprintf("t06-consumer-%s-%d", app.Instance, incarnation)),
	}, modeOpts...)

	cl, err := app.NewClient(opts...)
	if err != nil {
		return false, err
	}
	// BlockRebalanceOnPoll (at-least-once) makes a plain Close wait forever
	// for an AllowRebalance that a crashed loop never calls.
	defer cl.CloseAllowingRebalance()
	if err := ensureTopic(ctx, app, cl); err != nil {
		return false, err
	}
	app.Log.Info("incarnation started", "incarnation", incarnation)

	// kill -9 lands at an arbitrary moment: somewhere between 0.5 and 1.5 x CRASH_EVERY.
	crashAt := 0
	if cfg.crashEvery > 0 {
		crashAt = cfg.crashEvery/2 + rand.IntN(cfg.crashEvery) + 1
	}
	processedHere := 0
	// Stop only between batches: whatever a poll returned gets processed.
	for ctx.Err() == nil {
		fetches := cl.PollRecords(ctx, cfg.maxPoll)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
			}
		})
		recs := fetches.Records()

		if cfg.mode == modeAtMostOnce && len(recs) > 0 {
			// TODO(06.1): commit the batch BEFORE processing it, with
			// cl.CommitUncommittedOffsets(life). If the commit fails, log, call
			// cl.AllowRebalance() and skip the batch: it may have landed anyway.
		}

		for _, r := range recs {
			if crashAt > 0 && processedHere >= crashAt {
				die()
				app.Log.Warn("CRASH: simulated kill -9, no commit, no LeaveGroup",
					"incarnation", incarnation, "processed_by_this_incarnation", processedHere,
					"at_partition", r.Partition, "at_offset", r.Offset)
				return true, nil
			}
			time.Sleep(cfg.processTime)
			led.apply(r, incarnation)
			processedHere++
			if cfg.mode == modeMarks {
				// TODO(06.3): mark r as safe to autocommit.
			}
		}

		if cfg.mode == modeAtLeastOnce && len(recs) > 0 {
			// TODO(06.2): commit AFTER the whole batch is processed, with
			// cl.CommitUncommittedOffsets(life). Log a failure and carry on.
		}
		cl.AllowRebalance()
	}
	finalCommit(app, cl, cfg.mode)
	return false, nil
}

func modeOptions(mode string, log *slog.Logger) ([]kgo.Opt, error) {
	switch mode {
	case modeAutocommit:
		// franz-go default: every 5s, commit what was polled before the latest poll.
		return nil, nil
	case modeGreedy:
		// Every 5s, commit everything polled, processed or not.
		return []kgo.Opt{kgo.GreedyAutoCommit()}, nil
	case modeMarks:
		// TODO(06.3): autocommit only what we marked (kgo.AutoCommitMarks).
		return nil, errTODO
	case modeAtMostOnce:
		// TODO(06.1): no autocommit; we commit ourselves before processing.
		return nil, errTODO
	case modeAtLeastOnce:
		// TODO(06.2): no autocommit; block rebalances while a batch is in
		// flight (kgo.BlockRebalanceOnPoll) and commit in OnPartitionsRevoked.
		return nil, errTODO
	}
	return nil, fmt.Errorf("unknown MODE %q, want %s", mode,
		strings.Join([]string{modeAutocommit, modeGreedy, modeMarks, modeAtMostOnce, modeAtLeastOnce}, "|"))
}

// finalCommit runs on a clean shutdown, after everything polled has been
// processed.
func finalCommit(app *kit.App, cl *kgo.Client, mode string) {
	// TODO(06.4): run MODE=autocommit with a backlog (README step 4), stop it
	// with Ctrl+C and read the "exit:" lines. Find out in franz-go what Close
	// commits, decide which modes need a commit here and add it (with its own
	// timeout: ctx is already cancelled at this point).
	_, _, _ = app, cl, mode
}

// startOffset maps START to where a partition with no committed offset begins.
func startOffset(s string) (kgo.Offset, error) {
	switch s {
	case "earliest":
		return kgo.NewOffset().AtStart(), nil
	case "latest":
		return kgo.NewOffset().AtEnd(), nil
	case "none":
		return kgo.NewOffset().AtCommitted(), nil
	}
	return kgo.Offset{}, fmt.Errorf("bad START %q, want earliest|latest|none", s)
}

// ledger is the "database" the consumer writes payments into. It survives
// simulated crashes and knows, per partition, which offsets were applied.
type ledger struct {
	log *slog.Logger

	mu        sync.Mutex
	applied   map[int32]map[int64]int // partition -> offset -> times applied
	first     map[int32]int64
	last      map[int32]int64 // highest offset applied
	resumedIn map[int32]int   // incarnation that last touched the partition
	processed int
	crashes   int
}

func newLedger(log *slog.Logger) *ledger {
	return &ledger{
		log:       log,
		applied:   map[int32]map[int64]int{},
		first:     map[int32]int64{},
		last:      map[int32]int64{},
		resumedIn: map[int32]int{},
	}
}

func (l *ledger) apply(r *kgo.Record, incarnation int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	p := r.Partition
	if l.resumedIn[p] != incarnation {
		l.resumedIn[p] = incarnation
		l.crashes = max(l.crashes, incarnation-1)
		if last, ok := l.last[p]; ok {
			// The first record after a restart says what the commit covered.
			switch gap := r.Offset - (last + 1); {
			case gap < 0:
				l.log.Warn("resumed BEHIND what we processed: duplicates", "partition", p,
					"resumed_at", r.Offset, "last_processed", last, "reprocessing", -gap)
			case gap > 0:
				l.log.Warn("resumed AHEAD of what we processed: records LOST", "partition", p,
					"resumed_at", r.Offset, "last_processed", last, "skipped", gap)
			default:
				l.log.Info("resumed exactly after the last processed record", "partition", p, "resumed_at", r.Offset)
			}
		}
	}

	if l.applied[p] == nil {
		l.applied[p] = map[int64]int{}
		l.first[p] = r.Offset
	}
	l.applied[p][r.Offset]++
	l.first[p] = min(l.first[p], r.Offset)
	if cur, ok := l.last[p]; !ok || r.Offset > cur {
		l.last[p] = r.Offset
	}
	l.processed++
}

// counts returns unique applied offsets and holes below the highest applied one:
// a partition is read in order, so a hole will never be processed.
func (l *ledger) counts() (unique, lost int) {
	for p, offs := range l.applied {
		unique += len(offs)
		lost += int(l.last[p]-l.first[p]+1) - len(offs)
	}
	return unique, lost
}

func (l *ledger) report(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	unique, lost := l.counts()
	l.log.Info(msg, "processed", l.processed, "unique", unique,
		"duplicates", l.processed-unique, "lost", lost, "crashes", l.crashes)
}

func (l *ledger) reportEvery(ctx context.Context, every time.Duration) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			l.report("ledger")
		}
	}
}

func (l *ledger) lastProcessed() map[int32]int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return maps.Clone(l.last)
}

// reportExitCommits compares what is committed after a clean shutdown with
// what we actually processed: the difference is what the next run redoes.
func reportExitCommits(ctx context.Context, app *kit.App, group string, led *ledger) error {
	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	committed, err := kadm.NewClient(cl).FetchOffsets(ctx, group)
	if err != nil {
		return err
	}
	last := led.lastProcessed()
	for _, p := range slices.Sorted(maps.Keys(last)) {
		c, ok := committed.Lookup(paymentsTopic, p)
		if !ok {
			app.Log.Warn("exit: nothing committed", "partition", p, "last_processed", last[p])
			continue
		}
		app.Log.Info("exit: committed vs processed", "partition", p, "committed", c.At,
			"last_processed", last[p], "next_run_reprocesses", max(last[p]+1-c.At, 0),
			"next_run_skips", max(c.At-(last[p]+1), 0))
	}
	return nil
}

// offsets prints, per partition, where the group will resume and how far
// behind the log end that is.
func offsets(ctx context.Context, app *kit.App) error {
	group := kit.Env("GROUP", groupName)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	state, members, err := groupState(ctx, adm, group)
	if err != nil {
		return err
	}
	// TODO(06.5): fill these with adm.FetchOffsets (the group's commits),
	// adm.ListStartOffsets and adm.ListEndOffsets for paymentsTopic.
	var committed kadm.OffsetResponses
	var starts, ends kadm.ListedOffsets

	fmt.Printf("group=%s state=%s members=%d   (COMMITTED is the next offset the group will read)\n", group, state, members)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "PARTITION\tLOG-START\tCOMMITTED\tEND\tLAG")
	for p := range int32(partitions) {
		s, _ := starts.Lookup(paymentsTopic, p)
		e, _ := ends.Lookup(paymentsTopic, p)
		com, lag := "-", "-"
		if c, ok := committed.Lookup(paymentsTopic, p); ok && c.Err == nil && c.At >= 0 {
			com, lag = fmt.Sprint(c.At), fmt.Sprint(e.Offset-c.At)
		}
		fmt.Fprintf(w, "%d\t%d\t%s\t%d\t%s\n", p, s.Offset, com, e.Offset, lag)
	}
	return w.Flush()
}

// groupState works for classic and KIP-848 groups alike: ListGroups reports
// both, unlike DescribeGroups.
func groupState(ctx context.Context, adm *kadm.Client, group string) (state string, members int, err error) {
	listed, err := adm.ListGroups(ctx)
	if err != nil {
		return "", 0, err
	}
	g, ok := listed[group]
	if !ok {
		return "(none)", 0, nil
	}
	if described, err := adm.DescribeGroups(ctx, group); err == nil && described[group].Err == nil {
		members = len(described[group].Members)
	} else if cgs, err := adm.DescribeConsumerGroups(ctx, group); err == nil && cgs[group].Err == nil {
		members = len(cgs[group].Members)
	}
	return g.State, members, nil
}

// reset moves the group's committed offsets, like
// kafka-consumer-groups --reset-offsets. Dry run unless EXECUTE=true.
func reset(ctx context.Context, app *kit.App) error {
	group := kit.Env("GROUP", groupName)
	to := kit.Env("TO", "ago:5m")
	execute := kit.EnvBool("EXECUTE", false)
	force := kit.EnvBool("FORCE", false)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	state, members, err := groupState(ctx, adm, group)
	if err != nil {
		return err
	}
	fmt.Printf("group=%s state=%s members=%d\n", group, state, members)
	// TODO(06.6): refuse (return an error) unless the group has no members
	// (state "(none)", "Empty" or "Dead") or FORCE=true. Try FORCE=true
	// against a running consumer: what does the broker answer?
	_ = force

	target, err := resetTarget(ctx, adm, to)
	if err != nil {
		return err
	}
	current, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "PARTITION\tCOMMITTED\tNEW\tEFFECT")
	for p := range int32(partitions) {
		t, ok := target.Lookup(paymentsTopic, p)
		if !ok {
			continue
		}
		com, effect := "-", "(no commit yet)"
		if c, ok := current.Lookup(paymentsTopic, p); ok && c.Err == nil && c.At >= 0 {
			com = fmt.Sprint(c.At)
			switch d := t.At - c.At; {
			case d < 0:
				effect = fmt.Sprintf("re-process %d", -d)
			case d > 0:
				effect = fmt.Sprintf("skip %d", d)
			default:
				effect = "unchanged"
			}
		}
		fmt.Fprintf(w, "%d\t%s\t%d\t%s\n", p, com, t.At, effect)
	}
	if err := w.Flush(); err != nil {
		return err
	}

	if !execute {
		fmt.Println("dry run: set EXECUTE=true to commit these offsets")
		return nil
	}
	// TODO(06.6): commit target for the group with adm.CommitOffsets and
	// check both the request error and the per-partition errors.
	return errTODO
}

// resetTarget understands: start | end | ago:<duration>.
func resetTarget(ctx context.Context, adm *kadm.Client, to string) (kadm.Offsets, error) {
	// TODO(06.6): list the start offsets, the end offsets, or the offsets
	// after now-d (adm.ListOffsetsAfterMilli) of paymentsTopic and return them
	// with .Offsets(). Reject anything else.
	_, _, _ = ctx, adm, to
	return nil, errTODO
}
