// Reference solution for task 12. Try the task first: tasks/12-exactly-once/README.md
package main

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"idiomaticGo/kafka/internal/kit"
)

const (
	transfersTopic = "t12.transfers"
	ledgerTopic    = "t12.ledger"
	processorGroup = "t12.processor"
)

func main() {
	kit.Main(map[string]kit.Role{
		"producer":  produce,
		"processor": process,
		"consumer":  audit,
		"txns":      txns,
	})
}

type Transfer struct {
	ID     string    `json:"id"`
	From   string    `json:"from"`
	To     string    `json:"to"`
	Amount int64     `json:"amount"`
	At     time.Time `json:"at"`
}

// Entry is one leg of a transfer in the ledger. Debit and credit of the same
// transfer are keyed by different accounts, so they usually land in
// different partitions: exactly the case a single produce cannot make atomic.
type Entry struct {
	TransferID string `json:"transfer_id"`
	Leg        string `json:"leg"`
	Account    string `json:"account"`
	Amount     int64  `json:"amount"`
}

func ensureTopics(ctx context.Context, app *kit.App, cl *kgo.Client) error {
	for _, t := range []string{transfersTopic, ledgerTopic} {
		if err := app.EnsureTopic(ctx, cl, kit.Topic{Name: t, Partitions: 3}); err != nil {
			return err
		}
	}
	return nil
}

func produce(ctx context.Context, app *kit.App) error {
	interval := kit.EnvDuration("INTERVAL", 250*time.Millisecond)
	accounts := max(kit.EnvInt("ACCOUNTS", 10), 2)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := ensureTopics(ctx, app, cl); err != nil {
		return err
	}

	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}

		from, to := rand.IntN(accounts), rand.IntN(accounts-1)
		if to >= from {
			to++
		}
		t := Transfer{
			ID:     crand.Text()[:10],
			From:   fmt.Sprintf("acc-%d", from),
			To:     fmt.Sprintf("acc-%d", to),
			Amount: int64(rand.IntN(100) + 1),
			At:     time.Now(),
		}
		value, err := json.Marshal(t)
		if err != nil {
			return err
		}
		rec := &kgo.Record{Topic: transfersTopic, Key: []byte(t.From), Value: value}
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			app.Log.Error("produce failed", "err", err)
			continue
		}
		app.Log.Info("transfer", "id", t.ID, "from", t.From, "to", t.To, "amount", t.Amount,
			"partition", rec.Partition, "offset", rec.Offset)
	}
}

type processor struct {
	app      *kit.App
	sess     *kgo.GroupTransactSession
	failPct  int
	crashPct int
	slowTx   time.Duration
}

func process(ctx context.Context, app *kit.App) error {
	txnID := kit.Env("TRANSACTIONAL_ID", "t12-processor-"+app.Instance)

	sess, err := kgo.NewGroupTransactSession(app.ClientOpts(
		kgo.ConsumerGroup(processorGroup),
		kgo.ConsumeTopics(transfersTopic),
		kgo.TransactionalID(txnID),
		kgo.TransactionTimeout(kit.EnvDuration("TX_TIMEOUT", 40*time.Second)),
		kgo.SessionTimeout(kit.EnvDuration("SESSION_TIMEOUT", 45*time.Second)),
		// The input may itself be written by transactions (a chain of EOS
		// processors); never act on records that can still be aborted.
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.DefaultProduceTopic(ledgerTopic),
	)...)
	if err != nil {
		return err
	}
	defer sess.Close()
	if err := ensureTopics(ctx, app, sess.Client()); err != nil {
		return err
	}

	// InitProducerID now, not lazily at the first transaction: it bumps the
	// epoch, fencing our previous incarnation and aborting what it left open.
	pid, epoch, err := sess.Client().ProducerID(ctx)
	if err != nil {
		return fmt.Errorf("init producer id: %w", err)
	}
	app.Log.Info("transactional processor started", "transactional_id", txnID, "producer_id", pid, "epoch", epoch, "group", processorGroup)

	p := &processor{
		app:      app,
		sess:     sess,
		failPct:  kit.EnvInt("FAIL_PCT", 0),
		crashPct: kit.EnvInt("CRASH_PCT", 0),
		slowTx:   kit.EnvDuration("SLOW_TX", 0),
	}

	maxPoll := kit.EnvInt("MAX_POLL", 100)
	for {
		fetches := sess.PollRecords(ctx, maxPoll)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		if fetches.NumRecords() == 0 {
			continue
		}
		if err := p.transact(ctx, fetches.Records()); err != nil {
			return err
		}
	}
}

// transact turns one poll into one transaction: every ledger entry plus the
// input offsets commit together, or nothing does.
func (p *processor) transact(ctx context.Context, recs []*kgo.Record) error {
	// A started transaction is always ended, even if SIGTERM arrives mid-way:
	// canceling End leaves the outcome unknown.
	work := context.WithoutCancel(ctx)

	if err := p.sess.Begin(); err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	promise := kgo.AbortingFirstErrPromise(p.sess.Client())
	crashAt := crashPoint(p.crashPct, len(recs))
	transfers := 0
	for i, r := range recs {
		var t Transfer
		if err := json.Unmarshal(r.Value, &t); err != nil {
			p.app.Log.Error("skipping undecodable transfer", "partition", r.Partition, "offset", r.Offset, "err", err)
			continue
		}
		p.sess.Produce(work, entry(t, "debit"), promise.Promise())
		if i == crashAt {
			// Flush so the half-written transfer really is in the log when we die.
			_ = p.sess.Client().Flush(work)
			p.app.Log.Error("CRASH between debit and credit (CRASH_PCT)", "transfer", t.ID)
			os.Exit(1)
		}
		p.sess.Produce(work, entry(t, "credit"), promise.Promise())
		transfers++
	}

	commit, reason := kgo.TryCommit, ""
	switch err := promise.Err(); {
	case err != nil:
		commit, reason = kgo.TryAbort, "produce failed: "+err.Error()
	case roll(p.failPct):
		commit, reason = kgo.TryAbort, "simulated failure after writing (FAIL_PCT)"
	case p.slowTx > 0:
		p.app.Log.Warn("holding the transaction open (SLOW_TX)", "for", p.slowTx)
		select {
		case <-time.After(p.slowTx):
		case <-ctx.Done():
			commit, reason = kgo.TryAbort, "shutdown while the transaction was open"
		}
	}

	committed, err := p.sess.End(work, commit)
	switch {
	case errors.Is(err, kerr.ProducerFenced):
		return fmt.Errorf("fenced: a newer instance owns our transactional ID: %w", err)
	case err != nil:
		// Not retryable either, e.g. our epoch is stale because the coordinator
		// aborted the transaction on timeout. Die; the restart re-inits cleanly.
		return fmt.Errorf("end transaction: %w", err)
	}
	switch {
	case committed:
		p.app.Log.Info("transaction committed", "transfers", transfers, "input", offsetRanges(recs))
	case reason != "":
		p.app.Log.Warn("transaction aborted", "transfers", transfers, "reason", reason)
	default:
		// End downgraded our commit to an abort: the group rebalanced or our
		// member was fenced. The input offsets were rewound; we re-poll them.
		p.app.Log.Warn("transaction aborted by the session (rebalance)", "transfers", transfers)
	}
	return nil
}

func entry(t Transfer, leg string) *kgo.Record {
	e := Entry{TransferID: t.ID, Leg: leg, Account: t.To, Amount: t.Amount}
	if leg == "debit" {
		e.Account, e.Amount = t.From, -t.Amount
	}
	value, _ := json.Marshal(e)
	return &kgo.Record{Key: []byte(e.Account), Value: value}
}

// offsetRanges renders the polled input as "p0@12-15 p2@7-7" for the logs.
func offsetRanges(recs []*kgo.Record) string {
	first, last := map[int32]int64{}, map[int32]int64{}
	for _, r := range recs {
		if _, ok := first[r.Partition]; !ok {
			first[r.Partition] = r.Offset
		}
		last[r.Partition] = r.Offset
	}
	var out []string
	for _, p := range slices.Sorted(maps.Keys(first)) {
		out = append(out, fmt.Sprintf("p%d@%d-%d", p, first[p], last[p]))
	}
	return strings.Join(out, " ")
}

func roll(pct int) bool { return pct > 0 && rand.IntN(100) < pct }

// crashPoint picks, with probability pct per poll, the transfer of the batch
// after whose debit the process dies; -1 means no crash this time.
func crashPoint(pct, n int) int {
	if !roll(pct) {
		return -1
	}
	return rand.IntN(n)
}

// audit reads the whole ledger without a group and checks the invariants:
// every transfer has exactly one debit and one credit, and money is conserved.
func audit(ctx context.Context, app *kit.App) error {
	isolation := kit.Env("ISOLATION", "read_committed")
	every := kit.EnvDuration("REPORT", 2*time.Second)
	grace := kit.EnvDuration("GRACE", 5*time.Second)

	level := kgo.ReadCommitted()
	switch isolation {
	case "read_committed":
	case "read_uncommitted":
		level = kgo.ReadUncommitted()
	default:
		return fmt.Errorf("bad ISOLATION %q, want read_committed|read_uncommitted", isolation)
	}

	cl, err := app.NewClient(kgo.ConsumeTopics(ledgerTopic), kgo.FetchIsolationLevel(level))
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := ensureTopics(ctx, app, cl); err != nil {
		return err
	}
	app.Log.Info("auditing the ledger from the start", "isolation", isolation)

	l := newLedger(app, grace)
	next := time.Now().Add(every)
	for {
		pctx, cancel := context.WithDeadline(ctx, next)
		fetches := cl.PollFetches(pctx)
		cancel()
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) {
				app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
			}
		})
		fetches.EachRecord(l.add)
		if !time.Now().Before(next) {
			l.report(isolation)
			next = time.Now().Add(every)
		}
	}
}

type transferState struct {
	debits, credits int
	sum             int64
	firstSeen       time.Time
	flagged         bool
}

type ledger struct {
	app       *kit.App
	grace     time.Duration
	transfers map[string]*transferState
	last      map[int32]int64
	records   int
	gaps      int
}

func newLedger(app *kit.App, grace time.Duration) *ledger {
	return &ledger{app: app, grace: grace, transfers: map[string]*transferState{}, last: map[int32]int64{}}
}

func (l *ledger) add(r *kgo.Record) {
	l.records++
	if prev, ok := l.last[r.Partition]; ok && r.Offset != prev+1 {
		l.gaps++
	}
	l.last[r.Partition] = r.Offset

	var e Entry
	if err := json.Unmarshal(r.Value, &e); err != nil {
		l.app.Log.Error("undecodable ledger entry", "partition", r.Partition, "offset", r.Offset)
		return
	}
	t, ok := l.transfers[e.TransferID]
	if !ok {
		t = &transferState{firstSeen: time.Now()}
		l.transfers[e.TransferID] = t
	}
	if e.Leg == "debit" {
		t.debits++
	} else {
		t.credits++
	}
	t.sum += e.Amount
}

func (l *ledger) report(isolation string) {
	var balanced, pending, incomplete, duplicated int
	var created int64
	for id, t := range l.transfers {
		problem := ""
		switch {
		case t.debits > 1 || t.credits > 1:
			duplicated++
			problem = "duplicated legs"
		case t.debits == 0 || t.credits == 0:
			// Both legs commit atomically, but they live in different
			// partitions and arrive in different fetches: allow a moment.
			if time.Since(t.firstSeen) < l.grace {
				pending++
				continue
			}
			incomplete++
			problem = "missing leg"
		default:
			balanced++
		}
		created += t.sum
		if problem != "" && !t.flagged {
			t.flagged = true
			l.app.Log.Warn("bad transfer", "id", id, "problem", problem, "debits", t.debits, "credits", t.credits, "net", t.sum)
		}
	}
	l.app.Log.Info("audit", "isolation", isolation, "records", l.records, "transfers", len(l.transfers),
		"balanced", balanced, "pending", pending, "incomplete", incomplete, "duplicated", duplicated,
		"offset_gaps", l.gaps, "money_created", created)
}

// txns shows why a read_committed consumer is stuck: per partition the high
// watermark, the last stable offset and the open transaction holding it back.
func txns(ctx context.Context, app *kit.App) error {
	every := kit.EnvDuration("EVERY", 0)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	for {
		if err := printTxns(ctx, adm); err != nil {
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

func printTxns(ctx context.Context, adm *kadm.Client) error {
	hw, err := adm.ListEndOffsets(ctx, ledgerTopic)
	if err != nil {
		return err
	}
	lso, err := adm.ListCommittedOffsets(ctx, ledgerTopic)
	if err != nil {
		return err
	}
	producers, err := adm.DescribeProducers(ctx, kadm.TopicsSet{ledgerTopic: nil})
	if err != nil {
		return err
	}
	described, err := adm.DescribeTransactions(ctx)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "%s\nPARTITION\tHW\tLSO\tHW-LSO\tOPEN TRANSACTIONS (producer id/epoch from offset)\n", time.Now().Format("15:04:05"))
	partitions := hw[ledgerTopic]
	for _, p := range slices.Sorted(maps.Keys(partitions)) {
		h := partitions[p]
		s, _ := lso.Lookup(ledgerTopic, p)
		var open []string
		if dp, ok := producers[ledgerTopic].Partitions[p]; ok {
			for _, pr := range dp.ActiveProducers.Sorted() {
				if pr.CurrentTxnStartOffset >= 0 {
					open = append(open, fmt.Sprintf("%d/%d from %d", pr.ProducerID, pr.ProducerEpoch, pr.CurrentTxnStartOffset))
				}
			}
		}
		fmt.Fprintf(w, "%d\t%d\t%d\t%d\t%s\n", p, h.Offset, s.Offset, h.Offset-s.Offset, strings.Join(open, ", "))
	}
	fmt.Fprintf(w, "\nTRANSACTIONAL ID\tSTATE\tPRODUCER ID\tEPOCH\tTIMEOUT\tOPEN FOR\tPARTITIONS\n")
	for _, d := range described.Sorted() {
		if !strings.HasPrefix(d.TxnID, "t12-") {
			continue
		}
		openFor := "-"
		if d.State == "Ongoing" && d.StartTimestamp > 0 {
			openFor = time.Since(time.UnixMilli(d.StartTimestamp)).Round(100 * time.Millisecond).String()
		}
		var parts []string
		d.Topics.Each(func(t string, p int32) { parts = append(parts, fmt.Sprintf("%s-%d", t, p)) })
		slices.Sort(parts)
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\t%s\t%s\n", d.TxnID, d.State, d.ProducerID, d.ProducerEpoch,
			time.Duration(d.TimeoutMillis)*time.Millisecond, openFor, strings.Join(parts, " "))
	}
	fmt.Fprintln(w)
	return w.Flush()
}
