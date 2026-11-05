// Task 12: exactly-once. Read README.md first; the TODOs follow its "Your task" steps.
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
	cl       *kgo.Client
	crashPct int
}

func process(ctx context.Context, app *kit.App) error {
	// TODO(12.1): replace this client with kgo.NewGroupTransactSession(app.ClientOpts(...)...):
	// TransactionalID (TRANSACTIONAL_ID, default "t12-processor-"+app.Instance),
	// TransactionTimeout (TX_TIMEOUT) and FetchIsolationLevel(ReadCommitted). Then call sess.Client().ProducerID(ctx)
	// and log the producer id and epoch.
	cl, err := app.NewClient(
		kgo.ConsumerGroup(processorGroup),
		kgo.ConsumeTopics(transfersTopic),
		kgo.SessionTimeout(kit.EnvDuration("SESSION_TIMEOUT", 45*time.Second)),
		kgo.DisableAutoCommit(),
		kgo.DefaultProduceTopic(ledgerTopic),
	)
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := ensureTopics(ctx, app, cl); err != nil {
		return err
	}
	app.Log.Info("NON-transactional processor started", "group", processorGroup)

	p := &processor{app: app, cl: cl, crashPct: kit.EnvInt("CRASH_PCT", 0)}

	maxPoll := kit.EnvInt("MAX_POLL", 100)
	for {
		fetches := cl.PollRecords(ctx, maxPoll)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		if fetches.NumRecords() == 0 {
			continue
		}
		if err := p.handle(ctx, fetches.Records()); err != nil {
			return err
		}
	}
}

// handle is the naive version: two independent writes per transfer, then a
// separate offset commit. Every gap between them is a place to crash.
func (p *processor) handle(ctx context.Context, recs []*kgo.Record) error {
	// TODO(12.2): make this one transaction per poll: sess.Begin(), both legs
	// with sess.Produce and a kgo.AbortingFirstErrPromise, then
	// sess.End(TryCommit), or TryAbort on a produce error / FAIL_PCT. Keep the
	// CRASH_PCT exit between the legs. Use context.WithoutCancel for the work.
	crashAt := crashPoint(p.crashPct, len(recs))
	transfers := 0
	for i, r := range recs {
		var t Transfer
		if err := json.Unmarshal(r.Value, &t); err != nil {
			p.app.Log.Error("skipping undecodable transfer", "partition", r.Partition, "offset", r.Offset, "err", err)
			continue
		}
		if err := p.cl.ProduceSync(ctx, entry(t, "debit")).FirstErr(); err != nil {
			return produceErr(ctx, err)
		}
		if i == crashAt {
			p.app.Log.Error("CRASH between debit and credit (CRASH_PCT)", "transfer", t.ID)
			os.Exit(1)
		}
		if err := p.cl.ProduceSync(ctx, entry(t, "credit")).FirstErr(); err != nil {
			return produceErr(ctx, err)
		}
		transfers++
	}
	if err := p.cl.CommitUncommittedOffsets(ctx); err != nil {
		p.app.Log.Error("commit failed", "err", err)
		return nil
	}
	p.app.Log.Info("batch done", "transfers", transfers, "input", offsetRanges(recs))
	return nil
}

func produceErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return nil
	}
	return fmt.Errorf("produce: %w", err)
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

	_ = level

	// TODO(12.3): pass kgo.FetchIsolationLevel(level). Until then both auditors
	// read uncommitted, which is franz-go's (and Java's) default.
	cl, err := app.NewClient(kgo.ConsumeTopics(ledgerTopic))
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
	// TODO(12.4): fill these with adm.ListEndOffsets, adm.ListCommittedOffsets
	// (the LSO), adm.DescribeProducers(ctx, kadm.TopicsSet{ledgerTopic: nil})
	// and adm.DescribeTransactions(ctx).
	var (
		hw, lso   kadm.ListedOffsets
		producers kadm.DescribedProducersTopics
		described kadm.DescribedTransactions
	)

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
