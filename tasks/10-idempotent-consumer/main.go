// Task 10: idempotent consumer. Read README.md first; the TODOs follow its "Your task" steps.
package main

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"idiomaticGo/kafka/internal/kit"
)

const (
	depositsTopic = "t10.deposits"
	ledgerGroup   = "t10.ledger"
)

var (
	errTODO           = errors.New("not implemented yet, see README.md")
	errSimulatedCrash = errors.New("simulated crash")
)

func main() {
	kit.Main(map[string]kit.Role{
		"producer": produce,
		"consumer": consume,
		"check":    check,
		"cleanup":  cleanup,
		"reset":    reset,
	})
}

// Deposit is the event. EventID is minted once, where the business fact
// happens, and every copy of the event carries it: resends, replays, redrives.
type Deposit struct {
	EventID   string    `json:"event_id"`
	AccountID string    `json:"account_id"`
	Amount    int64     `json:"amount"` // cents
	At        time.Time `json:"at"`
}

func produce(ctx context.Context, app *kit.App) error {
	count := kit.EnvInt("COUNT", 2000)
	rate := kit.EnvInt("RATE", 50)
	accounts := kit.EnvInt("ACCOUNTS", 10)
	dupPct := kit.EnvInt("DUP_PCT", 5)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := ensureTopic(ctx, app, cl); err != nil {
		return err
	}

	app.Log.Info("producing", "count", count, "rate", rate, "accounts", accounts, "dup_pct", dupPct)
	var recent []Deposit
	var sent, resent int
	tick := time.NewTicker(time.Second / time.Duration(max(rate, 1)))
	defer tick.Stop()
	for count <= 0 || sent < count {
		select {
		case <-ctx.Done():
			app.Log.Info("producer stopped", "sent", sent, "resent", resent)
			return nil
		case <-tick.C:
		}

		d := Deposit{
			EventID:   crand.Text(),
			AccountID: fmt.Sprintf("acc-%02d", rand.IntN(accounts)),
			Amount:    100 * (1 + rand.Int64N(100)),
			At:        time.Now(),
		}
		if err := send(ctx, cl, d); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			app.Log.Error("produce failed", "err", err)
			continue
		}
		sent++
		recent = append(recent, d)
		if len(recent) > 100 {
			recent = recent[1:]
		}

		if rand.IntN(100) < dupPct {
			// An upstream retried after a timeout and published the same
			// business event again: same event_id, new offset. Producer
			// idempotence can't see this, it only dedupes its own retries.
			old := recent[rand.IntN(len(recent))]
			if err := send(ctx, cl, old); err != nil {
				app.Log.Error("re-send failed", "err", err)
				continue
			}
			resent++
			app.Log.Info("re-sent an old event", "event_id", old.EventID, "account", old.AccountID, "amount", money(old.Amount))
		}
		if sent%500 == 0 {
			app.Log.Info("progress", "sent", sent, "resent", resent)
		}
	}
	app.Log.Info("done", "sent", sent, "resent", resent, "records_on_log", sent+resent)
	return nil
}

func send(ctx context.Context, cl *kgo.Client, d Deposit) error {
	value, err := json.Marshal(d)
	if err != nil {
		return err
	}
	// Keyed by account: every deposit of an account goes to one partition,
	// so one consumer applies them in order.
	rec := &kgo.Record{Topic: depositsTopic, Key: []byte(d.AccountID), Value: value}
	return cl.ProduceSync(ctx, rec).FirstErr()
}

type consumer struct {
	app         *kit.App
	pool        *pgxpool.Pool
	inbox       bool
	offsetsInDB bool
	crashAfter  int
	commitEvery int

	handled, applied, skipped int
}

func consume(ctx context.Context, app *kit.App) error {
	pool, err := app.ConnectDB(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := ensureSchema(ctx, pool); err != nil {
		return err
	}
	if err := ensureTopic(ctx, app, nil); err != nil {
		return err
	}

	c := &consumer{
		app:         app,
		pool:        pool,
		inbox:       kit.EnvBool("INBOX", false),
		offsetsInDB: kit.EnvBool("OFFSETS_IN_DB", false),
		crashAfter:  kit.EnvInt("CRASH_AFTER_DB", 250),
		commitEvery: max(kit.EnvInt("COMMIT_EVERY", 100), 1),
	}
	restartDelay := kit.EnvDuration("RESTART_DELAY", 2*time.Second)
	app.Log.Info("consuming", "group", ledgerGroup, "inbox", c.inbox, "offsets_in_db", c.offsetsInDB,
		"crash_after_db", c.crashAfter, "commit_every", c.commitEvery)

	crashes := 0
	defer func() {
		app.Log.Info("consumer stopped", "handled", c.handled, "applied", c.applied, "skipped", c.skipped, "crashes", crashes)
	}()
	for {
		err := c.run(ctx)
		if !errors.Is(err, errSimulatedCrash) {
			return err
		}
		crashes++
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(restartDelay):
			app.Log.Info("restarting after the crash", "crashes", crashes)
		}
	}
}

// run is one lifetime of the process. A simulated crash ends it with
// errSimulatedCrash and the caller starts a new one, like a pod restart.
func (c *consumer) run(ctx context.Context) error {
	opts := []kgo.Opt{
		kgo.ConsumerGroup(ledgerGroup),
		kgo.ConsumeTopics(depositsTopic),
		kgo.DisableAutoCommit(),
	}
	if c.offsetsInDB {
		opts = append(opts, kgo.AdjustFetchOffsetsFn(c.loadOffsets))
	}
	cl, err := c.app.NewClient(opts...)
	if err != nil {
		return err
	}
	// Close leaves the group but commits nothing: autocommit is off and there
	// is no revoke callback. That is what makes the simulated crash honest.
	defer cl.Close()

	// Records whose DB transaction committed but whose offset Kafka doesn't know yet.
	var uncommitted []*kgo.Record
	commit := func() {
		if err := cl.CommitRecords(ctx, uncommitted...); err != nil {
			c.app.Log.Error("offset commit failed, will retry", "err", err)
			return
		}
		uncommitted = uncommitted[:0]
	}

	lastReport := time.Now()
	for n := 0; ; {
		// The timeout wakes the loop up when idle so the tail gets committed too.
		pollCtx, cancel := context.WithTimeout(ctx, time.Second)
		fetches := cl.PollFetches(pollCtx)
		cancel()
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return commitOnExit(cl, uncommitted)
		}
		for _, fe := range fetches.Errors() {
			if !errors.Is(fe.Err, context.DeadlineExceeded) {
				c.app.Log.Error("fetch error", "topic", fe.Topic, "partition", fe.Partition, "err", fe.Err)
			}
		}
		if fetches.NumRecords() == 0 && len(uncommitted) > 0 {
			commit()
		}

		for r := range fetches.RecordsAll() {
			var d Deposit
			if err := json.Unmarshal(r.Value, &d); err != nil {
				return fmt.Errorf("decode p%d@%d: %w", r.Partition, r.Offset, err)
			}
			applied, err := c.handle(ctx, r, d)
			if err != nil {
				if ctx.Err() != nil {
					return commitOnExit(cl, uncommitted)
				}
				return fmt.Errorf("apply p%d@%d: %w", r.Partition, r.Offset, err)
			}
			n++
			c.handled++
			if applied {
				c.applied++
			} else {
				c.skipped++
				c.app.Log.Info("duplicate skipped", "event_id", d.EventID, "partition", r.Partition, "offset", r.Offset)
			}
			uncommitted = append(uncommitted, r)

			if n == c.crashAfter {
				c.app.Log.Warn("simulated crash after the DB commit, before the offset commit",
					"handled_this_run", n, "records_to_be_redelivered", len(uncommitted))
				return errSimulatedCrash
			}
			if len(uncommitted) >= c.commitEvery {
				commit()
			}
		}

		if time.Since(lastReport) >= 5*time.Second {
			c.app.Log.Info("progress", "handled", c.handled, "applied", c.applied, "skipped", c.skipped)
			lastReport = time.Now()
		}
	}
}

func commitOnExit(cl *kgo.Client, uncommitted []*kgo.Record) error {
	if len(uncommitted) == 0 {
		return nil
	}
	// ctx is already canceled; the final commit gets its own short deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return cl.CommitRecords(ctx, uncommitted...)
}

// handle applies one deposit and reports false if it was a duplicate.
func (c *consumer) handle(ctx context.Context, r *kgo.Record, d Deposit) (applied bool, err error) {
	if !c.inbox && !c.offsetsInDB {
		// Naive: right the first time, wrong on every redelivery.
		return true, addToBalance(ctx, c.pool, d)
	}
	// TODO(10.1): INBOX=true. Open ONE transaction with pgx.BeginFunc(ctx, c.pool, ...),
	// claimEvent in it and addToBalance(ctx, tx, d) only if the event is new.
	// TODO(10.2): OFFSETS_IN_DB=true. In the same transaction, saveOffset(ctx, tx, r),
	// also for a skipped duplicate. Both flags can be on at the same time.
	return false, errTODO
}

// execer is what *pgxpool.Pool and pgx.Tx have in common.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func addToBalance(ctx context.Context, db execer, d Deposit) error {
	_, err := db.Exec(ctx, `
        INSERT INTO t10_accounts (account_id, balance) VALUES ($1, $2)
        ON CONFLICT (account_id) DO UPDATE
        SET balance = t10_accounts.balance + EXCLUDED.balance, updated_at = now()`,
		d.AccountID, d.Amount)
	return err
}

// claimEvent records eventID in the inbox and reports whether it was new.
// The primary key does the dedup: two members racing on the same event (a
// zombie after a rebalance) serialize on the index instead of both winning.
func claimEvent(ctx context.Context, tx pgx.Tx, eventID string) (fresh bool, err error) {
	// TODO(10.1): INSERT the event_id into t10_inbox with ON CONFLICT DO NOTHING.
	// The event is new iff one row was inserted: tag.RowsAffected() == 1.
	return false, errTODO
}

// saveOffset stores the next offset to consume for r's partition.
func saveOffset(ctx context.Context, tx pgx.Tx, r *kgo.Record) error {
	// TODO(10.2): upsert (ledgerGroup, r.Topic, r.Partition, r.Offset+1) into
	// t10_offsets. +1 because a stored offset means "the next one to read".
	return errTODO
}

// loadOffsets runs after each assignment, once Kafka has returned the group's
// committed offsets and before fetching starts. What it returns is where
// consumption begins, so Postgres overrides Kafka. An error here fails the
// group session and the client rejoins: better than consuming from a guess.
func (c *consumer) loadOffsets(ctx context.Context, assigned map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	// TODO(10.2): for each assigned partition that has a row in t10_offsets
	// (consumer_group = ledgerGroup), replace its offset with
	// kgo.NewOffset().At(next_offset) and log both numbers. Leave the others as
	// Kafka returned them. Until then, Kafka's committed offsets win.
	return assigned, nil
}

// cleanup deletes inbox rows older than INBOX_RETENTION. The inbox is only a
// dedup window: anything redelivered after its event_id is gone gets applied again.
func cleanup(ctx context.Context, app *kit.App) error {
	retention := kit.EnvDuration("INBOX_RETENTION", 24*time.Hour)

	pool, err := app.ConnectDB(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := ensureSchema(ctx, pool); err != nil {
		return err
	}

	// TODO(10.3): DELETE the t10_inbox rows whose processed_at is older than
	// retention and log how many went. Hint: make_interval(secs => $1).
	_ = retention
	return errTODO
}
