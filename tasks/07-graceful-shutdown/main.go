// Task 07: graceful shutdown and deploys. Read README.md first; the TODOs
// follow its "Your task" steps. The worker runs as is, with the naive
// shutdown most services ship with; the experiments measure what it costs.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"idiomaticGo/kafka/internal/kit"
)

const (
	jobsTopic    = "t07.jobs"
	resultsTopic = "t07.results"
	defaultGroup = "t07.workers"
)

var errTODO = errors.New("not implemented yet, see README.md")

func main() {
	kit.Main(map[string]kit.Role{
		"producer": produce,
		"consumer": work,
		"audit":    audit,
		"evict":    evict,
	})
}

type Job struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
}

// Result is what a worker writes for every job it finishes. The audit role
// joins results back to jobs to count duplicates, losses and stalls.
type Result struct {
	JobID       string    `json:"job_id"`
	Partition   int32     `json:"partition"`
	Offset      int64     `json:"offset"`
	Worker      string    `json:"worker"`
	Version     string    `json:"version"`
	CreatedAt   time.Time `json:"created_at"`
	ProcessedAt time.Time `json:"processed_at"`
}

func ensureTopics(ctx context.Context, app *kit.App, cl *kgo.Client) error {
	// 6 partitions: two per worker-a/b/c, so a leaving worker hands off
	// partitions and you can see who picks them up.
	if err := app.EnsureTopic(ctx, cl, kit.Topic{Name: jobsTopic, Partitions: 6}); err != nil {
		return err
	}
	return app.EnsureTopic(ctx, cl, kit.Topic{Name: resultsTopic, Partitions: 3})
}

func produce(ctx context.Context, app *kit.App) error {
	rate := max(kit.EnvInt("RATE", 10), 1)

	cl, err := app.NewClient(kgo.DefaultProduceTopic(jobsTopic))
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := ensureTopics(ctx, app, cl); err != nil {
		return err
	}

	run := strings.ToLower(rand.Text()[:4])
	var failed atomic.Int64
	// Records must outlive SIGTERM: a record produced with a cancelled
	// context can be failed while it still sits in the buffer.
	produceCtx := context.WithoutCancel(ctx)

	tick := time.NewTicker(time.Second / time.Duration(rate))
	defer tick.Stop()
	app.Log.Info("producing jobs", "topic", jobsTopic, "rate_per_sec", rate, "run", run)
	for n := 0; ; n++ {
		select {
		case <-ctx.Done():
			return flush(produceCtx, app, cl, n, failed.Load())
		case <-tick.C:
		}

		job := Job{ID: fmt.Sprintf("%s-%06d", run, n), CreatedAt: time.Now()}
		value, err := json.Marshal(job)
		if err != nil {
			return err
		}
		cl.Produce(produceCtx, &kgo.Record{Key: []byte(job.ID), Value: value}, func(r *kgo.Record, err error) {
			if err != nil {
				failed.Add(1)
				app.Log.Error("produce failed", "job", string(r.Key), "err", err)
			}
		})
		if n%(rate*10) == 0 {
			app.Log.Info("produced", "jobs", n+1)
		}
	}
}

// flush waits for buffered records before Close, which would fail them.
func flush(ctx context.Context, app *kit.App, cl *kgo.Client, produced int, failed int64) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	start := time.Now()
	if err := cl.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	app.Log.Info("producer flushed", "jobs", produced, "failed", failed, "took", time.Since(start).Round(time.Millisecond))
	return nil
}

type workerConfig struct {
	group           string
	protocol        string
	balancer        string
	static          bool
	sessionTimeout  time.Duration
	processTime     time.Duration
	maxPoll         int
	shutdownTimeout time.Duration
}

func loadWorkerConfig() workerConfig {
	return workerConfig{
		group:           kit.Env("GROUP", defaultGroup),
		protocol:        kit.Env("GROUP_PROTOCOL", "classic"),
		balancer:        kit.Env("BALANCER", "cooperative-sticky"),
		static:          kit.EnvBool("STATIC_MEMBERSHIP", false),
		sessionTimeout:  kit.EnvDuration("SESSION_TIMEOUT", 45*time.Second),
		processTime:     kit.EnvDuration("PROCESS_TIME", 100*time.Millisecond),
		maxPoll:         max(kit.EnvInt("MAX_POLL", 10), 1),
		shutdownTimeout: kit.EnvDuration("SHUTDOWN_TIMEOUT", 20*time.Second),
	}
}

func groupOpts(app *kit.App, wc workerConfig) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.ConsumerGroup(wc.group),
		kgo.ConsumeTopics(jobsTopic),
		kgo.DefaultProduceTopic(resultsTopic),
		// Ignored by KIP-848 groups: there the broker owns the session timeout.
		kgo.SessionTimeout(wc.sessionTimeout),
		kgo.OnPartitionsAssigned(logPartitions(app, "partitions assigned")),
		kgo.OnPartitionsLost(logPartitions(app, "partitions lost")),
		// No OnPartitionsRevoked yet: franz-go's default one is what commits
		// on revoke under autocommit, and setting your own replaces it.
		// TODO(07.3): AutoCommitMarks, BlockRebalanceOnPoll and an
		// OnPartitionsRevoked that commits the marked offsets.
	}

	switch wc.balancer {
	case "cooperative-sticky": // franz-go's default
	case "range":
		opts = append(opts, kgo.Balancers(kgo.RangeBalancer()))
	default:
		return nil, fmt.Errorf("BALANCER %q: want cooperative-sticky|range", wc.balancer)
	}

	switch wc.protocol {
	case "classic":
	case "consumer":
		opts = append(opts, kgo.ServerSideBalancer())
	default:
		return nil, fmt.Errorf("GROUP_PROTOCOL %q: want classic|consumer", wc.protocol)
	}

	if wc.static {
		// TODO(07.4): make this member static with kgo.InstanceID. Which
		// value survives a restart of the same replica? (README)
		return nil, errTODO
	}
	return opts, nil
}

func logPartitions(app *kit.App, msg string) func(context.Context, *kgo.Client, map[string][]int32) {
	return func(_ context.Context, _ *kgo.Client, ps map[string][]int32) {
		if len(ps[jobsTopic]) > 0 {
			app.Log.Info(msg, "partitions", ps[jobsTopic])
		}
	}
}

type worker struct {
	app *kit.App
	cl  *kgo.Client
	cfg workerConfig
}

func work(ctx context.Context, app *kit.App) error {
	wc := loadWorkerConfig()
	opts, err := groupOpts(app, wc)
	if err != nil {
		return err
	}
	cl, err := app.NewClient(opts...)
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := ensureTopics(ctx, app, cl); err != nil {
		return err
	}
	w := &worker{app: app, cl: cl, cfg: wc}

	app.Log.Info("worker started", "group", wc.group, "protocol", wc.protocol, "balancer", wc.balancer,
		"static", wc.static, "session_timeout", wc.sessionTimeout, "process_time", wc.processTime)

	for {
		fetches := cl.PollRecords(ctx, wc.maxPoll)
		if ctx.Err() != nil {
			// TODO(07.1): the naive shutdown: the process just ends, nothing is
			// committed and the group is never told. Leave the loop instead,
			// so the deferred Close runs and leaves the group.
			app.Log.Warn("exiting now: no drain, no commit, no LeaveGroup")
			os.Exit(0)
		}
		fetches.EachError(func(t string, p int32, err error) {
			app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		// TODO(07.2): ctx dies with SIGTERM and takes the in-flight batch with
		// it. Process with a context that survives the signal for up to
		// SHUTDOWN_TIMEOUT.
		w.handle(ctx, fetches.Records())
	}
}

func (w *worker) handle(ctx context.Context, recs []*kgo.Record) {
	for i, r := range recs {
		if err := w.process(ctx, r); err != nil {
			w.app.Log.Warn("batch cut short", "err", err, "done", i, "abandoned", len(recs)-i)
			return
		}
		// TODO(07.3): mark r as processed, so only finished jobs get committed.
	}
}

func (w *worker) process(ctx context.Context, r *kgo.Record) error {
	var job Job
	if err := json.Unmarshal(r.Value, &job); err != nil {
		w.app.Log.Error("skipping undecodable job", "partition", r.Partition, "offset", r.Offset, "err", err)
		return nil
	}

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-time.After(w.cfg.processTime):
	}

	value, err := json.Marshal(Result{
		JobID:       job.ID,
		Partition:   r.Partition,
		Offset:      r.Offset,
		Worker:      w.app.Instance,
		Version:     w.app.Version,
		CreatedAt:   job.CreatedAt,
		ProcessedAt: time.Now(),
	})
	if err != nil {
		return err
	}
	// The result must be acked before the job's offset is committed;
	// the other order loses the result if we die in between.
	if err := w.cl.ProduceSync(ctx, &kgo.Record{Key: r.Key, Value: value}).FirstErr(); err != nil {
		return fmt.Errorf("produce result: %w", err)
	}
	return nil
}

// evict removes static members by instance ID, the admin-side LeaveGroup a
// static member never sends itself. Use it when a member is gone for good
// (scale-down, replaced host) instead of waiting out its session timeout.
func evict(ctx context.Context, app *kit.App) error {
	group := kit.Env("GROUP", defaultGroup)
	ids := strings.Split(kit.Env("INSTANCES", ""), ",")

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()

	resps, err := kadm.NewClient(cl).LeaveGroup(ctx, kadm.LeaveGroup(group).InstanceIDs(ids...).Reason("evicted by operator"))
	if err != nil {
		return fmt.Errorf("evict from %s: %w", group, err)
	}
	if len(resps) == 0 {
		return errors.New("set INSTANCES=worker-a,worker-b")
	}
	for _, r := range resps.Sorted() {
		if r.Err != nil {
			app.Log.Warn("not evicted", "group", group, "instance", r.InstanceID, "err", r.Err)
			continue
		}
		app.Log.Info("evicted", "group", group, "instance", r.InstanceID)
	}
	return nil
}
