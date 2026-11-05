// Task 15: compaction and state. Read README.md first; the TODOs follow its "Your task" steps.
package main

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"net"
	"net/http"
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
	customersTopic = "t15.customers"
	eventsTopic    = "t15.customer-events"
)

var errTODO = errors.New("not implemented yet, see README.md")

func main() {
	kit.Main(map[string]kit.Role{
		"producer": produce,
		"api":      serve,
		"inspect":  inspect,
		"rebuild":  rebuild,
	})
}

// Customer is the latest state of one customer: the value in t15.customers.
type Customer struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	Tier      string    `json:"tier"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Event is one change: the value in t15.customer-events.
type Event struct {
	ID         string    `json:"id"`
	Type       string    `json:"type"`
	CustomerID string    `json:"customer_id"`
	Name       string    `json:"name,omitempty"`
	Email      string    `json:"email,omitempty"`
	Tier       string    `json:"tier,omitempty"`
	At         time.Time `json:"at"`
}

const (
	evCreated      = "customer.created"
	evEmailChanged = "customer.email_changed"
	evTierChanged  = "customer.tier_changed"
	evDeleted      = "customer.deleted"
)

func ensureTopics(ctx context.Context, app *kit.App, cl *kgo.Client) error {
	configs, err := customersTopicConfigs()
	if err != nil {
		return fmt.Errorf("%s configs: %w", customersTopic, err)
	}
	if err := app.EnsureTopic(ctx, cl, kit.Topic{Name: customersTopic, Partitions: 3, Configs: configs}); err != nil {
		return err
	}
	// The event log is the source of truth, so it never expires.
	return app.EnsureTopic(ctx, cl, kit.Topic{
		Name:       eventsTopic,
		Partitions: 3,
		Configs:    map[string]string{"retention.ms": "-1"},
	})
}

// customersTopicConfigs makes t15.customers a table: eventually one record per key.
func customersTopicConfigs() (map[string]string, error) {
	// TODO(15.1): return the five topic configs from README step 1: a
	// compacted topic that compacts within seconds on this cluster.
	return nil, errTODO
}

// ---------------------------------------------------------------- producer

var (
	names = []string{"ada", "alan", "barbara", "dennis", "edsger", "grace", "ken", "linus", "margaret", "rob"}
	tiers = []string{"free", "pro", "enterprise"}
)

// produce changes random customers. Every change goes to the event log and
// the new latest state (or a tombstone) to the compacted topic.
func produce(ctx context.Context, app *kit.App) error {
	numCustomers := kit.EnvInt("CUSTOMERS", 200)
	interval := kit.EnvDuration("INTERVAL", 20*time.Millisecond)
	deletePct := kit.EnvInt("DELETE_PCT", 3)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := ensureTopics(ctx, app, cl); err != nil {
		return err
	}

	live := make(map[string]Customer)
	var upserts, tombstones int
	tick := time.NewTicker(interval)
	defer tick.Stop()
	report := time.NewTicker(5 * time.Second)
	defer report.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-report.C:
			app.Log.Info("produced", "upserts", upserts, "tombstones", tombstones, "live_customers", len(live))
			continue
		case <-tick.C:
		}

		id := fmt.Sprintf("cust-%03d", rand.IntN(numCustomers))
		ev, c := change(live, id, deletePct)
		evValue, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		event := &kgo.Record{
			Topic:   eventsTopic,
			Key:     []byte(id),
			Value:   evValue,
			Headers: []kgo.RecordHeader{{Key: "event-type", Value: []byte(ev.Type)}},
		}
		state := &kgo.Record{Topic: customersTopic, Key: []byte(id)}
		if ev.Type == evDeleted {
			// TODO(15.2): this "deletes" with an empty value. Make it a real
			// tombstone (README step 2).
			state.Value = []byte{}
		} else if state.Value, err = json.Marshal(c); err != nil {
			return err
		}

		if err := cl.ProduceSync(ctx, event, state).FirstErr(); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			app.Log.Error("produce failed", "customer", id, "err", err)
			continue
		}
		if state.Value == nil {
			tombstones++
		} else {
			upserts++
		}
	}
}

// change applies one random change to live and returns the event and the new state.
func change(live map[string]Customer, id string, deletePct int) (Event, Customer) {
	now := time.Now()
	ev := Event{ID: crand.Text(), CustomerID: id, At: now}
	c, exists := live[id]
	switch {
	case !exists:
		name := names[rand.IntN(len(names))]
		c = Customer{ID: id, Name: name, Email: fmt.Sprintf("%s.%s@example.com", name, id), Tier: "free"}
		ev.Type, ev.Name, ev.Email, ev.Tier = evCreated, c.Name, c.Email, c.Tier
	case rand.IntN(100) < deletePct:
		delete(live, id)
		ev.Type = evDeleted
		return ev, Customer{}
	case rand.IntN(2) == 0:
		c.Email = fmt.Sprintf("%s.%d@example.com", c.Name, rand.IntN(1000))
		ev.Type, ev.Email = evEmailChanged, c.Email
	default:
		c.Tier = tiers[rand.IntN(len(tiers))]
		ev.Type, ev.Tier = evTierChanged, c.Tier
	}
	c.UpdatedAt = now
	live[id] = c
	return ev, c
}

// ---------------------------------------------------------------- catch-up

// catchUp answers "have I read everything that existed when I started?".
// The finish line is the end offset captured before reading, so records
// produced meanwhile don't push it forward and a busy topic still finishes.
type catchUp struct {
	target map[int32]int64 // end offset (high watermark) captured at startup
	next   map[int32]int64 // last offset seen + 1; starts at the log start offset
}

func newCatchUp(topic string, starts, ends kadm.ListedOffsets) (*catchUp, error) {
	// TODO(15.4): for every partition of topic in ends, set target to its end
	// offset and next to its start offset. Fail if ends.Error() is not nil.
	return nil, errTODO
}

func (c *catchUp) observe(r *kgo.Record) {
	// TODO(15.4): remember that r's partition has been read up to r.Offset.
}

func (c *catchUp) done() bool {
	// TODO(15.4): true once next reached target on every partition.
	return false
}

// remaining is an upper bound: compaction leaves offset gaps, so fewer
// records than this are actually left to read.
func (c *catchUp) remaining() int64 {
	var n int64
	for p, t := range c.target {
		n += max(0, t-c.next[p])
	}
	return n
}

// startFromBeginning captures the finish line, then starts reading every
// partition of topic from its log start offset.
func startFromBeginning(ctx context.Context, cl *kgo.Client, topic string) (*catchUp, error) {
	starts, ends, err := listOffsets(ctx, cl, topic)
	if err != nil {
		return nil, err
	}
	c, err := newCatchUp(topic, starts, ends)
	if err != nil {
		return nil, err
	}
	assignFrom(cl, topic, starts)
	return c, nil
}

func listOffsets(ctx context.Context, cl *kgo.Client, topic string) (starts, ends kadm.ListedOffsets, err error) {
	adm := kadm.NewClient(cl)
	if starts, err = adm.ListStartOffsets(ctx, topic); err != nil {
		return nil, nil, err
	}
	if ends, err = adm.ListEndOffsets(ctx, topic); err != nil {
		return nil, nil, err
	}
	return starts, ends, nil
}

// assignFrom consumes every partition of topic from the given offsets: no
// group, explicit offsets. A service with a local snapshot would pass the
// offsets it checkpointed together with that snapshot.
func assignFrom(cl *kgo.Client, topic string, from kadm.ListedOffsets) {
	assign := make(map[int32]kgo.Offset)
	from.Each(func(o kadm.ListedOffset) {
		if o.Topic == topic {
			assign[o.Partition] = kgo.NewOffset().At(o.Offset)
		}
	})
	cl.AddConsumePartitions(map[string]map[int32]kgo.Offset{topic: assign})
}

// ---------------------------------------------------------------- api

type row struct {
	Customer
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

// view is the in-memory table built from the compacted topic.
type view struct {
	mu        sync.RWMutex
	rows      map[string]row
	boot      *catchUp
	bootStart time.Time
	bootTook  time.Duration
	applied   int
	ready     atomic.Bool
}

// apply upserts or deletes one row. It reports false for a value it can't decode.
func (v *view) apply(r *kgo.Record) bool {
	id := string(r.Key)
	// TODO(15.3): a tombstone (nil Value) means the customer was deleted.
	var c Customer
	if err := json.Unmarshal(r.Value, &c); err != nil {
		return false
	}
	v.rows[id] = row{Customer: c, Partition: r.Partition, Offset: r.Offset}
	return true
}

func serve(ctx context.Context, app *kit.App) error {
	addr := kit.Env("HTTP_ADDR", ":8080")
	applyDelay := kit.EnvDuration("APPLY_DELAY", 0)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := ensureTopics(ctx, app, cl); err != nil {
		return err
	}

	v := &view{rows: make(map[string]row), bootStart: time.Now()}
	starts, _, err := listOffsets(ctx, cl, customersTopic)
	if err != nil {
		return err
	}
	assignFrom(cl, customersTopic, starts)
	// TODO(15.5): use startFromBeginning instead, keep its catchUp in v.boot
	// and set v.ready only once v.boot.done() (README step 5).
	v.ready.Store(true) // naive: "the process is up, so it is ready"

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	srv := &http.Server{Handler: v.routes(), ReadHeaderTimeout: 5 * time.Second}
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			app.Log.Error("http server", "err", err)
		}
	})
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
		wg.Wait()
	}()
	app.Log.Info("http listening", "addr", addr)

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			if applyDelay > 0 && !v.ready.Load() {
				time.Sleep(applyDelay)
			}
			v.mu.Lock()
			ok := v.apply(r)
			v.applied++
			v.mu.Unlock()
			if !ok {
				app.Log.Error("bad value, skipped", "key", string(r.Key), "partition", r.Partition, "offset", r.Offset)
			}
		})
	}
}

func (v *view) routes() http.Handler {
	mux := http.NewServeMux()
	// Liveness never depends on Kafka: restarting a pod that is busy
	// bootstrapping only makes it start over.
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"alive": true})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		v.mu.RLock()
		defer v.mu.RUnlock()
		if !v.ready.Load() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"ready": false, "applied": v.applied, "offsets_left": v.boot.remaining(), "target": v.boot.target,
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ready": true, "customers": len(v.rows), "bootstrap_ms": v.bootTook.Milliseconds(),
		})
	})
	mux.HandleFunc("GET /customers/{id}", func(w http.ResponseWriter, r *http.Request) {
		if !v.ready.Load() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "bootstrapping"})
			return
		}
		v.mu.RLock()
		c, ok := v.rows[r.PathValue("id")]
		v.mu.RUnlock()
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
			return
		}
		writeJSON(w, http.StatusOK, c)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// ---------------------------------------------------------------- inspect

type partStats struct {
	records, tombstones int
	last                map[string]bool // key -> last value was a tombstone
}

// inspect reads the topic from the start to its current end and compares
// what is physically there with how many distinct keys it holds.
func inspect(ctx context.Context, app *kit.App) error {
	topic := kit.Env("TOPIC", customersTopic)
	watch := kit.EnvDuration("WATCH", 0)

	for {
		if err := inspectOnce(ctx, app, topic); err != nil {
			return err
		}
		if watch <= 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(watch):
		}
	}
}

func inspectOnce(ctx context.Context, app *kit.App, topic string) error {
	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	c, err := startFromBeginning(ctx, cl, topic)
	if err != nil {
		return err
	}
	starts := maps.Clone(c.next)
	var parts kadm.TopicsSet
	parts.Add(topic, slices.Collect(maps.Keys(c.target))...)
	sizes, err := adm.DescribeAllLogDirs(ctx, parts)
	if err != nil {
		return fmt.Errorf("describe log dirs: %w", err)
	}

	stats := make(map[int32]*partStats)
	for p := range c.target {
		stats[p] = &partStats{last: make(map[string]bool)}
	}
	for !c.done() {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err := fetches.Err(); err != nil {
			return err
		}
		fetches.EachRecord(func(r *kgo.Record) {
			c.observe(r)
			if r.Offset >= c.target[r.Partition] {
				return // produced after we started; not part of this snapshot
			}
			s := stats[r.Partition]
			s.records++
			if r.Value == nil {
				s.tombstones++
			}
			s.last[string(r.Key)] = r.Value == nil
		})
	}

	brokers := slices.Sorted(maps.Keys(sizes))
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "%s  %s\n", time.Now().Format("15:04:05"), topic)
	fmt.Fprintf(w, "PARTITION\tSTART\tEND\tOFFSETS\tRECORDS\tTOMBSTONES\tKEYS\tLIVE KEYS\tBYTES ON DISK (broker %v)\n", brokers)
	var tot partStats
	var totOffsets, totKeys, totLive int64
	for _, p := range slices.Sorted(maps.Keys(stats)) {
		s := stats[p]
		live := 0
		for _, tomb := range s.last {
			if !tomb {
				live++
			}
		}
		var bytes []string
		for _, b := range brokers {
			d, _ := sizes[b].LookupPartition(topic, p)
			bytes = append(bytes, fmt.Sprintf("%dKiB", d.Size/1024))
		}
		offsets := c.target[p] - starts[p]
		fmt.Fprintf(w, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%s\n",
			p, starts[p], c.target[p], offsets, s.records, s.tombstones, len(s.last), live, strings.Join(bytes, " / "))
		tot.records += s.records
		tot.tombstones += s.tombstones
		totOffsets += offsets
		totKeys += int64(len(s.last))
		totLive += int64(live)
	}
	fmt.Fprintf(w, "TOTAL\t\t\t%d\t%d\t%d\t%d\t%d\t\n", totOffsets, tot.records, tot.tombstones, totKeys, totLive)
	return w.Flush()
}

// ---------------------------------------------------------------- rebuild

// rebuild replays the whole event log into a fresh projection, the way a new
// version of a read model is built next to the old one.
func rebuild(ctx context.Context, app *kit.App) error {
	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()

	c, err := startFromBeginning(ctx, cl, eventsTopic)
	if err != nil {
		return err
	}
	app.Log.Info("replaying event log", "topic", eventsTopic, "offsets_to_read", c.remaining())

	started := time.Now()
	projection := make(map[string]Customer)
	byType := make(map[string]int)
	deletedOnce := make(map[string]bool)
	var records int
	for !c.done() {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err := fetches.Err(); err != nil {
			return err
		}
		fetches.EachRecord(func(r *kgo.Record) {
			c.observe(r)
			if r.Offset >= c.target[r.Partition] {
				return
			}
			records++
			var ev Event
			if err := json.Unmarshal(r.Value, &ev); err != nil {
				app.Log.Error("bad event, skipped", "partition", r.Partition, "offset", r.Offset, "err", err)
				return
			}
			byType[ev.Type]++
			applyEvent(projection, ev)
			if ev.Type == evDeleted {
				deletedOnce[ev.CustomerID] = true
			}
		})
	}
	took := time.Since(started)

	app.Log.Info("projection rebuilt",
		"events", records,
		"took", took.Round(time.Millisecond),
		"events_per_sec", int(float64(records)/max(took.Seconds(), 0.001)),
		"live_customers", len(projection),
		"ever_deleted", len(deletedOnce),
	)
	for _, t := range slices.Sorted(maps.Keys(byType)) {
		app.Log.Info("events by type", "type", t, "count", byType[t])
	}
	return nil
}

func applyEvent(p map[string]Customer, ev Event) {
	c := p[ev.CustomerID]
	switch ev.Type {
	case evCreated:
		c = Customer{ID: ev.CustomerID, Name: ev.Name, Email: ev.Email, Tier: ev.Tier}
	case evEmailChanged:
		c.Email = ev.Email
	case evTierChanged:
		c.Tier = ev.Tier
	case evDeleted:
		delete(p, ev.CustomerID)
		return
	}
	c.UpdatedAt = ev.At
	p[ev.CustomerID] = c
}
