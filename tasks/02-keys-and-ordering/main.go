// Task 02: keys and ordering. Read README.md first; the TODOs follow its "Your task" steps.
package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"maps"
	mrand "math/rand/v2"
	"os"
	"slices"
	"strconv"
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
	ordersTopic = "t02.orders"
	groupName   = "t02.fulfillment"
)

var errTODO = errors.New("not implemented yet, see README.md")

func main() {
	kit.Main(map[string]kit.Role{
		"producer":     produce,
		"consumer":     consume,
		"inspect":      inspect,
		"partition-of": partitionOf,
	})
}

// Event is one step of an order's lifecycle. Seq is the position in states,
// starting at 1, so the consumer can tell "paid after shipped" from "paid".
type Event struct {
	OrderID    string    `json:"order_id"`
	MerchantID string    `json:"merchant_id"`
	Seq        int       `json:"seq"`
	State      string    `json:"state"`
	At         time.Time `json:"at"`
}

var states = []string{"created", "paid", "shipped", "delivered"}

func stateName(seq int) string {
	if seq < 1 || seq > len(states) {
		return "?"
	}
	return states[seq-1]
}

// ---------------------------------------------------------------- producer

type order struct {
	id, merchant string
	next         int
}

func produce(ctx context.Context, app *kit.App) error {
	topic := kit.Env("TOPIC", ordersTopic)
	keyBy := kit.Env("KEY_BY", "order")
	rate := max(1, kit.EnvInt("RATE", 200))
	active := max(1, kit.EnvInt("ACTIVE_ORDERS", 1000))
	merchants := max(1, kit.EnvInt("MERCHANTS", 50))
	hotPct := kit.EnvInt("HOT_MERCHANT_PCT", 0)
	salt := max(1, kit.EnvInt("SALT", 8))

	if _, err := keyFor(keyBy, Event{}, salt); err != nil {
		return err
	}

	cl, err := app.NewClient(kgo.MetadataMaxAge(kit.EnvDuration("METADATA_MAX_AGE", 10*time.Second)))
	if err != nil {
		return err
	}
	defer cl.Close()

	if err := app.EnsureTopic(ctx, cl, kit.Topic{Name: topic, Partitions: int32(kit.EnvInt("PARTITIONS", 3))}); err != nil {
		return err
	}

	run := rand.Text()[:4]
	var seq atomic.Int64
	newOrder := func() *order {
		m := fmt.Sprintf("m-%03d", 1+mrand.IntN(merchants))
		if mrand.IntN(100) < hotPct {
			m = "m-000" // the one huge tenant
		}
		return &order{id: fmt.Sprintf("ord-%s-%d", run, seq.Add(1)), merchant: m, next: 1}
	}
	orders := make([]*order, active)
	for i := range orders {
		orders[i] = newOrder()
	}

	perPartition := newCounter()
	var failed atomic.Int64
	app.Log.Info("producing", "topic", topic, "key_by", keyBy, "rate", rate, "active_orders", active, "hot_merchant_pct", hotPct)

	tick := time.NewTicker(time.Second / time.Duration(rate))
	defer tick.Stop()
	stats := time.NewTicker(kit.EnvDuration("STATS_EVERY", 5*time.Second))
	defer stats.Stop()
	for {
		select {
		case <-ctx.Done():
			// Out-of-order is the lesson here, not shutdown, so a short flush is enough.
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return cl.Flush(flushCtx)
		case <-stats.C:
			app.Log.Info("produced", "per_partition", perPartition.snapshotAndReset(), "failed_total", failed.Load())
			continue
		case <-tick.C:
		}

		// Random pick: events of different orders interleave like real traffic.
		i := mrand.IntN(len(orders))
		o := orders[i]
		ev := Event{OrderID: o.id, MerchantID: o.merchant, Seq: o.next, State: stateName(o.next), At: time.Now()}
		o.next++
		if o.next > len(states) {
			orders[i] = newOrder()
		}

		key, _ := keyFor(keyBy, ev, salt)
		value, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		rec := &kgo.Record{Topic: topic, Key: key, Value: value}
		cl.Produce(context.WithoutCancel(ctx), rec, func(r *kgo.Record, err error) {
			if err != nil {
				failed.Add(1)
				app.Log.Error("produce failed", "order", ev.OrderID, "err", err)
				return
			}
			perPartition.add(r.Partition)
		})
	}
}

// keyFor decides which partition an event lands in. Everything that must stay
// in order relative to each other has to share a key.
func keyFor(keyBy string, ev Event, salt int) ([]byte, error) {
	switch keyBy {
	case "order":
		// TODO(02.2): key by order ID. Until then every event goes out with a
		// null key, which is the bug you observe first.
		return nil, nil
	case "merchant":
		return []byte(ev.MerchantID), nil
	case "merchant-salted":
		// TODO(02.4): spread one merchant over SALT keys so that every event of
		// one order still shares a key. Hint: crc32.ChecksumIEEE(order ID) % SALT.
		_ = crc32.ChecksumIEEE
		return nil, errTODO
	case "none":
		return nil, nil
	}
	return nil, fmt.Errorf("bad KEY_BY %q, want order|merchant|merchant-salted|none", keyBy)
}

// ---------------------------------------------------------------- consumer

type verdict int

const (
	todo      verdict = iota - 1 // starter only: observe is not implemented yet
	ok                           // seq == last+1
	firstSeen                    // no state for this order yet
	duplicate                    // seq == last: a redelivery, not an ordering problem
	late                         // seq < last: an older state arrived after a newer one
	gap                          // seq > last+1: a predecessor has not arrived (yet)
)

var verdictNames = [...]string{"ok", "first_seen", "duplicate", "late", "gap"}

func (v verdict) String() string { return verdictNames[v] }

type orderState struct {
	seq       int
	partition int32
	seen      time.Time
}

// tracker is the per-instance state machine check. Each consumer instance only
// sees its own partitions, exactly like a real service keeping per-order state.
type tracker struct {
	mu     sync.Mutex
	orders map[string]orderState
}

func newTracker() *tracker { return &tracker{orders: make(map[string]orderState)} }

// observe records ev and classifies it against what this instance saw before
// for the same order. It returns the previous state for logging.
func (t *tracker) observe(ev Event, partition int32, now time.Time) (verdict, orderState) {
	t.mu.Lock()
	defer t.mu.Unlock()

	prev, seen := t.orders[ev.OrderID]
	// TODO(02.1): classify ev.Seq against prev.seq and store the new orderState
	// (README step 1). Which seq do you store after a late event?
	_, _, _ = seen, partition, now
	return todo, prev
}

// forgetPartitions drops state for orders last seen on partitions this
// instance just got (back): it may have missed records in between.
func (t *tracker) forgetPartitions(partitions []int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, st := range t.orders {
		if slices.Contains(partitions, st.partition) {
			delete(t.orders, id)
		}
	}
}

// evict bounds memory. Finished orders are kept for a while on purpose: a late
// "created" for an order we already saw delivered is exactly what we hunt for.
func (t *tracker) evict(olderThan time.Time) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for id, st := range t.orders {
		if st.seen.Before(olderThan) {
			delete(t.orders, id)
			n++
		}
	}
	return n
}

func consume(ctx context.Context, app *kit.App) error {
	topic := kit.Env("TOPIC", ordersTopic)
	group := kit.Env("GROUP", groupName)
	processTime := kit.EnvDuration("PROCESS_TIME", 0)
	logLimit := int64(kit.EnvInt("LOG_VIOLATIONS", 10))

	tr := newTracker()
	cl, err := app.NewClient(
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.MetadataMaxAge(kit.EnvDuration("METADATA_MAX_AGE", 10*time.Second)),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			app.Log.Info("assigned", "partitions", assigned[topic])
			tr.forgetPartitions(assigned[topic])
		}),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	var (
		counts       [len(verdictNames)]atomic.Int64 // since the last stats line
		violations   atomic.Int64                    // since start
		logged       atomic.Int64
		perPartition = newCounter()
		wg           sync.WaitGroup
	)
	ctx, stop := context.WithCancel(ctx)
	defer wg.Wait()
	defer stop()
	wg.Go(func() {
		tick := time.NewTicker(kit.EnvDuration("STATS_EVERY", 5*time.Second))
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			evicted := tr.evict(time.Now().Add(-2 * time.Minute))
			app.Log.Info("stats",
				"ok", counts[ok].Swap(0), "first_seen", counts[firstSeen].Swap(0), "duplicate", counts[duplicate].Swap(0),
				"late", counts[late].Swap(0), "gap", counts[gap].Swap(0), "violations_total", violations.Load(),
				"per_partition", perPartition.snapshotAndReset(), "evicted", evicted)
			logged.Store(0)
		}
	})

	app.Log.Info("consuming", "topic", topic, "group", group)
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		for iter := fetches.RecordIter(); !iter.Done(); {
			r := iter.Next()
			perPartition.add(r.Partition)
			if processTime > 0 {
				time.Sleep(processTime)
			}
			var ev Event
			if err := json.Unmarshal(r.Value, &ev); err != nil {
				app.Log.Error("bad record", "partition", r.Partition, "offset", r.Offset, "err", err)
				continue
			}
			v, prev := tr.observe(ev, r.Partition, time.Now())
			if v == todo {
				return errTODO
			}
			counts[v].Add(1)
			if v != late && v != gap {
				continue
			}
			violations.Add(1)
			if logged.Add(1) <= logLimit {
				app.Log.Warn("ordering violation",
					"kind", v.String(), "order", ev.OrderID,
					"got", fmt.Sprintf("%s(%d)", ev.State, ev.Seq),
					"after", fmt.Sprintf("%s(%d)", stateName(prev.seq), prev.seq),
					"partition", r.Partition, "prev_partition", prev.partition, "offset", r.Offset)
			}
		}
	}
}

// counter counts records per partition between stats lines.
type counter struct {
	mu sync.Mutex
	m  map[int32]int
}

func newCounter() *counter { return &counter{m: make(map[int32]int)} }

func (c *counter) add(p int32) {
	c.mu.Lock()
	c.m[p]++
	c.mu.Unlock()
}

func (c *counter) snapshotAndReset() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var b strings.Builder
	for _, p := range slices.Sorted(maps.Keys(c.m)) {
		fmt.Fprintf(&b, "p%d=%d ", p, c.m[p])
	}
	clear(c.m)
	return strings.TrimSpace(b.String())
}

// ---------------------------------------------------------------- partition-of

// murmur2 is Kafka's Utils.murmur2. Every Java-compatible client must return
// the same 32 bits, or the same key lands in different partitions.
func murmur2(data []byte) uint32 {
	// TODO(02.3): port Kafka's Utils.murmur2 (README step 3). Constants:
	// seed 0x9747b28c, m 0x5bd1e995, r 24. binary.LittleEndian.Uint32 reads a block.
	_ = binary.LittleEndian
	return 0
}

// partitionFor mirrors Java's Utils.toPositive(murmur2(key)) % numPartitions:
// the sign bit is masked, not negated.
func partitionFor(key []byte, n int) int {
	return int(murmur2(key)&0x7fffffff) % n
}

func partitionOf(ctx context.Context, app *kit.App) error {
	keys := strings.Split(kit.Env("KEYS", "ord-1,ord-2,ord-3,ord-4,ord-5,ord-6,ord-7,ord-8,m-000,m-001"), ",")
	counts, err := parseInts(kit.Env("COUNTS", "3,6"))
	if err != nil {
		return err
	}

	// franz-go's own keyed hashing, used as the reference. The default
	// partitioner hashes keys the same way, but only exposes PartitionByBackup.
	ref := kgo.StickyKeyPartitioner(nil).ForTopic("")

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprint(w, "KEY\tMURMUR2")
	for _, n := range counts {
		fmt.Fprintf(w, "\tP@%d", n)
	}
	fmt.Fprint(w, "\tMATCHES FRANZ-GO\n")
	var mismatches int
	for _, k := range keys {
		key := []byte(k)
		fmt.Fprintf(w, "%s\t%08x", k, murmur2(key))
		match := true
		for _, n := range counts {
			p := partitionFor(key, n)
			fmt.Fprintf(w, "\t%d", p)
			match = match && p == ref.Partition(&kgo.Record{Key: key}, n)
		}
		if !match {
			mismatches++
		}
		fmt.Fprintf(w, "\t%v\n", match)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if mismatches > 0 {
		return fmt.Errorf("%d keys hash differently from franz-go: check murmur2 (TODO 02.3)", mismatches)
	}

	sample := kit.EnvInt("SAMPLE", 100_000)
	for i := 1; i < len(counts); i++ {
		from, to := counts[i-1], counts[i]
		moved := 0
		for j := range sample {
			key := fmt.Appendf(nil, "ord-%d", j)
			if partitionFor(key, from) != partitionFor(key, to) {
				moved++
			}
		}
		fmt.Printf("%d -> %d partitions: %.1f%% of %d keys change partition\n",
			from, to, 100*float64(moved)/float64(sample), sample)
	}

	if topic := kit.Env("CHECK_TOPIC", ""); topic != "" {
		return checkTopic(ctx, app, topic, counts)
	}
	return nil
}

// checkTopic reads a topic to its current end and finds, per keyed record, the
// partition count murmur2 needs to explain it: records never move when a topic grows.
func checkTopic(ctx context.Context, app *kit.App, topic string, history []int) error {
	adm, err := app.NewClient()
	if err != nil {
		return err
	}
	defer adm.Close()
	details, err := kadm.NewClient(adm).ListTopics(ctx, topic)
	if err != nil {
		return err
	}
	if err := details.Error(); err != nil {
		return err
	}
	n := len(details[topic].Partitions)
	// Newest first: a key that did not move is explained by the current count.
	candidates := slices.Compact(slices.Sorted(slices.Values(slices.Concat(history, []int{n}))))
	slices.Reverse(candidates)
	ends, err := kadm.NewClient(adm).ListEndOffsets(ctx, topic)
	if err != nil {
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

	explainedBy := map[int]int{} // partition count -> records it explains
	var nullKeys, unexplained int
	for len(remaining) > 0 {
		pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		fetches := cl.PollFetches(pollCtx)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(fetches.Err0(), context.DeadlineExceeded) {
			fmt.Printf("no more records after 10s, stopping (partitions not read to the end: %v)\n", slices.Sorted(maps.Keys(remaining)))
			break
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
			if r.Key == nil {
				nullKeys++
				return
			}
			for _, m := range candidates {
				if m > int(r.Partition) && partitionFor(r.Key, m) == int(r.Partition) {
					explainedBy[m]++
					return
				}
			}
			unexplained++
		})
	}

	fmt.Printf("\n%s has %d partitions now. Records by the partition count that explains them, newest first:\n", topic, n)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, m := range candidates {
		fmt.Fprintf(w, "  murmur2 %% %d\t%d records\n", m, explainedBy[m])
	}
	fmt.Fprintf(w, "  none of %v\t%d records\n", candidates, unexplained)
	fmt.Fprintf(w, "  null key\t%d records\n", nullKeys)
	return w.Flush()
}

func parseInts(s string) ([]int, error) {
	var out []int
	for f := range strings.SplitSeq(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n < 1 {
			return nil, fmt.Errorf("bad partition count %q", f)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, errors.New("COUNTS is empty")
	}
	return out, nil
}

// ---------------------------------------------------------------- inspect

// inspect shows how evenly the topic is loaded: in total and over the last
// WINDOW, which is what a hot key shows up in first.
func inspect(ctx context.Context, app *kit.App) error {
	topic := kit.Env("TOPIC", ordersTopic)
	window := kit.EnvDuration("WINDOW", time.Minute)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	details, err := adm.ListTopics(ctx, topic)
	if err != nil {
		return err
	}
	if err := details.Error(); err != nil {
		return err
	}
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return err
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return err
	}
	since, err := adm.ListOffsetsAfterMilli(ctx, time.Now().Add(-window).UnixMilli(), topic)
	if err != nil {
		return err
	}

	partitions := slices.Sorted(maps.Keys(details[topic].Partitions))
	recent := make(map[int32]int64, len(partitions))
	var total, peak int64
	for _, p := range partitions {
		e, _ := ends.Lookup(topic, p)
		s, _ := since.Lookup(topic, p)
		recent[p] = e.Offset - s.Offset
		total += recent[p]
		peak = max(peak, recent[p])
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "PARTITION\tLEADER\tRECORDS\tLAST %s\tSHARE\t\n", window)
	for _, p := range partitions {
		s, _ := starts.Lookup(topic, p)
		e, _ := ends.Lookup(topic, p)
		share := 0.0
		if total > 0 {
			share = float64(recent[p]) / float64(total)
		}
		fmt.Fprintf(w, "%d\t%d\t%d\t%d\t%5.1f%%\t%s\n", p, details[topic].Partitions[p].Leader,
			e.Offset-s.Offset, recent[p], 100*share, strings.Repeat("#", int(share*50)))
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if total > 0 {
		mean := float64(total) / float64(len(partitions))
		fmt.Printf("skew (hottest / mean) over the last %s: %.2f\n", window, float64(peak)/mean)
	}
	return nil
}
