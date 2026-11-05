// Reference solution for task 05. Try the task first: tasks/05-consumer-groups/README.md
package main

import (
	"cmp"
	"context"
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
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"idiomaticGo/kafka/internal/kit"
)

const (
	ordersTopic = "t05.orders"
	groupName   = "t05.billing"
	partitions  = 6
)

func main() {
	kit.Main(map[string]kit.Role{
		"producer": produce,
		"consumer": consume,
		"watch":    watch,
	})
}

type Order struct {
	Seq         int64     `json:"seq"`
	OrderID     string    `json:"order_id"`
	AmountCents int64     `json:"amount_cents"`
	At          time.Time `json:"at"`
}

func ensureTopic(ctx context.Context, app *kit.App, cl *kgo.Client) error {
	return app.EnsureTopic(ctx, cl, kit.Topic{Name: ordersTopic, Partitions: partitions})
}

// produce writes a steady stream, so a gap in what a consumer processes can
// only come from the group, never from the producer.
func produce(ctx context.Context, app *kit.App) error {
	rate := max(kit.EnvInt("RATE", 50), 1)

	cl, err := app.NewClient(kgo.DefaultProduceTopic(ordersTopic))
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

	app.Log.Info("producing", "topic", ordersTopic, "rate_per_sec", rate)
	for seq := int64(1); ; {
		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return cl.Flush(flushCtx)
		case <-report.C:
			app.Log.Info("produced", "total", produced.Load(), "failed", failed.Load())
		case <-tick.C:
			o := Order{
				Seq:         seq,
				OrderID:     fmt.Sprintf("order-%d", rand.IntN(1000)),
				AmountCents: rand.Int64N(100_00),
				At:          time.Now(),
			}
			seq++
			value, err := json.Marshal(o)
			if err != nil {
				return err
			}
			cl.Produce(ctx, &kgo.Record{Key: []byte(o.OrderID), Value: value}, func(_ *kgo.Record, err error) {
				if err != nil {
					failed.Add(1)
					if ctx.Err() == nil {
						app.Log.Error("produce failed", "err", err)
					}
					return
				}
				produced.Add(1)
			})
		}
	}
}

func consume(ctx context.Context, app *kit.App) error {
	group := kit.Env("GROUP", groupName)
	balancer := kit.Env("BALANCER", "cooperative-sticky")
	protocol := kit.Env("GROUP_PROTOCOL", "classic")
	processTime := kit.EnvDuration("PROCESS_TIME", 5*time.Millisecond)
	batchTime := kit.EnvDuration("BATCH_TIME", 0)
	maxPoll := kit.EnvInt("MAX_POLL_RECORDS", 500)
	statsEvery := kit.EnvDuration("STATS_EVERY", 5*time.Second)

	balancing, err := groupBalancing(app.Log, balancer, protocol)
	if err != nil {
		return err
	}

	t := &tracker{log: app.Log, owned: map[int32]time.Time{}, waiting: map[int32]bool{}}
	opts := append([]kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(ordersTopic),
		// One client ID per process, so `watch` and kafka-consumer-groups
		// show which replica owns which partitions.
		kgo.ClientID("t05-consumer-" + app.Instance),
		// Callbacks run only between batches (after AllowRebalance), so the
		// revoke callback can commit everything polled: it is all processed.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(t.assigned),
		kgo.OnPartitionsRevoked(t.revoked),
		kgo.OnPartitionsLost(t.lost),
		kgo.OnPartitionsCallbackBlocked(func(context.Context, *kgo.Client) {
			app.Log.Info("rebalance is waiting for the current batch to finish")
		}),
	}, balancing...)

	cl, err := app.NewClient(opts...)
	if err != nil {
		return err
	}
	// With BlockRebalanceOnPoll a plain Close hangs if a poll is unreleased.
	defer cl.CloseAllowingRebalance()
	if err := ensureTopic(ctx, app, cl); err != nil {
		return err
	}

	app.Log.Info("consuming", "group", group, "balancer", balancer, "group_protocol", protocol,
		"process_time", processTime, "batch_time", batchTime)
	go t.reportEvery(ctx, statsEvery)
	for {
		fetches := cl.PollRecords(ctx, maxPoll)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(topic string, p int32, err error) {
			app.Log.Error("fetch error", "topic", topic, "partition", p, "err", err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			time.Sleep(processTime)
			t.processed(r)
		})
		if !fetches.Empty() {
			time.Sleep(batchTime) // e.g. one bulk write per poll
		}
		cl.AllowRebalance()
	}
}

// groupBalancing maps BALANCER and GROUP_PROTOCOL to client options.
func groupBalancing(log *slog.Logger, balancer, protocol string) ([]kgo.Opt, error) {
	var b kgo.GroupBalancer
	switch balancer {
	case "cooperative-sticky":
		b = kgo.CooperativeStickyBalancer()
	case "sticky":
		b = kgo.StickyBalancer()
	case "range":
		b = kgo.RangeBalancer()
	case "roundrobin":
		b = kgo.RoundRobinBalancer()
	default:
		return nil, fmt.Errorf("unknown BALANCER %q, want cooperative-sticky|sticky|range|roundrobin", balancer)
	}
	opts := []kgo.Opt{kgo.Balancers(b)}

	switch protocol {
	case "classic":
	case "consumer":
		// franz-go maps (cooperative-)sticky to the server's "uniform" assignor and
		// range to "range"; with any other balancer it quietly stays classic.
		if balancer == "roundrobin" {
			log.Warn("GROUP_PROTOCOL=consumer has no server-side roundrobin; franz-go will silently use the classic protocol")
		}
		opts = append(opts, kgo.ServerSideBalancer())
	default:
		return nil, fmt.Errorf("unknown GROUP_PROTOCOL %q, want classic|consumer", protocol)
	}
	return opts, nil
}

// tracker logs assignment changes and how long each new partition sat unprocessed.
// Callbacks run on franz-go's goroutine, records on the poll loop's: hence the mutex.
type tracker struct {
	log *slog.Logger

	mu        sync.Mutex
	owned     map[int32]time.Time // partition -> when we got it
	waiting   map[int32]bool      // assigned, first record not processed yet
	stoppedAt time.Time           // when we last went down to zero partitions
	count     int
	maxWait   time.Duration
}

func (t *tracker) assigned(_ context.Context, cl *kgo.Client, assigned map[string][]int32) {
	added := slices.Sorted(slices.Values(assigned[ordersTopic]))
	member, gen := cl.GroupMetadata()

	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	for _, p := range added {
		t.owned[p] = now
		t.waiting[p] = true
	}
	attrs := []any{"added", added, "owned", t.ownedLocked(), "member", shortID(member), "generation", gen}
	if !t.stoppedAt.IsZero() && len(added) > 0 {
		attrs = append(attrs, "owned_nothing_for", now.Sub(t.stoppedAt).Round(time.Millisecond))
		t.stoppedAt = time.Time{}
	}
	t.log.Info("partitions assigned", attrs...)
}

func (t *tracker) revoked(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
	// Overriding OnPartitionsRevoked replaces franz-go's own commit on
	// revoke, so this commit is ours to make.
	if err := cl.CommitUncommittedOffsets(ctx); err != nil {
		t.log.Error("commit on revoke failed", "err", err)
	}
	gone := slices.Sorted(slices.Values(revoked[ordersTopic]))
	if len(gone) == 0 {
		return // cooperative: called at the end of every session, even when nothing moves
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	for _, p := range gone {
		delete(t.owned, p)
		delete(t.waiting, p)
	}
	if len(t.owned) == 0 {
		t.stoppedAt = time.Now() // eager: every rebalance ends up here
	}
	t.log.Info("partitions revoked", "revoked", gone, "still_owned", t.ownedLocked())
}

func (t *tracker) lost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	gone := slices.Sorted(slices.Values(lost[ordersTopic]))
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, p := range gone {
		delete(t.owned, p)
		delete(t.waiting, p)
	}
	if len(t.owned) == 0 {
		t.stoppedAt = time.Now()
	}
	// No commit: we are no longer a member, the broker would reject it.
	t.log.Warn("partitions lost", "lost", gone)
}

func (t *tracker) processed(r *kgo.Record) {
	wait := time.Since(r.Timestamp)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.count++
	t.maxWait = max(t.maxWait, wait)
	if t.waiting[r.Partition] {
		delete(t.waiting, r.Partition)
		// With a steady producer, the first record's age is how long the
		// partition went unprocessed while it changed hands.
		t.log.Info("partition resumed", "partition", r.Partition, "offset", r.Offset,
			"first_record_waited", wait.Round(time.Millisecond),
			"since_assigned", time.Since(t.owned[r.Partition]).Round(time.Millisecond))
	}
}

func (t *tracker) reportEvery(ctx context.Context, every time.Duration) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		t.mu.Lock()
		t.log.Info("stats", "owned", t.ownedLocked(), "processed", t.count, "max_record_wait", t.maxWait.Round(time.Millisecond))
		t.count, t.maxWait = 0, 0
		t.mu.Unlock()
	}
}

func (t *tracker) ownedLocked() []int32 {
	return slices.Sorted(maps.Keys(t.owned))
}

// watch is a mini kafka-consumer-groups --describe in a loop.
func watch(ctx context.Context, app *kit.App) error {
	group := kit.Env("GROUP", groupName)
	topic := kit.Env("TOPIC", ordersTopic)
	every := kit.EnvDuration("EVERY", 2*time.Second)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	for {
		if err := printGroup(ctx, adm, group, topic); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			app.Log.Error("describe failed", "err", err)
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

type groupView struct {
	Protocol string // classic | consumer (KIP-848)
	State    string
	Assignor string
	Epoch    int32 // group epoch, KIP-848 only
	Members  []memberView
}

type memberView struct {
	ID       string
	ClientID string
	Host     string
	Epoch    int32   // KIP-848 only
	Assigned []int32 // what the member owns now
	Target   []int32 // KIP-848: what the coordinator wants it to own
}

// describeGroup asks the KIP-848 API first: a classic group answers it with
// GROUP_ID_NOT_FOUND, and then the classic DescribeGroups API is used.
func describeGroup(ctx context.Context, adm *kadm.Client, group, topic string) (groupView, error) {
	cgs, err := adm.DescribeConsumerGroups(ctx, group)
	if err != nil {
		return groupView{}, err
	}
	if cg, ok := cgs[group]; ok && cg.Err == nil {
		v := groupView{Protocol: "consumer", State: cg.State, Assignor: cg.AssignorName, Epoch: cg.Epoch}
		for _, m := range cg.Members {
			v.Members = append(v.Members, memberView{
				ID:       m.MemberID,
				ClientID: m.ClientID,
				Host:     m.ClientHost,
				Epoch:    m.MemberEpoch,
				Assigned: slices.Sorted(maps.Keys(m.Assignment[topic])),
				Target:   slices.Sorted(maps.Keys(m.TargetAssignment[topic])),
			})
		}
		return v, nil
	} else if ok && !errors.Is(cg.Err, kerr.GroupIDNotFound) {
		return groupView{}, cg.Err
	}

	gs, err := adm.DescribeGroups(ctx, group)
	if err != nil {
		return groupView{}, err
	}
	g, ok := gs[group]
	if !ok {
		return groupView{}, fmt.Errorf("group %s not described", group)
	}
	if g.Err != nil {
		return groupView{}, g.Err
	}
	v := groupView{Protocol: "classic", State: g.State, Assignor: g.Protocol}
	for _, m := range g.Members {
		mv := memberView{ID: m.MemberID, ClientID: m.ClientID, Host: m.ClientHost}
		if a, ok := m.Assigned.AsConsumer(); ok {
			for _, t := range a.Topics {
				if t.Topic == topic {
					mv.Assigned = slices.Sorted(slices.Values(t.Partitions))
				}
			}
		}
		v.Members = append(v.Members, mv)
	}
	return v, nil
}

func printGroup(ctx context.Context, adm *kadm.Client, group, topic string) error {
	v, err := describeGroup(ctx, adm, group, topic)
	if err != nil {
		return err
	}
	committed, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		return err
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return err
	}

	header := fmt.Sprintf("%s  group=%s  protocol=%s  assignor=%s  state=%s  members=%d",
		time.Now().Format("15:04:05"), group, v.Protocol, v.Assignor, v.State, len(v.Members))
	if v.Protocol == "consumer" {
		header += fmt.Sprintf("  group_epoch=%d", v.Epoch)
	}
	fmt.Println(header)

	owner := map[int32]string{}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  MEMBER\tCLIENT-ID\tHOST\tEPOCH\tOWNS\tTARGET")
	for _, m := range v.Members {
		id := shortID(m.ID)
		for _, p := range m.Assigned {
			owner[p] = id
		}
		epoch, target := "-", "-"
		if v.Protocol == "consumer" {
			epoch, target = fmt.Sprint(m.Epoch), partitionList(m.Target)
		}
		fmt.Fprintf(w, "  %s\t%s\t%s\t%s\t%s\t%s\n", id, m.ClientID, m.Host, epoch, partitionList(m.Assigned), target)
	}
	if err := w.Flush(); err != nil {
		return err
	}

	w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  PARTITION\tOWNER\tCOMMITTED\tEND\tLAG")
	for p := range int32(partitions) {
		e, _ := ends.Lookup(topic, p)
		c, ok := committed.Lookup(topic, p)
		com, lag := "-", "-"
		if ok && c.Err == nil && c.At >= 0 {
			com, lag = fmt.Sprint(c.At), fmt.Sprint(e.Offset-c.At)
		}
		fmt.Fprintf(w, "  %d\t%s\t%s\t%d\t%s\n", p, cmp.Or(owner[p], "-"), com, e.Offset, lag)
	}
	fmt.Fprintln(w)
	return w.Flush()
}

func partitionList(ps []int32) string {
	if len(ps) == 0 {
		return "(idle)"
	}
	s := make([]string, len(ps))
	for i, p := range ps {
		s[i] = fmt.Sprint(p)
	}
	return strings.Join(s, ",")
}

// shortID keeps the tail of a member ID: classic IDs are "<client-id>-<uuid>",
// KIP-848 IDs are client-generated UUIDs.
func shortID(id string) string {
	if len(id) <= 6 {
		return id
	}
	return id[len(id)-6:]
}
