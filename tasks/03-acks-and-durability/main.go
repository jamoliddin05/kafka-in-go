// Task 03: acks and durability. Read README.md first; the TODOs follow its "Your task" steps.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"idiomaticGo/kafka/internal/kit"
)

const (
	paymentsTopic = "t03.payments"
	groupName     = "t03.audit"
)

var errTODO = errors.New("not implemented yet, see README.md")

func main() {
	kit.Main(map[string]kit.Role{
		"producer": produce,
		"consumer": consume,
		"inspect":  inspect,
	})
}

// Payment carries what the consumer needs to prove loss: a per-(producer,
// partition) sequence without holes, unless the cluster lost something.
type Payment struct {
	Producer  string    `json:"producer"`
	Partition int32     `json:"partition"`
	Seq       int64     `json:"seq"`
	SentAt    time.Time `json:"sent_at"`
}

func ensureTopics(ctx context.Context, app *kit.App, cl *kgo.Client) error {
	for _, t := range []kit.Topic{
		{Name: paymentsTopic, Partitions: 3},
		// min.insync.replicas = RF: every broker restart makes it unwritable.
		{Name: "t03.strict", Partitions: 3, Configs: map[string]string{"min.insync.replicas": "3"}},
		// One copy: the broker that hosts a partition is a single point of loss.
		{Name: "t03.rf1", Partitions: 3, RF: 1},
	} {
		if err := app.EnsureTopic(ctx, cl, t); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------- producer

// producerOpts turns ACKS, DELIVERY_TIMEOUT and RECORD_RETRIES into client options.
func producerOpts(acks string, deliveryTimeout time.Duration, retries int) ([]kgo.Opt, error) {
	var opts []kgo.Opt
	switch acks {
	case "all":
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	case "leader", "none":
		// TODO(03.1): kgo.RequiredAcks(kgo.LeaderAck()) and kgo.NoAck(). Try
		// RequiredAcks alone first and read what kgo.NewClient says (README step 1).
		return nil, errTODO
	default:
		return nil, fmt.Errorf("bad ACKS %q, want all|leader|none", acks)
	}
	// TODO(03.2): add kgo.RecordDeliveryTimeout and kgo.RecordRetries (0 = not
	// set). Until then a retriable broker error is retried forever (README step 2).
	_, _ = deliveryTimeout, retries
	return opts, nil
}

// classify maps a produce error to a short, stable label for the stats line.
func classify(err error) string {
	// TODO(03.2): the Kafka error name for *kerr.Error (errors.AsType), short
	// labels for kgo.ErrRecordTimeout/ErrRecordRetries/ErrClientClosed and ctx errors.
	_ = kerr.IsRetriable
	return err.Error()
}

func produce(ctx context.Context, app *kit.App) error {
	topic := kit.Env("TOPIC", paymentsTopic)
	acks := kit.Env("ACKS", "all")
	rate := max(1, kit.EnvInt("RATE", 500))

	opts, err := producerOpts(acks, kit.EnvDuration("DELIVERY_TIMEOUT", 15*time.Second), kit.EnvInt("RECORD_RETRIES", 0))
	if err != nil {
		return err
	}
	opts = append(opts,
		// We pick partitions ourselves to keep a gapless seq per partition.
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		// franz-go lingers 10ms by default; 0 makes the latency below the cost of acks alone.
		kgo.ProducerLinger(kit.EnvDuration("LINGER", 0)),
	)
	cl, err := app.NewClient(opts...)
	if err != nil {
		return err
	}
	defer cl.Close()

	if err := ensureTopics(ctx, app, cl); err != nil {
		return err
	}
	partitions, err := partitionCount(ctx, cl, topic)
	if err != nil {
		return err
	}

	id := fmt.Sprintf("%s-%s", app.Instance, rand.Text()[:4])
	st := newProduceStats()
	app.Log.Info("producing", "topic", topic, "acks", acks, "rate", rate, "producer", id, "partitions", partitions)

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() {
		tick := time.NewTicker(kit.EnvDuration("STATS_EVERY", time.Second))
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				st.log(app, cl.BufferedProduceRecords())
			}
		}
	})

	seqs := make([]int64, partitions)
	err = pace(ctx, rate, func() {
		p := int32(st.sentTotal() % int64(partitions))
		seqs[p]++
		pay := Payment{Producer: id, Partition: p, Seq: seqs[p], SentAt: time.Now()}
		value, _ := json.Marshal(pay)
		start := time.Now()
		st.sent()
		// Records still buffered on Ctrl+C fail with context canceled; task 04 fixes that.
		cl.Produce(ctx, &kgo.Record{Topic: topic, Partition: p, Value: value}, func(_ *kgo.Record, err error) {
			st.done(pay, time.Since(start), err)
		})
	})
	st.log(app, cl.BufferedProduceRecords())
	return err
}

// pace calls fn rate times per second until ctx is done. Produce may block
// when the buffer is full; then the effective rate drops, which is the point.
func pace(ctx context.Context, rate int, fn func()) error {
	const tickEvery = 10 * time.Millisecond
	perTick := float64(rate) * tickEvery.Seconds()
	tick := time.NewTicker(tickEvery)
	defer tick.Stop()
	var owed float64
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
		for owed += perTick; owed >= 1 && ctx.Err() == nil; owed-- {
			fn()
		}
	}
}

func partitionCount(ctx context.Context, cl *kgo.Client, topic string) (int, error) {
	details, err := kadm.NewClient(cl).ListTopics(ctx, topic)
	if err != nil {
		return 0, err
	}
	if err := details.Error(); err != nil {
		return 0, err
	}
	return len(details[topic].Partitions), nil
}

type produceStats struct {
	mu        sync.Mutex
	total     int64
	sentN     int64
	ackedN    int64
	failed    map[string]int64
	latencies []time.Duration
	failedLog []string
}

func newProduceStats() *produceStats { return &produceStats{failed: map[string]int64{}} }

func (s *produceStats) sent() {
	s.mu.Lock()
	s.total++
	s.sentN++
	s.mu.Unlock()
}

func (s *produceStats) sentTotal() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total
}

func (s *produceStats) done(p Payment, took time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.failed[classify(err)]++
		if len(s.failedLog) < 5 {
			s.failedLog = append(s.failedLog, fmt.Sprintf("p%d/seq%d", p.Partition, p.Seq))
		}
		return
	}
	s.ackedN++
	s.latencies = append(s.latencies, took)
}

// log prints and resets one interval.
func (s *produceStats) log(app *kit.App, buffered int64) {
	s.mu.Lock()
	sent, acked, lat, failedLog := s.sentN, s.ackedN, s.latencies, s.failedLog
	failed := s.failed
	s.sentN, s.ackedN, s.latencies, s.failedLog, s.failed = 0, 0, nil, nil, map[string]int64{}
	s.mu.Unlock()

	slices.Sort(lat)
	var fails []string
	for _, k := range slices.Sorted(maps.Keys(failed)) {
		fails = append(fails, fmt.Sprintf("%s=%d", k, failed[k]))
	}
	attrs := []any{
		"sent", sent, "acked", acked, "failed", strings.Join(fails, " "), "buffered", buffered,
		"p50", quantile(lat, 0.50), "p99", quantile(lat, 0.99), "max", quantile(lat, 1),
	}
	if len(failedLog) > 0 {
		attrs = append(attrs, "first_failed", strings.Join(failedLog, ","))
	}
	app.Log.Info("acks", attrs...)
}

func quantile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := min(len(sorted)-1, int(q*float64(len(sorted))))
	return sorted[i].Round(10 * time.Microsecond)
}

// ---------------------------------------------------------------- consumer

type stream struct {
	producer  string
	partition int32
}

type finding struct {
	missingFrom, missing int64 // seqs [missingFrom, missingFrom+missing) were skipped
	duplicate            bool
	todo                 bool // starter only: observe is not implemented yet
}

// lossDetector expects every (producer, partition) seq exactly once, in order.
type lossDetector struct {
	next map[stream]int64
}

func newLossDetector() *lossDetector { return &lossDetector{next: map[stream]int64{}} }

func (d *lossDetector) observe(p Payment) finding {
	key := stream{p.Producer, p.Partition}
	want, seen := d.next[key]
	// TODO(03.3): in order, duplicate (< want) or hole (> want)? Store the next
	// expected seq; the first record of a stream is never a hole (README step 3).
	_, _ = want, seen
	return finding{todo: true}
}

func consume(ctx context.Context, app *kit.App) error {
	topic := kit.Env("TOPIC", paymentsTopic)
	group := kit.Env("GROUP", groupName)

	cl, err := app.NewClient(kgo.ConsumeTopics(topic), kgo.ConsumerGroup(group))
	if err != nil {
		return err
	}
	defer cl.Close()

	det := newLossDetector()
	var (
		mu                      sync.Mutex
		records, missing, dupes int64
		e2e                     []time.Duration
		wg                      sync.WaitGroup
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
			mu.Lock()
			slices.Sort(e2e)
			app.Log.Info("audit", "records", records, "missing_total", missing, "duplicates_total", dupes,
				"e2e_p50", quantile(e2e, 0.5), "e2e_max", quantile(e2e, 1))
			records, e2e = 0, nil
			mu.Unlock()
		}
	})

	app.Log.Info("auditing", "topic", topic, "group", group)
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		now := time.Now()
		mu.Lock()
		for iter := fetches.RecordIter(); !iter.Done(); {
			r := iter.Next()
			var p Payment
			if err := json.Unmarshal(r.Value, &p); err != nil {
				app.Log.Error("bad record", "partition", r.Partition, "offset", r.Offset, "err", err)
				continue
			}
			records++
			e2e = append(e2e, now.Sub(p.SentAt))
			f := det.observe(p)
			if f.todo {
				mu.Unlock()
				return errTODO
			}
			switch {
			case f.duplicate:
				dupes++
			case f.missing > 0:
				missing += f.missing
				app.Log.Warn("records missing", "producer", p.Producer, "partition", p.Partition,
					"from_seq", f.missingFrom, "to_seq", f.missingFrom+f.missing-1, "count", f.missing, "offset", r.Offset)
			}
		}
		mu.Unlock()
	}
}

// ---------------------------------------------------------------- inspect

type partitionState struct {
	partition                   int32
	leader, epoch               int32
	replicas, isr, elr, lastElr []int32
	offline                     []int32
}

// describePartitions uses DescribeTopicPartitions (KIP-966): unlike Metadata,
// it returns the eligible leader replicas.
func describePartitions(ctx context.Context, cl *kgo.Client, topic string) ([]partitionState, error) {
	// TODO(03.4): send a kmsg.DescribeTopicPartitionsRequest for topic, check its
	// ErrorCode and map the partitions to sorted partitionStates (README step 4).
	_ = kmsg.NewPtrDescribeTopicPartitionsRequest
	return nil, errTODO
}

func inspect(ctx context.Context, app *kit.App) error {
	topic := kit.Env("TOPIC", paymentsTopic)
	watch := kit.EnvDuration("WATCH", 0)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()

	for {
		if err := inspectOnce(ctx, cl, topic); err != nil {
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

func inspectOnce(ctx context.Context, cl *kgo.Client, topic string) error {
	adm := kadm.NewClient(cl)
	configs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return err
	}
	rc, err := configs.On(topic, nil)
	if err != nil {
		return err
	}
	minISR := 1
	fmt.Printf("%s  %s\n", time.Now().Format("15:04:05"), topic)
	for _, c := range rc.Configs {
		switch c.Key {
		case "min.insync.replicas", "unclean.leader.election.enable", "retention.ms":
			fmt.Printf("  %s=%s (%s)\n", c.Key, c.MaybeValue(), c.Source)
			if c.Key == "min.insync.replicas" {
				fmt.Sscan(c.MaybeValue(), &minISR)
			}
		}
	}

	parts, err := describePartitions(ctx, cl, topic)
	if err != nil {
		return err
	}
	// The end offset a consumer sees is the high watermark, not the leader's log end.
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "PARTITION\tLEADER\tEPOCH\tREPLICAS\tISR\tELR\tLAST KNOWN ELR\tHW\tSTATUS")
	for _, p := range parts {
		hw := "?"
		if e, ok := ends.Lookup(topic, p.partition); ok && e.Err == nil {
			hw = fmt.Sprint(e.Offset)
		}
		fmt.Fprintf(w, "%d\t%d\t%d\t%v\t%v\t%v\t%v\t%s\t%s\n", p.partition, p.leader, p.epoch,
			p.replicas, p.isr, p.elr, p.lastElr, hw, status(p, minISR))
	}
	return w.Flush()
}

func status(p partitionState, minISR int) string {
	var s []string
	switch {
	case p.leader < 0:
		s = append(s, "OFFLINE")
	case len(p.isr) < min(minISR, len(p.replicas)):
		s = append(s, "UNDER_MIN_ISR(acks=all fails, HW frozen)")
	case len(p.isr) < len(p.replicas):
		s = append(s, "UNDER_REPLICATED")
	}
	if len(s) == 0 {
		return "ok"
	}
	return strings.Join(s, ",")
}
