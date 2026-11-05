package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"idiomaticGo/kafka/internal/kit"
)

// audit reads t07.jobs and t07.results up to their current end and reports
// what the last deploy cost: duplicates, lost jobs, and how long jobs waited.
func audit(ctx context.Context, app *kit.App) error {
	group := kit.Env("GROUP", defaultGroup)
	since := kit.EnvDuration("SINCE", 5*time.Minute)
	stall := kit.EnvDuration("STALL", time.Second)

	cl, err := app.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	// Commits first: every result of a committed job is then below the end
	// offsets listed next, so "committed but no result" really means lost.
	committed, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		return fmt.Errorf("committed offsets of %s: %w", group, err)
	}
	ends, err := adm.ListEndOffsets(ctx, jobsTopic, resultsTopic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return fmt.Errorf("end offsets (run the producer first?): %w", err)
	}
	starts, err := adm.ListStartOffsets(ctx, jobsTopic, resultsTopic)
	if since > 0 && err == nil {
		starts, err = adm.ListOffsetsAfterMilli(ctx, time.Now().Add(-since).UnixMilli(), jobsTopic, resultsTopic)
	}
	if err == nil {
		err = starts.Error()
	}
	if err != nil {
		return fmt.Errorf("start offsets: %w", err)
	}
	recs, err := readRange(ctx, app, starts, ends)
	if err != nil {
		return err
	}
	rep := buildReport(recs, committed, stall)
	rep.print(group, since, stall)
	return nil
}

// readRange reads every partition from starts up to (excluding) ends.
func readRange(ctx context.Context, app *kit.App, starts, ends kadm.ListedOffsets) ([]*kgo.Record, error) {
	want := map[string]map[int32]kgo.Offset{}
	left := 0
	ends.Each(func(e kadm.ListedOffset) {
		s, _ := starts.Lookup(e.Topic, e.Partition)
		if s.Offset < 0 || s.Offset >= e.Offset {
			return
		}
		if want[e.Topic] == nil {
			want[e.Topic] = map[int32]kgo.Offset{}
		}
		want[e.Topic][e.Partition] = kgo.NewOffset().At(s.Offset)
		left++
	})
	if left == 0 {
		return nil, nil
	}

	cl, err := app.NewClient(kgo.ConsumePartitions(want))
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var recs []*kgo.Record
	for left > 0 {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("reading topics: %w", err)
		}
		fetches.EachError(func(t string, p int32, err error) {
			app.Log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			e, _ := ends.Lookup(p.Topic, p.Partition)
			for _, r := range p.Records {
				if r.Offset >= e.Offset {
					continue
				}
				recs = append(recs, r)
				if r.Offset == e.Offset-1 {
					left--
				}
			}
		})
	}
	return recs, nil
}

type jobState struct {
	job     Job
	part    int32
	offset  int64
	results []Result
}

func (j *jobState) wait() time.Duration { return j.results[0].ProcessedAt.Sub(j.job.CreatedAt) }
func (j *jobState) owner() string       { return j.results[0].Worker + "@" + j.results[0].Version }

type partStats struct {
	jobs, done, dupes, lost, pending int
	maxWait                          time.Duration
	maxWaitAt                        time.Time
	owners                           []string
}

type stallRun struct {
	part          int32
	from          time.Time
	worst         time.Duration
	jobs          int
	before, after string
}

type report struct {
	parts      map[int32]*partStats
	stalls     []stallRun
	dupes      []*jobState
	unknownRes int
}

func buildReport(recs []*kgo.Record, committed kadm.OffsetResponses, stall time.Duration) report {
	byPart := map[int32][]*jobState{}
	byID := map[string]*jobState{}
	var results []Result
	for _, r := range recs {
		switch r.Topic {
		case jobsTopic:
			var j Job
			if json.Unmarshal(r.Value, &j) != nil {
				continue
			}
			js := &jobState{job: j, part: r.Partition, offset: r.Offset}
			byPart[r.Partition] = append(byPart[r.Partition], js)
			byID[j.ID] = js
		case resultsTopic:
			var res Result
			if json.Unmarshal(r.Value, &res) == nil {
				results = append(results, res)
			}
		}
	}

	rep := report{parts: map[int32]*partStats{}}
	for _, res := range results {
		js, ok := byID[res.JobID]
		if !ok {
			rep.unknownRes++ // job is older than SINCE
			continue
		}
		js.results = append(js.results, res)
	}

	for p, jobs := range byPart {
		slices.SortFunc(jobs, func(a, b *jobState) int { return cmp.Compare(a.offset, b.offset) })
		ps := &partStats{}
		rep.parts[p] = ps
		commit, hasCommit := committed.Lookup(jobsTopic, p)

		var run *stallRun
		lastOwner := ""
		for _, js := range jobs {
			ps.jobs++
			if len(js.results) == 0 {
				if hasCommit && js.offset < commit.At {
					ps.lost++
				} else {
					ps.pending++
				}
				continue
			}
			slices.SortFunc(js.results, func(a, b Result) int { return a.ProcessedAt.Compare(b.ProcessedAt) })
			ps.done++
			if len(js.results) > 1 {
				ps.dupes += len(js.results) - 1
				rep.dupes = append(rep.dupes, js)
			}
			if w := js.wait(); w > ps.maxWait {
				ps.maxWait, ps.maxWaitAt = w, js.job.CreatedAt
			}
			if o := js.owner(); len(ps.owners) == 0 || ps.owners[len(ps.owners)-1] != o {
				ps.owners = append(ps.owners, o)
			}

			if js.wait() > stall {
				if run == nil {
					run = &stallRun{part: p, from: js.job.CreatedAt, before: lastOwner, after: js.owner()}
				}
				run.jobs++
				run.worst = max(run.worst, js.wait())
			} else if run != nil {
				rep.stalls = append(rep.stalls, *run)
				run = nil
			}
			lastOwner = js.owner()
		}
		if run != nil {
			rep.stalls = append(rep.stalls, *run)
		}
	}
	slices.SortFunc(rep.stalls, func(a, b stallRun) int { return a.from.Compare(b.from) })
	slices.SortFunc(rep.dupes, func(a, b *jobState) int { return a.job.CreatedAt.Compare(b.job.CreatedAt) })
	return rep
}

func (rep report) print(group string, since, stall time.Duration) {
	window := "everything"
	if since > 0 {
		window = "jobs from the last " + since.String()
	}
	fmt.Printf("audit of %s, group %s, %s\n\n", jobsTopic, group, window)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "PARTITION\tJOBS\tDONE\tDUPLICATES\tLOST\tPENDING\tMAX WAIT\tAT\tOWNERS")
	var total partStats
	for _, p := range slices.Sorted(maps.Keys(rep.parts)) {
		ps := rep.parts[p]
		total.jobs += ps.jobs
		total.done += ps.done
		total.dupes += ps.dupes
		total.lost += ps.lost
		total.pending += ps.pending
		fmt.Fprintf(w, "%d\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\n", p, ps.jobs, ps.done, ps.dupes, ps.lost, ps.pending,
			ps.maxWait.Round(10*time.Millisecond), ps.maxWaitAt.Format("15:04:05"), ownerChain(ps.owners))
	}
	fmt.Fprintf(w, "total\t%d\t%d\t%d\t%d\t%d\t\t\t\n", total.jobs, total.done, total.dupes, total.lost, total.pending)
	w.Flush()

	fmt.Printf("\nstalls (jobs that waited longer than %s):\n", stall)
	if len(rep.stalls) == 0 {
		fmt.Println("  none")
	}
	for _, s := range rep.stalls {
		before := s.before
		if before == "" {
			before = "(start)"
		}
		fmt.Printf("  p%d  from %s  worst wait %-7s  %4d jobs  %s -> %s\n",
			s.part, s.from.Format("15:04:05.0"), s.worst.Round(100*time.Millisecond), s.jobs, before, s.after)
	}

	if len(rep.dupes) > 0 {
		fmt.Printf("\nduplicates (first %d of %d jobs processed more than once):\n", min(len(rep.dupes), 5), len(rep.dupes))
		for _, js := range rep.dupes[:min(len(rep.dupes), 5)] {
			var by []string
			for _, r := range js.results {
				by = append(by, fmt.Sprintf("%s@%s %s", r.Worker, r.Version, r.ProcessedAt.Format("15:04:05.000")))
			}
			fmt.Printf("  %s p%d@%d: %s\n", js.job.ID, js.part, js.offset, strings.Join(by, ", "))
		}
	}
	if rep.unknownRes > 0 {
		fmt.Printf("\n(%d results belong to jobs older than the window and were skipped)\n", rep.unknownRes)
	}
}

func ownerChain(owners []string) string {
	if len(owners) > 6 {
		return fmt.Sprintf("%s -> ... (%d owners) -> %s", owners[0], len(owners), owners[len(owners)-1])
	}
	return strings.Join(owners, " -> ")
}
