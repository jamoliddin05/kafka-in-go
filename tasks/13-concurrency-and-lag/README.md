# 13 · Concurrency and lag

> **Goal:** process far more than one record at a time per partition without
> breaking per-key order or committing past unfinished work, keep memory
> bounded with pause/resume, hand partitions over cleanly while work is in
> flight, and measure lag the way you should alert on it: in time.

## Why this matters in prod

**Story 1: the 6-partition ceiling.** A notification service made one 40 ms
HTTP call per record, sequentially, on a topic with 6 partitions. Traffic
quadrupled, so the team scaled from 6 to 24 pods. Eighteen pods sat idle with
no partitions, and lag kept climbing. Adding partitions to a keyed topic would
have reshuffled keys and broken per-user order (task 02). The fix was
in-process concurrency: 6 pods, each running 32 workers routed by user ID.

**Story 2: the lost tail.** Their next version used a plain worker pool and
committed "the highest offset any worker finished" per partition. During
every deploy, records still in flight *below* that offset (slow HTTP calls,
retries) were simply never processed: the committed offset had already moved
past them, and the next pod started there. About 200 notifications vanished
per deploy, with no error anywhere. Kafka commits a position, not a set of
acknowledged messages; with out-of-order completion, the only safe position
is the end of the contiguous finished prefix.

## Concepts

```
                 ┌────────── poll loop: the only goroutine that touches the client ──────────┐
                 │  PollRecords ─► dispatch (non-blocking, rebalances held) ─► AllowRebalance │
                 └────────────────────────────────────────────────────────────────────────────┘
sequential      process inline                          order: per partition   max: 1 / latency
per-partition   p0 ─► [queue] ─► goroutine              order: per partition   max: partitions owned / latency
                p1 ─► [queue] ─► goroutine
per-key         any p ─► hash(key) % W ─► [q0] ─► w0    order: per key         max: W / latency, but the
                                      └─► [q1] ─► w1                           busiest worker sets the pace
unordered       any p ─► [one shared queue] ─► w0..wW   order: none            max: W / latency
```

- **Partitions cap a group's parallelism**, because each partition has
  exactly one owner. A member that processes records one by one does
  `1 / latency` records per second, whatever it owns. The only ways out are
  more partitions (painful on a keyed topic) or concurrency inside the member.
- **Goroutine per partition** keeps Kafka's own guarantee (order within a
  partition) and is the simplest correct model; franz-go's
  `goroutine_per_partition_consuming` examples show it. Its ceiling is the
  number of partitions you own.
- **Per-key pool**: route `hash(key) % W` to W workers. Order is kept where
  you usually need it (per entity), and parallelism is W instead of the
  partition count. A hot key is processed by one worker, at one record per
  latency.
- **Unordered pool**: any idle worker takes the next record. Fastest, and
  only correct when order does not matter at all (independent emails,
  idempotent upserts with versions).
- **The commit problem.** Offsets are cumulative: committing N means
  "everything below N is done". As soon as records of one partition finish
  out of order (per-key *and* unordered), you may only commit the contiguous
  finished prefix, the **watermark**:

```
partition 3, dispatched 100..107 (✓ finished, · running or queued)

offset   100  101  102  103  104  105  106  107
          ✓    ✓    ✓    ·    ✓    ✓    ·    ✓
                         ▲
               commit 103, not 108: a crash now must redo 103 and 106.
               104, 105, 107 will be processed twice. That is the price.
```

  After a crash or a `lost` partition, everything above the watermark runs
  again, so handlers must be idempotent (task 10). Confluent's Parallel
  Consumer goes further and encodes the finished offsets above the watermark
  into the commit metadata to shrink that window.
- **Backpressure.** The client keeps fetching while your workers sleep. Without
  a limit, in-process queues grow without bound, and in this task 20 s
  without pausing left 680 k records in flight and 380 MB of RSS. Pause a
  partition when its in-flight count reaches `MAX_INFLIGHT`, resume at half
  (the gap avoids flapping). franz-go's `PauseFetchPartitions` stops fetching
  it *and* drops the partition's already buffered data. Nothing is lost: the
  client re-fetches from where you stopped polling.
- **Rebalancing with work in flight.** With `BlockRebalanceOnPoll`, revoke
  and assign callbacks run only between `AllowRebalance` and the next poll,
  never while you dispatch, so the poll loop and the callbacks never race on
  the partition map. On revoke, for each partition: stop starting its records
  (the queued ones are above the watermark, so dropping them loses nothing),
  **wait for the ones already running** (otherwise the next owner can apply
  seq N+1 while you are still applying N), commit the watermark, return. On
  `lost` (session expired), do the same but **don't commit**: someone else may
  own the partition already.
- **Where commits come from.** Two places, serialized by one mutex: a
  periodic commit (`COMMIT_INTERVAL`) and the revoke callback. A periodic
  commit therefore can never land after the final commit of a partition you
  gave away.
- **Lag** is the KPI. *Records* = log end − committed offset. *Time* = now −
  timestamp of the first uncommitted record, i.e. how long the oldest
  unprocessed thing has been waiting. Alert on time: 50 records of lag on a
  low-traffic partition can be two hours of a stuck poison pill, while
  100 k records on a busy one can be three seconds. Committed offsets trail
  real progress by up to `COMMIT_INTERVAL` and by whatever is stuck below
  the watermark, so lag measured from outside is slightly pessimistic. That
  is the right direction for an alert.

Fetch tuning, for when the consumer, not the handler, is the bottleneck:
`FetchMaxBytes` (50 MiB per fetch, and one fetch per broker can be buffered),
`FetchMaxPartitionBytes` (1 MiB), `FetchMinBytes` (1 byte: answer as soon as
anything exists), `FetchMaxWait` (how long the broker may wait for
`FetchMinBytes`, 5 s in franz-go and 500 ms in Java; it only matters when
there is no data or you raised `FetchMinBytes`), `MaxConcurrentFetches`
(unbounded) and the `max` of `PollRecords`.

## Coming from Pulsar

| Pulsar | Kafka | The difference that bites |
|---|---|---|
| Shared subscription: the broker spreads messages over consumers, individual acks | No broker-side equivalent for consumer groups; an unordered pool plus a watermark in the client (or a share group, task 16) | You cannot ack 105 while 103 is running: the watermark *is* your individual-ack emulation |
| Key_Shared: the broker routes key hash ranges to consumers | A per-key pool inside each member | The parallelism lives in one process; a hot key still serializes |
| Exclusive / Failover | A consumer group | Same ceiling: one active consumer per partition |
| `receiverQueueSize` prefetch with flow permits | `FetchMaxBytes` / `FetchMaxPartitionBytes` and `PauseFetchPartitions` | franz-go prefetches up to 50 MiB per broker by default |
| `ackTimeout` redelivers a message that takes too long | Nothing is redelivered while you own the partition; a stuck record just holds the watermark | Watch lag time per partition |
| `msgBacklog` per subscription | Lag in records from committed vs end offsets; lag in time you compute | Kafka has no built-in "age of the oldest unacked message" |

## Run it

```bash
make dev T=13-concurrency-and-lag R="producer consumer"            # MODE etc. from task.env
go run ./tasks/13-concurrency-and-lag -role lag                       # lag table every second
MODE=per-key go run ./tasks/13-concurrency-and-lag -role consumer     # another member, from the host
make scale T=13-concurrency-and-lag R=consumer N=3                    # more members in compose
```

A consumer started from the host joins the same group as the compose one
(`GROUP`), so starting it is a rebalance. The consumer prints every 2 s:

```
msg=progress mode=per-key rate=2526/s processed=70370 violations=0 inflight=4460 paused=12/12
```

`violations` counts events applied after a newer event of the same user;
`inflight` = dispatched, not finished; `paused` = partitions paused by
backpressure. The producer writes 12 partitions, 1000 users and a per-user
sequence number. Knobs are in [`task.env`](task.env).

**Starting fresh.** A new `GROUP` starts at the beginning of the topic
(franz-go's default start offset is the earliest). To skip the backlog,
move an empty group to the end first:

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:29092 \
  --group t13.consumer --topic t13.events --reset-offsets --to-latest --execute
```

In the starter, `MODE=sequential` works, the other modes exit with
`not implemented yet`, the lag tool shows `?` for LAG TIME, and
`go test ./tasks/13-concurrency-and-lag/` fails. The reference is in
[`solution/`](solution/main.go):
`make dev T=13-concurrency-and-lag/solution R="producer consumer"`.

## Your task

1. **TODO(13.1) Watermark tracker.** In [`watermark.go`](watermark.go)
   implement `add` and `done`: keep the dispatched offsets in order, mark
   finished ones, and when the oldest finishes, drop the finished prefix and
   set the commit to one past it (with that record's leader epoch). Offsets
   can have gaps.
   *Done when:* `go test ./tasks/13-concurrency-and-lag/` passes. It is the
   same test as the solution's, including random completion orders and a
   revoke mid-flight.
2. **TODO(13.2) Goroutine per partition.** In `assigned`, give each
   partition a queue (`part.recs`) and a goroutine that calls `e.handle`; in
   `route`, send to it.
   *Done when:* `MODE=per-partition` runs at about 12 × 50 = 600 rec/s with
   `violations=0` while the producer outpaces it.
3. **TODO(13.3) Worker pools.** In `startPool`, create the queues and
   workers (per worker for per-key, one shared for unordered); route by
   `maphash.Bytes(seed, key) % W` or to the shared queue.
   *Done when:* `per-key` reaches thousands of rec/s with `violations=0`,
   and `unordered` is faster still, with violations climbing.
4. **TODO(13.4) Backpressure.** Pause in `dispatch` at `MAX_INFLIGHT`,
   resume in `handle` at half.
   *Done when:* behind a backlog, `paused` shows up in the progress line and
   `inflight` stays under 12 × (`MAX_INFLIGHT` + `MAX_POLL`).
5. **TODO(13.5) Revoke with work in flight.** In `release`, after the given
   code stops the partitions: wait for running records, then commit the
   watermark unless the partitions were lost.
   *Done when:* starting a second consumer makes the first log
   `revoked: running records finished, watermark committed ... committed="map[6:36820 ...]"`,
   and the second logs `first record after assignment partition=6 offset=36820`:
   the same offset for every partition that moved.
6. **TODO(13.6) Lag in time.** In `headTimestamps`, read the first record at
   each committed offset with a group-less client and return its timestamp.
   *Done when:* LAG TIME shows durations, and a stalled partition's LAG TIME
   grows by one second per second.

## Experiments

**A. Measure the four modes.** Build a backlog first so the consumer is
always the bottleneck (`RATE=5000` producer for 20 s, keep it running), then
run each mode for about 25 s. On this laptop (MacBook Air, the 3-broker
cluster in Docker shared with other workloads), with `PROCESS_TIME=20ms`
(0.5x to 1.5x), `WORKERS=64`, 12 partitions, one consumer,
`MAX_INFLIGHT=500`, `MAX_POLL=500`:

| MODE | on this laptop | ceiling | per-key order |
|---|---|---|---|
| sequential | ~47 rec/s | 1 / 20 ms = 50 | kept |
| per-partition | ~590 rec/s | 12 × 50 = 600 | kept |
| per-key | ~2500 rec/s | 64 × 50 = 3200 | kept |
| unordered | ~3200 rec/s | 64 × 50 = 3200 | broken, ~340 violations/s |

Why does per-key stay below its ceiling? (1000 users over 64 workers is not
an even split. The busiest worker's queue keeps records of every partition
in flight; those partitions hit `MAX_INFLIGHT`, get paused, and the idle
workers starve.)

**B. Scale out instead.** `make scale T=13-concurrency-and-lag R=consumer N=3` with
`MODE=sequential`: each member still does about 47 rec/s. Try `N=13`: one
member gets no partition and does nothing. Compare with one per-key member.

**C. A hot key.** Restart the producer with `RATE=1500 HOT_PCT=10` (150 rec/s
for `user-0`, three times what one worker can do), reset two fresh groups to
the end, and run `MODE=per-key` and `MODE=unordered` side by side for 45 s
(`GROUP=t13.hk-per-key`, `GROUP=t13.hk-unordered`). Here:

```
group t13.hk-per-key                                 group t13.hk-unordered
PARTITION  LAG   LAG TIME                            PARTITION  LAG  LAG TIME
        0  1195     12.3s                                    0   11     100ms
        1    13     100ms                                    1   16     100ms
        7  9041     35.5s   ← user-0's partition             7   22     100ms
  (5 more at ~1500 / 13 s, 4 more at ~0)              TOTAL  163     100ms
    TOTAL 17810     35.5s
```

per-key processed 1290 rec/s (falling behind), unordered kept up at
1500 rec/s with 3478 order violations. Why do partitions that never see
`user-0` show 13 s of lag in per-key mode? (Their keys share the hot
worker's queue, and each record waiting there holds its partition's
watermark.) What happens on restart to everything finished above those
watermarks?

**D. Rebalance with work in flight.** Run one per-key consumer, add a second
after 10 s, stop the second after another 10 s. Every partition that moves
must start exactly where the previous owner committed:

```
inst=A "revoked: running records finished, watermark committed" partitions="[6 7 8 9 10 11]" committed="map[6:36820 7:36880 8:36619 9:37231 10:35560 11:36165]" dropped_unstarted=2584 took=27ms
inst=B "first record after assignment" partition=6 offset=36820
inst=B "first record after assignment" partition=7 offset=36880
```

2584 queued records were dropped in 27 ms, and none was lost: they are
above the watermark. Only partitions 6-11 moved: the default
cooperative-sticky balancer revokes only what changes hands (task 05). Now
`kill -9` a member instead. What does the survivor's first offset look like,
and why does it take about 45 s before it gets the partitions?

**E. Turn backpressure off.** Run per-partition with `MAX_INFLIGHT=1000000`
behind a backlog. After 20 s here: `inflight=681582`, RSS 381 MB, versus
`inflight=7471 paused=12/12`, RSS 140 MB with the default. The rate is the
same (about 585 rec/s). What else gets slower when the queues are huge?
(Look at what a revoke has to throw away, and at what `BlockRebalanceOnPoll`
does when `dispatch` blocks on a full queue.)

**F. Records vs time.** With the lag tool running, stop the consumer. LAG
TIME grows by one second per second on every partition regardless of
`RATE`; LAG in records grows with `RATE`. Restart the producer with `RATE=10`:
100 records of lag now means 10 s. Which of the two would you put in an
alert, and with what threshold?

## Edge cases and gotchas

- **Order across a handoff** only holds if revoke waits for running records.
  Order within one process is not enough: two members can briefly work on
  the same partition if the old one does not wait.
- **A crash skips the revoke.** Everything above the last committed
  watermark runs again, including records that finished out of order.
  Handlers must be idempotent.
- **One slow record holds the watermark.** Throughput looks fine while the
  committed offset of that partition stands still and lag time climbs. That
  is the signal for a stuck record or a poison pill (task 09).
- **Pauses outlive assignments** in franz-go ("paused partitions persist until
  resumed"). Resume on revoke, or a partition that comes back later stays
  paused forever.
- **Pause/resume is not free:** each pause throws away buffered data that
  must be fetched again. Resume at a lower mark than you pause at.
- **`BlockRebalanceOnPoll` delays every rebalance** by whatever runs between
  a poll and `AllowRebalance`. Keep `dispatch` non-blocking, and size
  `MAX_POLL` so that sequential processing of one poll stays far below the
  rebalance timeout (60 s by default). `OnPartitionsCallbackBlocked` tells
  you a rebalance is waiting.
- **Revoke must finish within the rebalance timeout.** Drop queued records,
  wait only for running ones. If one record can run for minutes, give the
  handler a context you cancel on revoke.
- **W is limited by what is downstream.** 64 workers in front of a
  10-connection DB pool is 10 workers plus 54 goroutines waiting. Throughput
  = concurrency / latency (Little's law); pick W from the downstream
  capacity, not from the CPU count.
- **Lag is measured against the high watermark** (kadm's `Lag` lists end
  offsets that way). For a transactional topic read with `read_committed`,
  the reachable end is the LSO, so an open transaction shows up as lag that
  no amount of consumer speed removes (task 12).

## Check yourself

<details><summary>1. Why can't you commit the highest finished offset of a partition?</summary>

A commit of N means "everything below N is done". If 103 is still running
when you commit 108 and the process dies, the next owner starts at 108 and
103 is never processed. Commit one past the longest finished prefix instead,
and accept that a crash replays whatever finished above it.
</details>

<details><summary>2. 12 partitions and 30 pods in one group. How many pods do work? And if each runs a per-key pool of 16 workers?</summary>

12 pods, one partition each; 18 idle. With per-key pools, still 12 pods own
partitions, but each processes up to 16 records at once: up to 192 in
parallel, bounded by key distribution and downstream capacity. The idle 18
pods only help as hot standbys.
</details>

<details><summary>3. On revoke, why wait for running records but drop the queued ones?</summary>

Queued records are above the watermark you are about to commit, so the next
owner will process them anyway, and dropping them makes the revoke fast.
Running records may already be producing side effects; if you don't wait,
the next owner can apply a newer event of the same key before you finish the
older one.
</details>

<details><summary>4. Partition A has 50 records of lag, partition B has 2 million. Which one do you page on?</summary>

You can't tell from records. Look at lag in time: if A's oldest unprocessed
record is two hours old (a stuck record), page on A. If B's is three seconds
old on a topic doing 500 k records/s, B is fine.
</details>

<details><summary>5. What does a hot key do in per-key mode, besides slowing that key?</summary>

Its worker's queue backs up, and every record from other partitions that
hashes to that worker waits behind it. Each such record holds its
partition's watermark, so commits stall on many partitions, their lag in time
grows, and a restart replays everything finished above those watermarks.
</details>

## franz-go ↔ Java client names

| franz-go | Java / docs | Note |
|---|---|---|
| `kgo.OnPartitionsAssigned / Revoked / Lost` | `ConsumerRebalanceListener.onPartitionsAssigned / Revoked / Lost` | |
| `kgo.BlockRebalanceOnPoll` + `AllowRebalance` | (built in) Java runs the listener inside `poll()` | without it, franz-go callbacks run concurrently with your processing |
| `Client.PauseFetchPartitions / ResumeFetchPartitions` | `consumer.pause() / resume()` | |
| `Client.PollRecords(ctx, n)` | `max.poll.records` | Java default 500 |
| `Client.CommitOffsetsSync(ctx, offsets, fn)` | `commitSync(Map<TopicPartition, OffsetAndMetadata>)` | |
| `kgo.RebalanceTimeout` | `max.poll.interval.ms` (sent as the rebalance timeout) | Java also leaves the group if you don't poll within it; franz-go has no poll deadline |
| `kgo.FetchMaxBytes` | `fetch.max.bytes` | 50 MiB in both |
| `kgo.FetchMaxPartitionBytes` | `max.partition.fetch.bytes` | 1 MiB in both |
| `kgo.FetchMinBytes` / `kgo.FetchMaxWait` | `fetch.min.bytes` / `fetch.max.wait.ms` | max wait: franz-go 5 s, Java 500 ms |
| `kgo.MaxConcurrentFetches` | (none) | bounds buffered fetch memory |
| `kadm.Lag` | `kafka-consumer-groups.sh --describe`, `Admin.listConsumerGroupOffsets` + `listOffsets` | lag in records only |

## Further reading

- franz-go: [goroutine-per-partition examples](https://github.com/twmb/franz-go/tree/master/examples/goroutine_per_partition_consuming), [`BlockRebalanceOnPoll`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#BlockRebalanceOnPoll)
- Confluent: [Introducing the Parallel Consumer](https://www.confluent.io/blog/introducing-confluent-parallel-message-processing-client/), [source](https://github.com/confluentinc/parallel-consumer)
- Kafka docs: [Consumer configs](https://kafka.apache.org/documentation/#consumerconfigs)
- LinkedIn [Burrow](https://github.com/linkedin/Burrow): lag evaluation by window instead of a fixed threshold
- [KIP-932: Queues for Kafka](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka), broker-side parallelism beyond partitions (task 16)
