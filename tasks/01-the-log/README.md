# 01 · The log

> **Goal:** stop thinking of Kafka as a queue. A topic is a set of append-only
> logs that nobody "consumes away". Everything else in this course follows
> from that one idea.

## Why this matters in prod

**Story 1: the silent skip.** A reconciliation consumer was switched off over
a long holiday: 9 days. Topic retention was the default 7 days. When it came
back, its committed offset pointed at data that had already been deleted, so
the client fell back to `auto.offset.reset=latest`. Two days of payments were
skipped without a single error log. Finance found it a month later. In Pulsar
the unacked backlog would still have been there. **Kafka retention ignores
consumers completely.**

**Story 2: the time machine.** A bad deploy wrote wrong totals into a read
model for 3 hours. The fix was not a data migration: ship the bug fix, reset
the consumer group to "3 hours ago" by timestamp, and let it reprocess. That
only works because the log is still there and can be read by time.

## Concepts

```
topic t01.events (3 partitions, RF=3)

partition 0  │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │ 7 │ 8 │   ← producers append at the end
               ▲                   ▲               ▲
        log start offset      reader A          high watermark = 9
     (moves on retention)   (its own state)   (next offset to be written)

partition 1  │ 0 │ 1 │ 2 │ 3 │ 4 │                   offsets are per partition,
partition 2  │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │           there is no global order
```

- **Partition = ordered, append-only, immutable log.** Every record gets the
  next **offset** in its partition. Ordering exists *only* within a partition.
- **Record** = key + value + headers + timestamp. The broker adds
  (partition, offset). The key decides the partition (task 02). Headers carry
  metadata such as trace context (task 08) and event type (task 14).
- **Reading doesn't delete.** Readers hold their own position. Ten
  independent services can read the same partition at different speeds, and
  a new one can start from the very beginning.
- **On disk** each replica keeps a directory per partition, split into
  **segments**: `<base-offset>.log` (record batches), `.index` (offset to
  byte position), `.timeindex` (timestamp to offset). Only the newest segment
  (the *active* one) is written to. A new segment is rolled when the active
  one hits `segment.bytes` (default 1 GiB) or gets older than `segment.ms`
  (default 7 days).
- **Retention** deletes **whole closed segments** from the head of the log,
  by time (`retention.ms`, default 7d) or size (`retention.bytes`, per
  partition). The **log start offset** moves forward when that happens. The
  active segment is never deleted, so "retention 30s" really means "segment
  roll interval + 30s + up to the retention check interval".
- **High watermark (HW)**: the offset up to which all in-sync replicas have
  the data. Consumers only see records below HW. Task 03 is about why.
- The time index makes **"give me the offset at 14:05"** a cheap lookup
  (`ListOffsets` with a timestamp). That is what powers replay-by-time.

## Coming from Pulsar

| Pulsar | Kafka | The difference that bites |
|---|---|---|
| Topic (optionally partitioned) | Topic, **always** partitioned (≥1) | The partition count is the max parallelism of a consumer group (task 05) |
| MessageId (ledger, entry, batch idx, partition) | (partition, offset): a plain int64 per partition | Offsets are dense and ordered, so "everything below N" is one number |
| Brokers are stateless, BookKeeper stores ledgers | Brokers store partitions on local disk, replicated broker-to-broker | Losing a broker means re-replicating its partitions (task 03) |
| Subscription cursor lives on the broker, tracks individual acks | Group's committed offset: one number per partition (task 06) | No ack "holes" |
| Retention: unacked backlog kept (backlog quota), plus a retention policy for acked data | **Time/size only.** A slow or dead consumer loses data when it falls behind retention | Story 1 above. Monitor lag in *time*, not just records |
| Reader API | Consumer **without** a group (`ConsumePartitions`, assign + seek) | Exactly the `consumer` role in this task |
| `seek(timestamp)` | `ListOffsets` by timestamp, then seek | Same capability, different API |
| Topic compaction makes a *separate* compacted view | Compaction rewrites the log itself (task 15) | Old versions are gone for everyone |

## Run it

```bash
make up                                        # once: 3 brokers + UI + Jaeger + Postgres
make dev T=01-the-log R="producer consumer"    # hot reload; Ctrl+C to stop
```

One-shot tools run from your host (same code, defaults point at
`localhost:9092-9094`):

```bash
go run ./tasks/01-the-log -role inspect
TOPIC=t01.short-retention go run ./tasks/01-the-log -role inspect
FROM=ago:30s go run ./tasks/01-the-log -role consumer
```

Knobs live in [`task.env`](task.env). Kafka UI is at http://localhost:8085.

Until you finish the TODOs, the roles exit with `not implemented yet`. That
is expected. The reference is in [`solution/`](solution/main.go), and you can
run it with `make dev T=01-the-log/solution R="producer consumer"`. Try first.

## Your task

1. **TODO(01.1) Produce.** Build the record (key = user id, two headers) and
   send it with `ProduceSync`. Log the partition and offset the broker
   assigned.
   *Done when:* the producer logs a growing offset per partition, and the same
   `user-N` always lands in the same partition.
2. **TODO(01.2) Positions.** Implement `offset:<n>` and `ago:<duration>` in
   `parseFrom`.
3. **TODO(01.3) Read.** Write the poll loop: `PollFetches`, log errors, log
   every record with partition/offset/key/timestamp.
   *Done when:* `FROM=start` replays everything, `FROM=end` shows only new
   records, and `FROM=offset:5 PARTITION=0` starts exactly at offset 5.
4. **TODO(01.4) Inspect.** Fill in start offsets, end offsets and
   offsets-by-time with `kadm`.
   *Done when:* the table shows START, END, RECORDS, the offset 1 minute ago,
   and LEADER/REPLICAS/ISR for each partition.

## Experiments

**A. Look at the producer log first.** With 5 keys and 3 partitions, one
partition probably got nothing at all. Why? Task 02 answers it.

**B. Many readers, one log.** Start two readers from the host at the same
time: one `FROM=start`, one `FROM=end`. Stop one and start it again: it starts
over from its `FROM`, because nothing about it is stored on the broker. (Task
06 adds committed offsets.)

**C. Look at the files.** Every broker holds a replica, so any broker will do:

```bash
docker compose exec kafka-1 ls -la /var/lib/kafka/data/t01.events-0/
docker compose exec kafka-1 /opt/kafka/bin/kafka-dump-log.sh --print-data-log \
  --files /var/lib/kafka/data/t01.events-0/00000000000000000000.log | head -20
```

In the dump, find `baseOffset`, `producerId`, `baseSequence`, `CreateTime`
and `compresscodec`. Where does `producerId` come from when you never
configured one, and who picked snappy? (Task 04.) Also notice that the
`.index`/`.timeindex` files are preallocated at 10 MiB.

**D. Watch retention eat the log.** Set `TOPIC=t01.short-retention` for the
producer (in `task.env`), run it for a minute, then repeat:

```bash
TOPIC=t01.short-retention go run ./tasks/01-the-log -role inspect
docker compose exec kafka-1 ls /var/lib/kafka/data/t01.short-retention-0/
```

START keeps moving forward. Segments are rolled every 10s and then deleted
(renamed to `.deleted` first, removed a minute later).

**E. Read what no longer exists.** While D is running, read an offset that
retention already deleted:
`TOPIC=t01.short-retention FROM=offset:0 PARTITION=0 go run ./tasks/01-the-log -role consumer`.
Which offset do you actually get first? Now run it again with
`KAFKA_LOG_LEVEL=info`. The only trace of what happened is an INFO line from
franz-go: `received OFFSET_OUT_OF_RANGE on the first fetch, resetting to the
configured ConsumeResetOffset`. At the default `warn` level you see nothing.
That is the mechanism behind Story 1. A group consumer whose committed offset
fell off the log takes exactly the same path.

**F. Time travel.** `FROM=ago:2m` against `t01.events`. Compare the first
offset you get with the `OFFSET 1m0s AGO` column of `inspect` (`AGO=2m`).

**G. Replicas.** `make describe TOPIC=t01.events`, then `make broker-stop B=2`
and describe again. Watch LEADER and ISR change, then
`make broker-start B=2`. Task 03 explains it.

## Edge cases and gotchas

- **No global order.** Two events for different keys can be read in a
  different order than they were produced. If you need order, it has to be
  per key (task 02).
- **Offsets aren't always contiguous.** With transactions (task 12) and
  compaction (task 15) you'll see gaps. Never compute "count = end - start"
  for business logic, and never use offsets as IDs.
- **Timestamps are producer-supplied by default** (`CreateTime`). A producer
  with a wrong clock breaks time-based lookups. Set
  `message.timestamp.type=LogAppendTime` on the topic if you need broker time.
- **Retention is per segment.** A huge `segment.bytes` on a low-traffic topic
  can keep data far longer than `retention.ms`. That matters for GDPR deletes.
- **auto-create is off** in this cluster, as it should be in prod. A typo in a
  topic name fails loudly instead of creating `paymnets` with 1 partition.

## Check yourself

<details><summary>1. A consumer reads partition 0 at offset 42. What exactly is stored on the broker about this reader?</summary>

Nothing, if it has no group. With a group, only the last committed offset for
(group, topic, partition) in `__consumer_offsets`. The broker never tracks
per-message state for classic consumers.
</details>

<details><summary>2. Retention is 7 days, but the oldest record on disk is 12 days old. Why?</summary>

Retention deletes only closed segments, and only once *all* records in a
segment are older than retention.ms (it uses the segment's largest
timestamp). A segment that stays active for a long time on a quiet topic
(1 GiB / 7 days rolling) keeps old records around.
</details>

<details><summary>3. Why can't a consumer read a record that the leader has already written?</summary>

It's above the high watermark: not yet replicated to all in-sync replicas.
If the leader died now, a new leader might not have it. Exposing it would let
consumers see data that later "un-happens".
</details>

<details><summary>4. How would you reprocess the last 3 hours for one service without affecting others?</summary>

Stop that service's group, then reset its offsets to a timestamp
(`kafka-consumer-groups --reset-offsets --to-datetime ... --execute`, or kadm
`ListOffsetsAfterMilli` + `CommitOffsets`), then start it again. Other groups
have their own offsets and don't notice. The handler must be idempotent
(task 10).
</details>

## franz-go ↔ Java client names

| franz-go | Java / docs | Note |
|---|---|---|
| `kgo.ConsumePartitions` | `consumer.assign()` + `seek()` | no group |
| `kgo.ConsumeStartOffset` | `auto.offset.reset` (first read, no commit) | franz-go default is **earliest**, Java's is **latest** |
| `kgo.ConsumeResetOffset` | `auto.offset.reset` (offset out of range) | default `RewindOffset(1m)`; below log start resumes at log start |
| `kgo.NewOffset().AfterMilli(ts)` | `offsetsForTimes()` | |
| `kadm.ListStartOffsets / ListEndOffsets` | `beginningOffsets() / endOffsets()` | |

## Further reading

- Kafka docs: [Design: the log](https://kafka.apache.org/documentation/#design_filesystem)
- Jay Kreps: [The Log: What every software engineer should know about real-time data's unifying abstraction](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying)
- franz-go: [package kgo docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo)
