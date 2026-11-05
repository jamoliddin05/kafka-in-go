# 06 · Offsets and delivery semantics

> **Goal:** know exactly which number a group stores, when franz-go writes
> it, and how the moment you commit decides whether a crash duplicates work
> or loses it. Then move a group's position by time, the way you would after
> a bad deploy.

## Why this matters in prod

**Story 1: the renamed group.** A refactor renamed the consumer group from
`billing` to `billing-v2`. The Java service used the default
`auto.offset.reset=latest`, so the new group started at the end of the log:
every payment produced between the old pods stopping and the new ones
starting was never billed, with no error anywhere. A Go service on
franz-go has the opposite default (start at the *earliest* offset), so the
same rename in a sibling team re-sent seven days of receipt emails. Same
bug, two failure modes: a new `group.id` is a new subscription with no
position.

**Story 2: the OOM kill that charged people twice.** A consumer with
autocommit called a payment provider per record. The pod was OOM-killed;
the replacement resumed from the last autocommit, up to 5 seconds and a
whole poll behind, and charged a few hundred cards again. The "fix" was to
commit before calling the provider. The next OOM kill silently dropped the
payments in flight. Neither ordering is safe on its own: the answer is
commit-after-processing plus an idempotent handler (task 10).

## Concepts

```
partition 1   … 40  41  42  43  44  45  46  47 …
                        ▲               ▲
                committed = 42      the consumer's position (in memory)
        "the next offset the group will read", not "the last one processed"

processed: 40 41 42 43 44   crash now → the group resumes at 42 → 42, 43, 44 run twice
```

- **One number per (group, topic, partition).** No per-message acks. A commit
  of 42 means "everything below 42 is done". It lives in the compacted
  internal topic `__consumer_offsets` (50 partitions; the group's coordinator
  leads the one `hash(group.id) % 50` maps to), together with the leader
  epoch, a commit timestamp and free-form metadata (franz-go writes the
  member ID there).
- **The order of commit and side effect *is* the delivery guarantee:**

  | | crash between the two | you get |
  |---|---|---|
  | commit, then process | the rest of the batch was committed but never done | **at-most-once**: loss, never duplicates |
  | process, then commit | done but not committed | **at-least-once**: duplicates, never loss |
  | atomic with the output | n/a | **exactly-once**, only for Kafka→Kafka (task 12); for a DB or HTTP call, make the side effect idempotent (task 10) |

### What franz-go's autocommit really commits (v1.22.2, `consumer_group.go`)

franz-go keeps two offsets per partition: **dirty** (one past the last
record the latest poll returned) and **head** (what may be committed).

- A goroutine commits **head** every `AutoCommitInterval` (5s), using the
  group's context, skipping partitions that didn't move. It pauses while the
  group is joining/syncing and while a `CommitOffsetsSync` is in flight.
- **Default:** at the *start* of every `PollFetches`/`PollRecords` call,
  head := dirty. So autocommit covers what *earlier* polls returned, never
  the batch you are holding right now. If you process synchronously before
  polling again, that is "processed", and default autocommit is
  at-least-once.
- **`GreedyAutoCommit`:** head := dirty as soon as a poll returns. The 5s
  tick can commit records you have not processed yet.
- **`AutoCommitMarks`:** head moves only when you call `MarkCommitRecords`
  (never backwards). You decide record by record what is done.
- **`DisableAutoCommit`:** no loop. `CommitUncommittedOffsets` commits dirty
  (everything polled); `CommitRecords` commits the records you pass (+1).
- **On revoke** (a rebalance, or `Close`/`LeaveGroup`) the default
  `OnPartitionsRevoked` makes a blocking commit of **head**, with the client's
  context. Override the callback and that commit is gone (task 05).
- **On lost** (`UNKNOWN_MEMBER_ID`, fenced): no commit, the default callback is
  empty.
- **On `Close`:** leave → revoke → that head commit → LeaveGroup. With default
  autocommit this covers everything polled *before your last poll call*. A
  loop that stops between batches without polling again leaves its last
  batch uncommitted, which is why franz-go's docs say to make one final
  synchronous commit before closing. (Java's `close()` commits everything
  returned so far, including the last poll.)
- **On a crash** (kill -9, OOM): nothing. Everything since the last
  *successful* commit is read again.

### Where a group starts

The start-offset policy is consulted **only** when a partition has no
committed offset (new group, new partition, expired offsets) or the commit
points outside the log. Java has one property for both, `auto.offset.reset`,
default `latest`. franz-go splits it:

- `kgo.ConsumeStartOffset`, no commit yet: default `NewOffset().AtStart()`,
  i.e. **earliest**. `AtEnd()` is latest; `AtCommitted()` is `none` (the
  partition errors with "partition has no prior committed offset").
- `kgo.ConsumeResetOffset`, `OFFSET_OUT_OF_RANGE`: a commit that fell below
  the log start (retention deleted it, task 01) resumes at the log start.
  When the broker lost data and the client can't tell where, the default
  `RewindOffset(time.Minute)` resumes a minute before the last consumed
  record.

### How long a commit lives

`offsets.retention.minutes` = 10080 (7 days) on Kafka 4.3.1 (`kafka-configs
--describe --entity-type brokers --entity-name 1 --all`). Since KIP-211 the
clock does **not** run while the group has members. It starts when the group
becomes `Empty`; a partition's offset also expires 7 days after its last
commit once the group no longer subscribes to that topic. Offsets of
standalone consumers (manual assignment, no membership) expire 7 days after
their last commit. The sweep runs every
`offsets.retention.check.interval.ms` (10 min). Deleting the group or the
topic removes them at once. A service switched off for 8 days comes back
with no position and falls into its start-offset policy.

### Moving a group

Reset only an **inactive** group. `kafka-consumer-groups --reset-offsets`
refuses otherwise ("Assignments can only be reset if the group ... is
inactive"), and the broker itself rejects an outside commit to a live
classic group with `UNKNOWN_MEMBER_ID` (we sent one with `FORCE=true`). The
kadm equivalent: `ListOffsetsAfterMilli` (or start/end offsets), then
`CommitOffsets`.

## Coming from Pulsar

| Pulsar | Kafka | The difference that bites |
|---|---|---|
| Individual ack; acking 1, 2, 4 redelivers 3 | One committed offset per partition | You can't say "4 is done but 3 isn't". Parallel processing inside a partition needs a contiguous watermark (task 13) |
| `negativeAcknowledge`, `ackTimeout` redelivery | Nothing. An uncommitted record is re-read only after a crash or rebalance | Retries are your code (task 09) |
| Cumulative ack | Commit offset N = everything below N | The only kind there is |
| `subscriptionInitialPosition` (default Latest) | `auto.offset.reset` (Java default latest); franz-go `ConsumeStartOffset` (default earliest) | Check which client defaults you inherit before renaming a group |
| Subscription expiration (off unless the namespace sets it) | `offsets.retention.minutes`: 7 days after the group goes empty | The position vanishes silently; the next start uses the start-offset policy |
| Cursor reset / `seek(timestamp)` on a subscription | `--reset-offsets --to-datetime` / kadm `CommitOffsets` | Only while the group is inactive |
| Unacked backlog is retained | Retention ignores commits (task 01) | A commit can point at data that no longer exists |

## Run it

```bash
make dev T=06-offsets-and-delivery R="producer consumer"     # MODE=autocommit from task.env
go run ./tasks/06-offsets-and-delivery -role offsets          # committed vs end, per partition
```

The crash experiments run from the host, one consumer per group (the
ledger that counts duplicates and losses lives inside that process):

```bash
GROUP=t06.amo MODE=at-most-once CRASH_EVERY=600 go run ./tasks/06-offsets-and-delivery -role consumer
```

`CRASH_EVERY=N` simulates `kill -9` after a random 0.5–1.5×N records: the
client's own context is cancelled (no commit, no LeaveGroup can get out), a
new client is built after `RESTART_AFTER`, and it waits up to
`SESSION_TIMEOUT` (10s here instead of 45s, task 07 is about that wait) for
the dead member to be evicted. The ledger survives, like a database would,
and logs what each restart cost:

```
msg="CRASH: simulated kill -9, no commit, no LeaveGroup" incarnation=1 processed_by_this_incarnation=629 at_partition=1 at_offset=30463
msg="resumed BEHIND what we processed: duplicates" partition=1 resumed_at=30368 last_processed=30370 reprocessing=3
msg="resumed AHEAD of what we processed: records LOST" partition=1 resumed_at=31337 last_processed=31317 skipped=19
msg=ledger processed=9348 unique=9348 duplicates=0 lost=34 crashes=16
```

`lost` counts holes below the highest processed offset; a loss at the very
tail shows up in the `exit:` lines printed on Ctrl+C (committed vs last
processed, per partition).

Resetting (dry run unless `EXECUTE=true`; stop the consumers first):

```bash
GROUP=t06.ledger TO=ago:10m go run ./tasks/06-offsets-and-delivery -role reset
GROUP=t06.ledger TO=ago:10m EXECUTE=true go run ./tasks/06-offsets-and-delivery -role reset
```

Knobs live in [`task.env`](task.env). `MODE=autocommit` and `MODE=greedy`
work out of the box; the other modes, `offsets` and `reset` exit with
`not implemented yet` until you do the TODOs. The reference is in
[`solution/`](solution/main.go):
`make dev T=06-offsets-and-delivery/solution R="producer consumer"`.

## Your task

1. **TODO(06.1) At-most-once.** Disable autocommit and commit each batch
   *before* processing it.
   *Done when:* with `CRASH_EVERY=600` the ledger reports `duplicates=0` and
   the log shows `resumed AHEAD ... records LOST` after some crashes.
2. **TODO(06.2) At-least-once.** Disable autocommit, block rebalances while a
   batch is in flight, commit after the batch, and commit in
   `OnPartitionsRevoked`.
   *Done when:* `lost=0` across many crashes, and every `reprocessing=` is at
   most one batch (`MAX_POLL_RECORDS`).
3. **TODO(06.3) Marks.** `AutoCommitMarks` + `MarkCommitRecords` after each
   processed record.
   *Done when:* `lost=0` with crashes, and a Ctrl+C prints
   `next_run_reprocesses=0` for every partition.
4. **TODO(06.4) What `Close` commits.** Give the consumer a backlog so it is
   busy (stop it, then run
   `docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:29092 --group t06.ledger --topic t06.payments --reset-offsets --by-duration PT2M --execute`),
   start it with `MODE=autocommit PROCESS_TIME=5ms`, Ctrl+C after a few
   seconds and read the `exit:` lines. Work out from franz-go's source why
   the last batch is not committed, and fix `finalCommit` for the mode(s)
   that need it.
   *Done when:* the same run prints `next_run_reprocesses=0` (we saw 100,
   one full batch, before the fix).
5. **TODO(06.5) Offsets.** Fill committed, start and end offsets.
   *Done when:* `-role offsets` shows LOG-START, COMMITTED, END and LAG, and
   LAG matches `make group G=t06.ledger`.
6. **TODO(06.6) Reset.** Refuse a group with members, compute the target
   offsets for `start | end | ago:<d>`, commit them.
   *Done when:* the dry run shows `re-process N` per partition, `EXECUTE=true`
   moves the group (check with `-role offsets`), and running it against a
   live consumer fails with a clear message.

## Experiments

**A. The crash matrix.** One consumer per mode, each in its own group,
started at the end of the topic, same load, for 4 minutes:

```bash
go build -o /tmp/t06 ./tasks/06-offsets-and-delivery          # or ./tasks/06-offsets-and-delivery/solution
RATE=40 /tmp/t06 -role producer > /tmp/t06-producer.log 2>&1 &  # the only producer running
for m in autocommit greedy marks at-most-once at-least-once; do
  GROUP=t06.m-$m TO=end EXECUTE=true /tmp/t06 -role reset > /dev/null
  GROUP=t06.m-$m MODE=$m CRASH_EVERY=600 PROCESS_TIME=10ms SESSION_TIMEOUT=6s \
    /tmp/t06 -role consumer > /tmp/t06-$m.log 2>&1 &
done
sleep 240; pkill -INT -f '/tmp/t06 -role'; sleep 10
grep 'final ledger' /tmp/t06-*.log
```

On this laptop (40 payments/s, 10ms per payment, 15–20 crashes per mode):

| MODE | processed | duplicates | lost | one crash cost |
|---|---|---|---|---|
| autocommit | 11 547 | 3 714 | 0 | re-read up to ~300 per partition: everything since the last 5s tick |
| greedy | 11 352 | 2 179 | 40 | mostly re-reads, sometimes `skipped=2..19`: the tick had committed records still being processed |
| marks | 11 775 | 4 340 | 0 | like autocommit: bounded by the interval, not by the batch |
| at-most-once | 9 348 | 0 | 34 (+7 at the tail) | `skipped=1..11`: the rest of the batch |
| at-least-once | 9 543 | 38 | 0 | `reprocessing=2..6`: the processed part of the batch |

Which numbers would change if the handler took 200ms per record? Which
mode would you put in front of a payment provider?

**B. The commit interval.** Repeat autocommit with `AUTOCOMMIT_INTERVAL=1s`.
On this laptop: ~40 duplicates per crash instead of ~190 at 5s. The window
shrinks; it never closes.

**C. A crash loop that makes no progress.** Reset a group to `ago:3m` (a big
backlog), then run `MODE=autocommit CRASH_EVERY=300 SESSION_TIMEOUT=6s` and
watch `-role offsets`. Each incarnation processes a few hundred records and
dies before its first useful autocommit, so COMMITTED barely moves: in 14
crashes ours processed 4 725 records, 3 152 of them duplicates. The same run
with `MODE=at-least-once` processed 4 121, 624 duplicates, and caught one
partition up. This is what a pod that OOMs 20 seconds after every start
looks like.

**D. The renamed group.** Start new groups with `START=latest`,
`START=earliest` and `START=none` for 10 seconds each, then `-role offsets`
for each. `latest` committed right at the end (everything before it is
skipped for this group forever); `earliest` started at offset 0 of the whole
retention window; `none` logged `partition has no prior committed offset`
and processed nothing. How would you rename a group safely? (Stop the old
one, reset the new one to the old one's offsets, or `--to-datetime` of the
switch, then start it.)

**E. Replay after a bad deploy.** Stop the consumer, `TO=ago:10m` as a dry
run, then `EXECUTE=true`, then start the consumer again and watch LAG drain.
Now try the same with the consumer running and `FORCE=true EXECUTE=true`: the
broker answers `UNKNOWN_MEMBER_ID`. The CLI version:

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:29092 \
  --group t06.ledger --topic t06.payments --reset-offsets --by-duration PT10M --dry-run
```

`--execute` instead of `--dry-run` applies it; `--to-datetime`, `--shift-by`,
`--to-offset`, `--to-earliest`, `--to-latest` are the other targets.

**F. Look inside `__consumer_offsets`.** `t06.ledger` lives in partition 3
(Java's `"t06.ledger".hashCode()` mod 50):

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:29092 \
  --topic __consumer_offsets --partition 3 --offset earliest --timeout-ms 5000 \
  --formatter org.apache.kafka.tools.consumer.OffsetsMessageFormatter | grep -o '{"key":[^}]*"t06.ledger"[^}]*}},"value":{[^}]*}}}'
```

Each commit is a record keyed by (group, topic, partition) with `offset`,
`leaderEpoch`, `commitTimestamp` and `metadata` (the member ID for franz-go
commits, empty for our reset). The 4.3 formatter prints records without
newlines, hence the `grep -o`. Why does compaction suit this topic?

## Edge cases and gotchas

- **Commit offset+1.** The committed offset is the next one to read.
  `CommitRecords` adds the +1 for you; raw offset APIs (kadm `CommitOffsets`,
  Java `commitSync(Map)`) do not. Commit `r.Offset` and the last record is
  processed again after every restart.
- **Async processing + autocommit = loss.** Hand records to goroutines and
  poll again, and the next poll promotes the previous batch to head: it gets
  committed while the goroutines are still working. Use marks, or commit a
  contiguous watermark yourself (task 13).
- **Commits can go backwards.** `CommitRecords` with an older record rewinds
  the commit. After a rebalance, a slow member can commit for a partition it
  no longer owns and rewind the new owner's progress; `BlockRebalanceOnPoll`
  prevents that, and KIP-848 rejects a commit with a stale member epoch.
- **A failed commit may have landed.** A timeout or cancelled context doesn't
  tell you the broker didn't write it. At-most-once code must treat a failed
  commit as "possibly done".
- **Autocommit errors are only logged** by the default callback ("default
  commit failed"). Set `kgo.AutoCommitCallback` and alert on them.
- **The start-offset policy also fires 7 days after a group goes empty**, and
  for partitions added to the topic later. "We never had this problem in
  staging" usually means staging consumers never sat idle for a week.
- **franz-go drains one buffered fetch at a time.** `PollRecords(ctx, 100)`
  takes from the first buffered fetch response (one per broker) until it is
  empty, so during a big catch-up some partitions wait while others drain.
  In Experiment C one partition never got processed between crashes.
- **Ctrl+C right after a simulated crash takes up to `SESSION_TIMEOUT`.**
  `Close` waits for the rebalance that evicts the dead member before it can
  leave.

## Check yourself

<details><summary>1. A consumer processed offsets 0–99 of partition 0 and committed. What number is stored, and where does a restart begin?</summary>

100, and it begins at 100. The committed offset is the next offset to read.
</details>

<details><summary>2. With franz-go's default autocommit and synchronous processing, can a crash lose a record? Can a clean Close?</summary>

A crash cannot lose one: autocommit only covers what earlier polls returned,
and those were processed before the next poll. It re-reads everything since
the last successful commit. A clean `Close` can't lose one either, but it
commits only what was polled before your last poll call, so a loop that
stops between batches re-processes its last batch unless it commits once
more before closing. Loss appears with async processing or
`GreedyAutoCommit`.
</details>

<details><summary>3. Why does at-least-once with a commit after every batch still produce duplicates?</summary>

The crash can land after some records of the batch were processed and before
the commit. Those are re-read. The bound is one batch, which is why
`MAX_POLL_RECORDS` is also a duplicate budget.
</details>

<details><summary>4. A group has been empty for 8 days. What happens when its consumers start?</summary>

Its offsets expired after 7 days (`offsets.retention.minutes`, counted from
the moment the group became empty). Every partition has no commit, so the
start-offset policy decides: Java's default jumps to the end and skips
everything in between; franz-go's default re-reads the whole retention
window.
</details>

<details><summary>5. You need to reprocess the last hour for one service while it keeps running. What exactly do you do?</summary>

You can't while it runs: the CLI refuses and the broker rejects outside
commits to a live group. Scale that service to zero (all members leave, the
group goes `Empty`), reset by time (`--by-duration PT1H` / kadm
`ListOffsetsAfterMilli` + `CommitOffsets`), scale back up. Other groups are
untouched. The handler must be idempotent (task 10).
</details>

## franz-go ↔ Java client names

| franz-go | Java / docs | Note |
|---|---|---|
| default autocommit, `kgo.AutoCommitInterval` (5s) | `enable.auto.commit=true`, `auto.commit.interval.ms` (5000) | Java commits inside `poll()` and in `close()` |
| `kgo.DisableAutoCommit` | `enable.auto.commit=false` | |
| `kgo.GreedyAutoCommit` | (none) | |
| `kgo.AutoCommitMarks` + `cl.MarkCommitRecords` | (none) | closest: `commitAsync` of offsets you track yourself |
| `cl.CommitUncommittedOffsets` / `cl.CommitRecords` | `commitSync()` / `commitSync(Map)` | |
| `cl.CommitOffsets` | `commitAsync` | |
| `kgo.ConsumeStartOffset` (default `AtStart`) | `auto.offset.reset` when there is no commit (default `latest`) | |
| `kgo.ConsumeResetOffset` (default `RewindOffset(1m)`) | `auto.offset.reset` on out-of-range | |
| `kgo.NewOffset().AtCommitted()` | `auto.offset.reset=none` | |
| `cl.PollRecords(ctx, n)` | `max.poll.records` | |
| `kgo.AutoCommitCallback` | `OffsetCommitCallback` | |
| kadm `FetchOffsets` / `CommitOffsets` | `Admin.listConsumerGroupOffsets` / `alterConsumerGroupOffsets` | |
| kadm `ListOffsetsAfterMilli` | `offsetsForTimes()` | |

## Further reading

- Kafka docs: [Message delivery semantics](https://kafka.apache.org/documentation/#semantics), [Consumer configs](https://kafka.apache.org/documentation/#consumerconfigs), [Broker configs](https://kafka.apache.org/documentation/#brokerconfigs) (`offsets.retention.minutes`)
- [KIP-211: Revise Expiration Semantics of Consumer Group Offsets](https://cwiki.apache.org/confluence/display/KAFKA/KIP-211%3A+Revise+Expiration+Semantics+of+Consumer+Group+Offsets)
- franz-go: [package kgo](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo) (`ConsumerGroup`, `GreedyAutoCommit`, `AutoCommitMarks`, `ConsumeStartOffset`) and [package kadm](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kadm)
- [Kafka protocol guide](https://kafka.apache.org/protocol) (OffsetCommit / OffsetFetch)
