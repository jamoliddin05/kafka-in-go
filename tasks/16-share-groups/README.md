# 16 · Share groups

> **Goal:** use Kafka 4's queues (KIP-932). In a share group the broker
> hands out individual records, locks each one to a consumer, and tracks
> whether it was accepted, released for another try, or rejected. That's
> the closest Kafka gets to Pulsar's Shared subscription. Learn what it
> gives you, what it costs (order, duplicates), and when a plain consumer
> group is still the right tool.

## Why this matters in prod

**Story 1: thirty pods, six partitions.** A thumbnail service read
`uploads` (6 partitions) with a consumer group. Each job took 0.2–20 seconds
depending on the image. During a marketing campaign the backlog grew, the
autoscaler went to 30 pods, and 24 of them sat idle: a consumer group never
gives a partition to more than one member. Meanwhile one 20-second video
blocked every upload queued behind it in its partition. Repartitioning a
live topic wasn't an option (task 02). Independent jobs with uneven cost are
exactly what share groups are for: many consumers per partition, and a slow
record doesn't block the ones after it.

**Story 2: every email sent twice.** The team moved to a share group and
kept the defaults. franz-go asks for up to 500 records per fetch, the broker
locks each of them for 30 seconds, and each email took about 200 ms. After
30 seconds the 150th email was being sent while the broker had already
handed the remaining ~350 to other consumers. Those were sent again, and the
late acks from the first consumer were rejected or silently ignored. **The
acquisition lock is a budget**: records per fetch × time per record has to
fit well inside it, and the handler must be idempotent anyway (task 10).

## Concepts

```
t16.jobs partition 0, as the share group t16.workers sees it

        all done              in flight (tracked per record)            not fetched yet
... 2348 2349 2350 │ 2351  2352  2353  2354  2355  2356  2357 │ 2358 2359 ...
                   │ ACK   ACQ   AVL   ACQ   ARC   ACQ   AVL  │
                   ▲ SPSO: share-partition start offset       ▲ end of the in-flight window
     ACK acknowledged, ACQ acquired (locked to one consumer), AVL available, ARC archived
```

Per-record state machine on the broker:

```
              ShareFetch acquires it, lock = share.record.lock.duration.ms (30s)
 AVAILABLE ───────────────────────────────────────────────────────────► ACQUIRED
     ▲                                                                     │
     │  RELEASE, lock expired, session closed, member left                 ├── ACCEPT ─► ACKNOWLEDGED
     └──────────────────── delivery count + 1 ─────────────────────────────┤
                         (unless this was delivery number                  ├── REJECT ─► ARCHIVED
                          share.delivery.count.limit (5): then ARCHIVED)   │
                                                                           └── RENEW: same state, new lock (4.2+)
```

- **The broker assigns partitions to members** (server-side assignor
  `simple`). With more members than partitions, several members share a
  partition; with fewer, each gets a subset. A member only fetches its
  assigned partitions. Observed on this cluster with 2 partitions: 1 member
  gets both, 2 members get one each, with 3 members one of them gets both,
  and 6 members get one each (3 per partition).
- **Inside a partition, records are handed out individually.** Consumers
  sharing a partition get different records; the broker keeps up to
  `group.share.partition.max.record.locks` (2000) records in flight per
  partition.
- **Acknowledgement types:** `ACCEPT` (done), `RELEASE` (give it back now,
  delivery count +1), `REJECT` (unprocessable: archive it, never redeliver),
  `RENEW` (extend the lock without finishing, KIP-1222, Kafka 4.2+, allowed
  by the group config `share.renew.acknowledge.enable=true`).
- **Delivery count** comes with every record (`Record.DeliveryCount()`, 1 on
  the first delivery). When a record that was delivered
  `share.delivery.count.limit` times (default 5, broker bounds 2–10) comes
  back, it's archived.
- **Archived means invisible, not deleted.** The data stays in the topic
  until retention; the group just stops delivering it. Kafka 4.3.1 has **no
  dead-letter queue for share groups**: KIP-1191 (accepted) designs one,
  but there is no config for it on this cluster
  (`kafka-configs ... --entity-type brokers --all | grep -i dead` finds
  nothing). Build your own: on the last attempt, produce the record to a DLQ
  topic, then ACCEPT.
- **No ordering.** Not across partitions, not within a partition, not per
  key. A released record comes back after newer ones; two records with the
  same key run at the same time on two consumers.
- **At-least-once, always.** Redelivery happens on lock expiry while the
  first consumer may still be working, and on any session loss.
- **Settings live on the group, not in the client.** `share.auto.offset.reset`
  (default `latest`), `share.record.lock.duration.ms`,
  `share.delivery.count.limit`, `share.session.timeout.ms`,
  `share.isolation.level` (default `read_uncommitted`) are *group configs*:
  resource type GROUP in `IncrementalAlterConfigs`, or `kafka-configs
  --entity-type groups`. Unset ones fall back to the broker's
  `group.share.*` defaults. Changes apply to a running group (a 15 s lock
  took effect without restarting anything).
- **Where a new group starts.** `share.auto.offset.reset` is applied when
  the broker initializes a share-partition that has no persisted state.
  A brand-new group therefore skips everything already in the topic. Setting
  `earliest` later does **not** move partitions that already have state;
  `kafka-share-groups.sh --reset-offsets` does, and only while the group has
  no members.

What happens to records a consumer holds, as measured on this cluster:

| The consumer... | Its records come back | Observed |
|---|---|---|
| ACKs RELEASE | at once, delivery count +1 | next delivery within milliseconds, often on another consumer |
| shuts down (`Close`) mid-batch | at once: franz-go releases everything polled but not acked | 0.5 s, delivery 2 |
| crashes (process gone, so the TCP connection closes) | at once: the broker drops the share session with the connection | 0.7 s after `os.Exit`, delivery 2 |
| ... and was the only member on that partition | when its membership expires (`share.session.timeout.ms`, 45 s) and the partition is reassigned | 44 s, the partition's lag grew meanwhile |
| is frozen or stuck with the connection open (GC pause, `SIGSTOP`, deadlock, network black hole) | when the lock expires, counted from the **fetch**, not from your poll | ~15 s after the fetch with a 15 s lock |

## Coming from Pulsar

| Pulsar | Kafka | The difference that bites |
|---|---|---|
| Shared subscription | Share group | Partitions are still assigned to members. A member that hangs while alone on a partition stalls it |
| Individual ack | `ACCEPT` per record | franz-go accepts every record you didn't ack at your next poll. A forgotten ack is a success |
| `negativeAcknowledge` (redelivered after `negativeAckRedeliveryDelay`) | `RELEASE` | Redelivered immediately, no delay: a flaky dependency burns all 5 attempts in seconds |
| `ackTimeout` (client side, off by default) | Acquisition lock (broker side, always on, 30 s) | Counts from the fetch, including records the client prefetched but you haven't polled |
| `redeliveryCount` | `Record.DeliveryCount()` | Same idea |
| `DeadLetterPolicy` (`maxRedeliverCount`, DLQ topic) | `share.delivery.count.limit` → ARCHIVED | No DLQ topic in 4.3. Archived records silently stop |
| Key_Shared | **No equivalent** | Per-key order means a consumer group (+ task 13's per-key workers) |
| `subscriptionInitialPosition` (client) | `share.auto.offset.reset` (group config, default `latest`) | Set on the broker; doesn't move partitions that already have state |
| Subscription backlog | `kafka-share-groups.sh --describe`: START-OFFSET and LAG | |

## Run it

```bash
make dev T=16-share-groups R="producer consumer"     # hot reload; Ctrl+C to stop
make scale T=16-share-groups R=consumer N=6          # six consumers, two partitions
make logs R=consumer
```

One-shot tools from your host:

```bash
go run ./tasks/16-share-groups -role inspect                    # members, assignment, start offset, lag
go run ./tasks/16-share-groups -role config                     # share.auto.offset.reset=earliest
DELIVERY_LIMIT=3 LOCK_MS=15000 go run ./tasks/16-share-groups -role config
STALL_PCT=100 go run ./tasks/16-share-groups -role consumer     # an extra, misbehaving consumer
```

The CLI view of the same group:

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-share-groups.sh --bootstrap-server kafka-1:29092 --describe --group t16.workers
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-share-groups.sh --bootstrap-server kafka-1:29092 --describe --group t16.workers --members
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:29092 --describe --entity-type groups --entity-name t16.workers --all | grep share
```

Knobs live in [`task.env`](task.env). The starter runs: it joins the share
group and processes jobs, but **never acks**, and that is the first bug to
look at. `config` exits with `not implemented yet` until TODO(16.1). The
reference is in [`solution/`](solution/main.go):
`make dev T=16-share-groups/solution R="producer consumer"`.

## Your task

1. **TODO(16.1) Group config.** In the `config` role, send an
   `IncrementalAlterConfigsRequest` (kmsg; kadm has no helper for group
   configs) with one resource of type `ConfigResourceTypeGroupConfig` named
   after the group and one `Set` entry per key. Check each resource's error
   code, then call `printGroupConfigs`.
   *Done when:* `go run ./tasks/16-share-groups -role config` prints
   `share.auto.offset.reset  earliest  GROUP_CONFIG`, and
   `DELIVERY_LIMIT=3` shows up the same way.

2. **TODO(16.2) Acknowledge.** Before that, run the starter for a minute and
   grep the consumer logs: `job failed` appears, but every line says
   `delivery=1`, and `inspect` (or the CLI) shows LAG 0. Every failed job
   was **accepted** at the next poll. In one run 21 of 40 jobs failed and
   none was retried. Now ack each record: `AckAccept` on success,
   `AckRelease` on a transient error, `AckReject` on poison.
   *Done when:* flaky jobs reappear with `delivery=2`, `3`, ..., poison jobs
   appear once with `ack=reject`, and no job ever shows `delivery=6`.

3. **TODO(16.3) Ack callback.** Add `kgo.ShareAckCallback` and log every
   result with a non-nil `Err`.
   *Done when:* in Experiment F the stalled consumer logs `ack failed` with
   `SHARE_SESSION_NOT_FOUND` after it wakes up.

4. **TODO(16.4) Inspect.** Fill `g` with `adm.DescribeShareGroups` and
   `offsets` with `adm.DescribeShareGroupOffsets`.
   *Done when:* with 6 consumers the MEMBER table lists six members, each
   with one partition, three per partition.

## Experiments

**A. A new group starts at "latest".** With jobs already in `t16.jobs`, start
a consumer in a brand-new group: `GROUP=t16.fresh go run
./tasks/16-share-groups -role consumer`. It processes nothing until the
producer writes something new; `GROUP=t16.fresh ... -role inspect` shows
START OFFSET -1 (no state yet) for its partitions. Now stop it, run
`GROUP=t16.fresh go run ./tasks/16-share-groups -role config` and start it
again. In our run, the partition that already had state stayed where it was,
and the one that had none was read from offset 0. To rewind both:

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-share-groups.sh --bootstrap-server kafka-1:29092 \
  --reset-offsets --to-earliest --group t16.fresh --topic t16.jobs --execute     # group must be empty
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-share-groups.sh --bootstrap-server kafka-1:29092 \
  --delete --group t16.fresh                                                      # clean up
```

**B. More consumers than partitions.** `make scale T=16-share-groups
R=consumer N=6`, then `inspect`. Count processed jobs per replica in
`make logs R=consumer` (in one 20-second run, six host-run consumers handled
73 to 101 deliveries each). Scale to 1 and back to 6 and watch the ASSIGNED
column. Why does each member get only one partition?

**C. Redelivery and the delivery limit.** Pick a flaky job with `fails 5
times` or more in the logs and follow its key: deliveries 1 to 5, often on
different consumers, and never a 6th. With a single consumer, all five
attempts of one job took about 6 seconds. Then `DELIVERY_LIMIT=3 go run
./tasks/16-share-groups -role config` and repeat: nothing goes past
`delivery=3`. What would a DLQ need to know to catch these jobs before they
disappear?

**D. A crash.** With six consumers running, start one more from the host
that crashes on its first record:

```bash
CRASH_PCT=100 INSTANCE=crasher go run ./tasks/16-share-groups -role consumer
docker compose logs consumer | grep 'offset=490 '     # the offset from its "holding=" log line
```

In our run `holding=0/490` was processed by another consumer with
`delivery=2` 0.7 seconds later: the connection closed, and the broker
released the records at once. Now stop all consumers, run two from the host
(`INSTANCE=a` and `INSTANCE=b WORK=3s`), and `kill -9` the slow one right
after it polls. Its partition (it was the only member on it) sat untouched
for 44 seconds, until `share.session.timeout.ms` removed the dead member.

**E. A frozen consumer.** Run a slow consumer from the host and freeze it
instead of killing it, so its connection stays open:

```bash
INSTANCE=frozen WORK=1s LOG_LEVEL=debug go run ./tasks/16-share-groups -role consumer &
kill -STOP <pid>; sleep 40; kill -CONT <pid>      # or: docker pause / unpause a compose replica
```

The records it held (`polled records=...` in its debug log) come back on
other consumers when the lock expires. Then it wakes up and **processes them
again**: duplicates, and its acks fail with `SHARE_SESSION_NOT_FOUND` /
`INVALID_SHARE_SESSION_EPOCH`. Compare the redelivery time with the time of
its *fetch*: records the client had prefetched were already burning their
lock before the application ever saw them.

**F. A stuck handler.** `STALL_PCT=100 go run ./tasks/16-share-groups -role
consumer` holds its records for 45 s without polling. They are redelivered
after about 30 s (both the 30 s lock and franz-go's 30 s idle-connection
timeout end it). When it wakes up, it finishes the stale job and its ack
fails (`SHARE_SESSION_NOT_FOUND`). With `LOCK_MS=15000` and `STALL=20s` one
run went further: the stale ack came back **without any error**, because
another consumer had already accepted the record. What does that tell you
about relying on ack errors to detect duplicates?

**G. Graceful shutdown.** Ctrl+C a consumer in the middle of a job. The job
it was on and the rest of its batch are released by `Close` and show up on
another consumer with `delivery=2` within a second.

## Edge cases and gotchas

- **A missing ack is a success.** franz-go accepts everything from the
  previous poll that you didn't ack when you call `PollRecords` again (the
  Java client does the same in its default `implicit` acknowledgement mode).
  An error path that `continue`s without `Ack` loses the record.
- **Budget the lock.** `ShareMaxRecords` (default 500) × time per record,
  plus what the client prefetched, must fit inside
  `share.record.lock.duration.ms` with room to spare. The broker may return
  more than `ShareMaxRecords` to avoid splitting a record batch, unless you
  set `ShareMaxRecordsStrict` (KIP-1206, Kafka 4.2+). For one long job use
  `AckRenew`. The lock can only be configured between 15 and 60 s.
- **Duplicates are guaranteed, and silent.** Lock expiry, a dropped
  connection, a leader change: the record goes to someone else while the
  first consumer may still finish it. The late ack may fail or may come back
  clean (Experiment F). Make the handler idempotent (task 10).
- **RELEASE has no backoff.** A transient error against a dead dependency
  burns all attempts in seconds and archives the job. Back off before
  releasing (while the lock lasts), or move the job to a retry topic
  (task 09).
- **A stuck member can stall a partition.** Records are only fetched by the
  partition's assigned members. With few members per partition, a hung
  consumer holds its share of in-flight records until the lock expires, and
  a dead one holds the partition until `share.session.timeout.ms`.
- **There is no DLQ (4.3.1).** Archived records leave no trace in the group.
  If you need them: check `DeliveryCount()` against the limit you
  configured, produce to your own DLQ on the last attempt, then ACCEPT.
- **No ordering, no per-key anything.** Don't use a share group for events
  whose order matters per entity (state machines, balances). Use a consumer
  group and task 13's per-key worker pool.
- **Changing `share.auto.offset.reset` doesn't rewind a running group.**
  Use `kafka-share-groups.sh --reset-offsets` with the group empty.
- **Hold records only while you poll.** The broker ties the share session to
  one connection. If you stop polling for longer than franz-go's
  `ConnIdleTimeout` (30 s), the idle connection is closed and everything you
  hold is released.
- **START-OFFSET trails.** The share-partition start offset is persisted
  lazily and can lag a few records behind; alert on LAG.
- **Limits:** at most `group.share.max.size` (200) members per group.

## When to use which

| You need | Use |
|---|---|
| Per-key order, replay by offset, consume-transform-produce exactly-once (task 12) | Consumer group |
| More parallelism than partitions **and** per-key order | Consumer group + per-key worker pool (task 13) |
| Independent jobs, uneven processing time, more workers than partitions, per-record retry/reject | Share group |
| A queue where jobs must never run twice | Neither alone: idempotent handler (task 10) with either |

## Check yourself

<details><summary>1. Six consumers in a share group, topic with 2 partitions. Who reads what?</summary>

The broker's `simple` assignor gives each member one partition, three
members per partition (verified with `inspect`). Inside a partition the
three get different records, handed out one by one. All six are busy, which
a consumer group could never do with 2 partitions.
</details>

<details><summary>2. Your handler returns an error and the code path forgets to ack. What happens to the record?</summary>

It's accepted at the next `PollRecords`, exactly like a success. No retry,
no error, LAG 0. Every path must end in an explicit ACCEPT, RELEASE or
REJECT.
</details>

<details><summary>3. One consumer is SIGKILLed while holding 5 records; another freezes in a 60 s GC pause. When are the records redelivered?</summary>

The killed one's connection closes, so the broker releases its records right
away (if other members share the partition; otherwise the partition waits
for `share.session.timeout.ms`). The frozen one keeps its connection: its
records are redelivered when the lock expires, 30 s after they were fetched.
When it wakes up it processes them again and its acks fail or are ignored.
</details>

<details><summary>4. When is a share group the wrong choice?</summary>

When order matters per key, when you rely on replaying exact offsets per
consumer, when you need Kafka transactions around consume and produce, and
when you'd need Key_Shared semantics. Also when a job must never run twice
and the handler can't be made idempotent.
</details>

<details><summary>5. How do you get a DLQ on Kafka 4.3?</summary>

Build it: know the delivery limit you configured, and when
`DeliveryCount()` reaches it and the job still fails, produce the record (plus
error and attempt count in headers) to a DLQ topic, then ACCEPT. REJECT
poison immediately, or send it to the DLQ the same way. KIP-1191 will make
the broker do this, but 4.3.1 has no config for it.
</details>

## franz-go ↔ Java client names

| franz-go | Java / docs | Note |
|---|---|---|
| `kgo.ShareGroup(g)` | `KafkaShareConsumer` with `group.id` | |
| `kgo.ShareMaxRecords(n)` | `max.poll.records` | soft limit |
| `kgo.ShareMaxRecordsStrict()` | `share.acquire.mode=record_limit` | default `batch_optimized` |
| `r.Ack(kgo.AckAccept / AckRelease / AckReject / AckRenew)` | `acknowledge(record, AcknowledgeType.ACCEPT / RELEASE / REJECT / RENEW)` | |
| auto-accept of un-acked records at the next poll | `share.acknowledgement.mode=implicit` (default) | franz-go always does this |
| `cl.MarkAcks(status)` (no records) | none | marks everything not yet acked from the last poll |
| `cl.FlushAcks(ctx)` | `commitSync()` | otherwise sent within ~1 s or with the next fetch |
| `kgo.ShareAckCallback` | `setAcknowledgementCommitCallback` | |
| `r.DeliveryCount()` | `ConsumerRecord.deliveryCount()` | |
| `r.AcquisitionDeadline()` | `acquisitionLockTimeoutMs()` | |
| `kadm.DescribeShareGroups` / `DescribeShareGroupOffsets` | `Admin.describeShareGroups` / `listShareGroupOffsets`, `kafka-share-groups.sh --describe` | |
| `kadm.AlterShareGroupOffsets` | `Admin.alterShareGroupOffsets`, `kafka-share-groups.sh --reset-offsets` | group must be empty |
| group config via `IncrementalAlterConfigs` (GROUP) | `kafka-configs.sh --entity-type groups` | `share.*` keys |

## Further reading

- [KIP-932: Queues for Kafka](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)
- [KIP-1222: Acquisition lock timeout renewal in share consumer explicit mode](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1222:+Acquisition+lock+timeout+renewal+in+share+consumer+explicit+mode)
- [KIP-1206: Strict max fetch records in share fetch](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1206:+Strict+max+fetch+records+in+share+fetch)
- [KIP-1191: Dead-letter queues for share groups](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1191:+Dead-letter+queues+for+share+groups) (accepted, not in 4.3.1)
- Kafka docs: [Configuration](https://kafka.apache.org/documentation/#configuration), see the broker configs `group.share.*` and the group configs `share.*`
- franz-go: [package kgo docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo) (`ShareGroup`, `Record.Ack`, `AckStatus`)
