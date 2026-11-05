# 15 · Compaction and state

> **Goal:** use a topic as a table. A compacted topic keeps the latest value
> per key, a tombstone deletes a key, and a service rebuilds its in-memory
> state from the log on every start and only then says it is ready. The full
> event log stays next to it, so any projection can be rebuilt from scratch.

## Why this matters in prod

**Story 1: ready with half a table.** A pricing service loaded its price list
from a compacted topic in a background goroutine and started its HTTP server
right away. The readiness probe checked "is the server up", so Kubernetes
routed traffic to every new pod within a second of starting. During each
rolling deploy the new pods answered "no price for this SKU" for about 40
seconds and the checkout fell back to a default price. Nobody noticed for a
week, because the pods were "healthy". The fix was one rule: **ready means
"I have read everything that existed when I started"**, not "my process is
up".

**Story 2: the customer who came back.** A user asked for their account to be
deleted. The service wrote a tombstone to the compacted `customers` topic,
and every consumer dropped the row, except a search indexer that had been
switched off for a migration. It kept its own snapshot plus offsets and
resumed from them three days later. `delete.retention.ms` was the default
24 hours, so the cleaner had long since removed the tombstone. The indexer
never saw the delete, and the deleted profile stayed searchable. **A consumer
that can be away longer than `delete.retention.ms` must rebuild from the
beginning, not resume.**

## Concepts

```
t15.customers partition 0, before cleaning               │ active segment
offset   0    1    2    3    4    5    6    7    8       │  9    10
key      A    B    A    C    B    A    D    C    B       │  A    B
value    a1   b1   a2   c1   b2   a3   d1   ∅    b3      │  a4   b4
                                          ∅ = tombstone

after a cleaning pass over the closed segments (offsets 0..8)
offset                       5    6    7    8            │  9    10
key                          A    D    C    B            │  A    B
value                        a3   d1   ∅    b3           │  a4   b4
   offsets keep their numbers: 0..4 are simply gone (gaps)
   c1 is gone, the tombstone for C stays for delete.retention.ms
   a3 and b3 survive this pass: a4/b4 sit in the active segment, which the cleaner ignores
```

- **`cleanup.policy=compact`** makes the log cleaner keep *at least* the
  latest record per key. It runs in the background, so at any moment a reader
  can see several versions of a key. The only guarantee is that the latest
  one is never lost.
- **Only closed segments are cleaned.** The active segment is never
  touched. A segment closes when it reaches `segment.bytes` (1 GiB default)
  or `segment.ms` (7 days default) old, and that check happens **on the next
  append**. A partition that stops receiving writes keeps its last segment
  exactly as it is: dirty values and tombstones included (Experiment D).
- **When cleaning starts:** the cleaner (1 thread by default,
  `log.cleaner.backoff.ms=15000` between checks when idle) picks the log
  with the highest *dirty ratio* (bytes not yet cleaned / total bytes) above
  `min.cleanable.dirty.ratio` (default 0.5). `min.compaction.lag.ms`
  (default 0) protects records younger than that from being compacted, so
  every consumer gets a window to see every update. `max.compaction.lag.ms`
  (default effectively infinite) is a deadline: older dirty records get
  cleaned even below the dirty ratio, and the active segment is rolled once
  its first record is older than that, but again only **on the next
  append**. Verified: with `max.compaction.lag.ms=10000` and no writes, all
  20 versions of a key were still there after 80 s; one more write rolled
  the segment and the cleaner dropped 19 of them.
- **Tombstone** = a record with a key and a **null** value. On the first
  pass it removes all older values of the key and gets a *delete horizon*
  (you'll see `deleteHorizonMs` in `kafka-dump-log`). A later pass, after
  `delete.retention.ms` (24 h default), removes the tombstone itself.
  Readers that are offline longer than that never learn about the delete.
- **Every replica compacts on its own.** The same partition can be 17 KiB on
  one broker and 68 KiB on another a few seconds later. Which superseded
  versions you see depends on which replica you read from.
- **The log start offset doesn't move.** START stays 0; compaction leaves
  gaps instead. `END - START` is not a record count.
- **`cleanup.policy=compact,delete`** does both: latest value per key, and
  segments older than `retention.ms` are deleted anyway. Good for "latest
  state, but only for keys active in the last 7 days".
- **Where Kafka itself uses it:** `__consumer_offsets` and
  `__transaction_state` are compacted (check with `kafka-configs
  --describe`; `__share_group_state` from task 16 is not). So are Kafka
  Connect's config/offset/status topics and Kafka Streams' changelog topics,
  which are what a KTable is made of.

**Bootstrapping state from a compacted topic:**

```
start ──► ListEndOffsets: target = {0: 4611, 1: 4761, 2: 3681}   the finish line, frozen
      ──► assign every partition at its start offset             no group, explicit offsets
      ──► apply records; /readyz = 503 {"offsets_left": ...}
      ──► every partition has delivered offset target-1          /readyz = 200
      ──► keep consuming: the table follows the topic            eventually consistent
```

The target is captured **before** reading, because on a busy topic the high
watermark keeps moving and "caught up with the current end" may never be
true. There's **no consumer group**: every replica of the service needs the
whole table, not a share of the partitions, and committed offsets mean
nothing for a map that is rebuilt on each start. A service that keeps a local
snapshot (RocksDB, a file) stores the offsets *with the snapshot* and
assigns partitions at those offsets instead of at the start, which is what
Kafka Streams does with its checkpoint file. That only works if it was not
away longer than `delete.retention.ms` (Story 2).

**Event log vs snapshot.** `t15.customer-events` (delete policy,
`retention.ms=-1`) is the source of truth: every change, in order. The
compacted `t15.customers` is a derived snapshot: cheap to load, but the
history is gone. **Never compact a topic whose intermediate events matter**
(orders, payments, anything you'd want to replay into a new read model).
Rebuilding a projection = reading the event log from the beginning with a new
reader or a **new consumer group** (`auto.offset.reset=earliest`). That is
how you deploy *blue/green projections*: v2 builds next to v1 under its own
group id into its own table, you switch reads when its lag is near zero, then
delete v1 and its group. Keeping years of events on brokers is what tiered
storage ([KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage),
`remote.storage.enable=true` per topic) is for: closed segments move to object
storage, consumers read them transparently.

## Coming from Pulsar

| Pulsar | Kafka | The difference that bites |
|---|---|---|
| Topic compaction writes a compacted ledger *next to* the original backlog | `cleanup.policy=compact` rewrites the partition itself | Old versions are gone for every reader. There is no uncompacted view to fall back to |
| Readers opt in with `readCompacted(true)` | Every reader of a compacted topic reads the compacted log plus the not-yet-cleaned tail | You can't opt out per consumer, and you can't opt in on a delete-policy topic |
| Compaction runs on a backlog threshold or `pulsar-admin topics compact` | The cleaner runs continuously, driven by dirty ratio and lag configs | Never on the active segment, so a quiet partition is never fully compacted |
| Message with a key and an empty payload = delete | Tombstone = **null** value. An empty value is an ordinary value | The starter "deletes" with `[]byte{}`, and nothing ever gets deleted |
| `TableView`: latest value per key, kept up to date by the client | Compacted topic + your own map + catch-up logic (this task), or a Kafka Streams `GlobalKTable` | "Am I caught up?" is your code, and so is the readiness probe |
| The original messages, deletes included, stay in the backlog until retention | Tombstones are removed `delete.retention.ms` after the first cleaning pass | A long-offline reader resuming from its own snapshot misses deletes (Story 2) |

## Run it

```bash
make dev T=15-compaction-and-state R="producer api"   # hot reload; Ctrl+C to stop
curl -s localhost:8080/readyz
curl -s localhost:8080/customers/cust-042
```

One-shot tools and extra API instances run from your host. The compose `api`
already holds port 8080, so give a host-run api another port:

```bash
go run ./tasks/15-compaction-and-state -role inspect              # records on disk vs distinct keys
WATCH=15s go run ./tasks/15-compaction-and-state -role inspect    # ... every 15s until Ctrl+C
go run ./tasks/15-compaction-and-state -role rebuild              # replay the event log
APPLY_DELAY=10ms HTTP_ADDR=:8081 go run ./tasks/15-compaction-and-state -role api
```

Knobs live in [`task.env`](task.env). Until TODO(15.1) is done every role
exits with `not implemented yet`, and `inspect`/`rebuild` need TODO(15.4).
The reference is in [`solution/`](solution/main.go):
`make dev T=15-compaction-and-state/solution R="producer api"`.

## Your task

1. **TODO(15.1) Compacted topic.** Return the configs for `t15.customers`:
   `cleanup.policy=compact`, `segment.ms=5000`,
   `min.cleanable.dirty.ratio=0.01`, `delete.retention.ms=10000`,
   `min.compaction.lag.ms=0`. These make compaction visible within seconds;
   in prod you'd keep the defaults for everything but the policy.
   `EnsureTopic` never changes an existing topic, so if you created it some
   other way first, delete it
   (`docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:29092 --delete --topic t15.customers`).
   *Done when:* `make describe TOPIC=t15.customers` shows the five configs
   and the producer logs `produced upserts=... tombstones=0`.

   Now run `producer api` and look at the **two prod bugs the starter ships
   with**. The producer's `live_customers` drifts below 200 while
   `/readyz` reports `"customers":200`: deleted customers are still served,
   and the api logs `bad value, skipped` for every delete. And `/readyz` is
   200 from the first millisecond (`"bootstrap_ms":0`).

2. **TODO(15.2) Tombstones.** A delete must be a record with the customer's
   key and a `nil` value. `[]byte{}` is an empty value
   (`valueSize: 0` in the dump), and so is `json.Marshal` of a nil pointer
   (the 4 bytes `null`).
   *Done when:* the producer logs `tombstones` growing, and `make tail
   TOPIC=t15.customers` prints `null` as the value of some records.

3. **TODO(15.3) Apply deletes.** In `view.apply`, a nil value removes the
   key from the table.
   *Done when:* no more `bad value, skipped`, and a customer the producer
   deleted returns 404 (try Experiment C).

4. **TODO(15.4) Catch-up tracker.** Implement `newCatchUp`, `observe` and
   `done`: the target per partition is its end offset captured at startup,
   `next` starts at its start offset and becomes `offset+1` of every record
   seen. A partition whose start equals its end is done before reading
   anything. `inspect` and `rebuild` use this to know when to stop.
   *Done when:* `-role inspect` prints its table and exits, and `-role
   rebuild` logs `projection rebuilt`.

5. **TODO(15.5) Readiness.** In the api, use `startFromBeginning`, keep its
   `catchUp` in `v.boot`, feed it every record, and flip `v.ready` (and log
   `bootstrap complete, ready`) only when it's done. Check it right after
   assigning too: an empty topic is caught up before the first record.
   *Done when:* with `APPLY_DELAY=10ms` a fresh api answers `/readyz` with
   503 and a shrinking `offsets_left`, `GET /customers/{id}` with 503, and
   both switch to 200 after `bootstrap complete, ready`.

## Experiments

**A. Watch the cleaner.** With the producer running (about 50 changes per
second over 200 keys):

```bash
WATCH=15s go run ./tasks/15-compaction-and-state -role inspect
```

OFFSETS grows without bound, KEYS stays at 200, RECORDS hovers in the
hundreds: after 16 minutes of producing one run showed 48,620 offsets and 935
records. BYTES ON DISK differs per broker for a few seconds at a time: each
replica cleans on its own. Why is RECORDS never equal to KEYS while the
producer runs?

**B. Look at the files.**

```bash
docker compose exec -T kafka-1 ls -la /var/lib/kafka/data/t15.customers-0/
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-dump-log.sh --print-data-log \
  --files /var/lib/kafka/data/t15.customers-0/00000000000000000000.log | head -20
```

The cleaned segments are merged into one file named after the first base
offset (`00000000000000000000.log`). Next to it are a few closed segments
waiting for the next pass and the active one, recognisable by its 10 MiB
preallocated `.index` and `.timeindex`. `*.deleted` files are segments the
cleaner replaced; they disappear about a minute later. In the dump, the
first record may be at offset 381, and `kafka-dump-log` lists every
"Non-consecutive offset". Tombstones show as `valueSize: -1` with
`deleteHorizonMs: OptionalLong[...]`.

**C. Delete a customer by hand and watch the tombstone go.** Use a key the
producer never touches:

```bash
K="docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:29092 --topic t15.customers --reader-property parse.key=true --reader-property key.separator=|"
echo 'vip-001|{"id":"vip-001","name":"grace","email":"grace@example.com","tier":"enterprise"}' | $K
curl -s localhost:8080/customers/vip-001                              # 200
echo 'vip-001|NULL' | $K --reader-property null.marker=NULL           # the tombstone
curl -s localhost:8080/customers/vip-001                              # 404
# repeat every 10s or so:
docker compose exec -T kafka-1 sh -c 'for f in /var/lib/kafka/data/t15.customers-0/*.log; do
  /opt/kafka/bin/kafka-dump-log.sh --print-data-log --files $f; done' | grep -B1 'key: vip-001'
```

(`vip-001` hashes to partition 0.) In one run the tombstone was written at
23:20:15. At 23:20:29 the value was gone and the tombstone had a delete
horizon of 23:20:35. At 23:20:43 the tombstone was gone too. A cache that
was offline during those 30 seconds and resumes from its own snapshot: what
does it hold for `vip-001`, forever?

**D. The active segment is never cleaned.** Stop the producer and run
`inspect` a few times over a minute. RECORDS stops shrinking above KEYS (one
run: 324 records for 199 keys, unchanged after 45 more seconds) and the
TOMBSTONES left in the last segment never expire, because nothing rolls the
segment until the next write, not even `max.compaction.lag.ms`. What does that
mean for a GDPR delete on a low-traffic topic, and what could you do about
it?

**E. Bootstrap cost follows keys, not history.** With the producer running
for a while, start a second api that applies slowly and poll it:

```bash
APPLY_DELAY=10ms HTTP_ADDR=:8081 go run ./tasks/15-compaction-and-state -role api &
while sleep 1; do curl -s -o /dev/null -w '%{http_code} ' localhost:8081/readyz; curl -s localhost:8081/readyz; done
```

One run: `offsets_to_read=13053`, `records_applied=932`, `took=10.513s`,
with nine 503s and then 200s. The first fetch jumps `offsets_left` from 5246
to 704 because whole ranges were compacted away. Now
`go run ./tasks/15-compaction-and-state -role rebuild` reads the event log:
14,106 events for the same 200 customers. In year five the compacted topic
still bootstraps 200 keys; the event log replay keeps growing.

**F. Rebuild a projection.** `rebuild` reports `live_customers` (it matched
`inspect`'s LIVE KEYS in our run, 192) and `ever_deleted`, the number of
customers deleted at least once. Can you compute `ever_deleted` from
`t15.customers`? How would you ship "v2 of the customer read model" without
downtime (hint: new group id, `earliest`, build into a new table, switch
reads, delete the old group)?

**G. Readiness during a deploy.** Set `APPLY_DELAY=10ms` in `task.env`,
re-run `make dev T=15-compaction-and-state R="producer api"` (compose
recreates the container), and poll `localhost:8080/readyz` while it starts.
Then do the same with your starter from step 1 and count how many requests
a Kubernetes service would have routed to a pod with an incomplete table.

## Edge cases and gotchas

- **An empty value is not a tombstone.** Neither is `"null"`, `"{}"` or a
  `"deleted": true` flag. Only a null value deletes, and only a deleted key
  ever leaves the topic.
- **No key, no compaction.** The broker rejects keyless records on a
  compacted topic: `InvalidRecordException: Compacted topic cannot accept
  message without key`.
- **Never add partitions to a compacted topic.** The key → partition mapping
  changes (task 02): the old value of a key stays in its old partition
  forever, the new one lands in another, and a bootstrapping reader ends up
  with whichever it applies last. Re-key into a new topic instead.
- **Several versions per key are normal.** Compaction is not dedup.
  Last-write-wins only holds within a partition, by offset.
- **Transactional producers end the log with a commit marker.** A reader
  never receives it as a record, so "seen offset `target-1`" never happens
  on a quiet transactional topic and readiness would hang. With franz-go,
  `kgo.KeepControlRecords()` hands the markers to you; the Java consumer has
  `position()`. Plain (non-transactional) compacted topics don't have this
  problem: the last record lives in the active segment, which is never
  cleaned.
- **Liveness must not depend on the bootstrap.** If `/livez` waits for
  `ready`, a slow bootstrap gets the pod killed and restarted from zero, in a
  loop. Use readiness (or a startup probe) for "caught up".
- **The table is eventually consistent.** A `ProduceSync` that returned is
  not yet visible in `GET`: the consumer may not have applied it. The api
  returns `partition` and `offset` with each row so you can see how fresh it
  is. Read-your-writes means waiting until the view has applied that offset.
- **Every replica holds the whole table in memory.** 2M keys × 1 KiB is
  2 GiB per pod, and the bootstrap reads all of it at each start. Past that,
  persist it locally (RocksDB, as Kafka Streams does) or use a database.
- **The producer here dual-writes** the event and the snapshot as two
  independent records. If it crashes in between they disagree. In prod,
  derive the snapshot from the event log with a processor, or write both in
  one transaction (task 12).
- **A dead cleaner thread is silent.** Compaction stops, disks fill. Alert
  on the cleaner metrics (`DeadThreadCount`, `max-dirty-percent`,
  `time-since-last-run-ms`).
- **Unique keys defeat compaction.** A compacted topic keyed by event ID
  never shrinks: every key is the latest version of itself.

## Check yourself

<details><summary>1. Why does the api capture end offsets before reading instead of reading "until it's caught up"?</summary>

On a busy topic the high watermark moves while you read, so "caught up with
the current end" may never be true, or be true only for a moment. The end
offsets captured at startup are a fixed finish line: once every partition
has delivered `target-1`, the table contains at least everything that
existed when the process started. Records produced meanwhile are applied too,
they just don't delay readiness.
</details>

<details><summary>2. A producer writes key A three times, then a tombstone for A. A new reader starts from the beginning 5 minutes later. What does it see?</summary>

Anything from all four records to nothing at all. It depends on how far the
cleaner got on the leader replica: nothing cleaned yet (all four), cleaned
once (only the tombstone), or cleaned again after `delete.retention.ms` (no
record for A). The guarantee is only that the final state (A deleted) is
what you end up with, if you apply everything in order.
</details>

<details><summary>3. Why not compact the order events topic "to save disk"?</summary>

Compaction keeps one record per key. For `orders` keyed by order ID that is
the last event of each order: `created`, `paid` and `shipped` collapse into
`shipped`. Any future projection that needs the history (time from paid to
shipped, refunds per week) can't be built anymore, for anyone. Keep events
with delete policy and long retention (or tiered storage); publish a
separate compacted snapshot topic if you need fast bootstraps.
</details>

<details><summary>4. Why doesn't the state service use a consumer group?</summary>

A group splits partitions between members, but every instance of the service
needs the entire table. Committed offsets don't help either: the map is in
memory and is rebuilt on every start. Assign all partitions explicitly at
their start offsets (or at offsets stored with a local snapshot).
</details>

<details><summary>5. A compacted partition holds 10 GiB for 1,000 keys and doesn't shrink. What do you check?</summary>

Whether the keys actually repeat (unique keys never compact). The segment
size and `segment.ms`: with 1 GiB / 7 days defaults on a slow topic, most
data sits in the active segment. The dirty ratio vs
`min.cleanable.dirty.ratio`, and a large `min.compaction.lag.ms`. Whether
the cleaner thread is alive (`DeadThreadCount`). And the other replicas:
each one cleans on its own.
</details>

## franz-go ↔ Java client names

| franz-go | Java / docs | Note |
|---|---|---|
| `kgo.Record{Key: k, Value: nil}` | `new ProducerRecord<>(topic, key, null)` | the tombstone |
| `kadm.ListEndOffsets` / `ListStartOffsets` | `consumer.endOffsets()` / `beginningOffsets()` | |
| `cl.AddConsumePartitions`, `kgo.ConsumePartitions` | `consumer.assign()` + `seek()` | no group |
| (no position API for direct consumers) | `consumer.position(tp)` | why `catchUp` tracks `offset+1` itself |
| `kgo.KeepControlRecords` | none: the Java consumer always skips markers | see the transactional gotcha |
| `kadm.DescribeAllLogDirs` | `Admin.describeLogDirs()`, `kafka-log-dirs.sh` | BYTES ON DISK per replica |
| topic configs (`cleanup.policy`, `segment.ms`, ...) | the same names | broker-side, identical for every client |
| this task's `view` + `catchUp` | Kafka Streams `GlobalKTable` / `KTable` | |

## Further reading

- Kafka docs: [Log compaction](https://kafka.apache.org/documentation/#compaction)
- Kafka docs: [Topic configs](https://kafka.apache.org/documentation/#topicconfigs) (`cleanup.policy`, `min.cleanable.dirty.ratio`, `delete.retention.ms`, `max.compaction.lag.ms`)
- [KIP-354: Add a Maximum Log Compaction Lag](https://cwiki.apache.org/confluence/display/KAFKA/KIP-354%3A+Add+a+Maximum+Log+Compaction+Lag)
- [KIP-405: Kafka Tiered Storage](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage)
- Pulsar docs: [Topic compaction](https://pulsar.apache.org/docs/next/concepts-topic-compaction/)
- franz-go: [package kadm docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kadm)
