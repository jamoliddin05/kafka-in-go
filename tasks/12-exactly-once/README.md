# 12 · Exactly-once

> **Goal:** know exactly what a Kafka transaction buys you (atomic writes to
> several partitions *plus* the consumer offsets), what it costs (an open
> transaction stalls every `read_committed` reader of the partitions it
> touched), what it does not cover (anything outside Kafka), and how a zombie
> instance gets fenced.

## Why this matters in prod

**Story 1: the half transfer.** A payments service consumed `transfers`,
wrote a `debit` and a `credit` entry to `ledger` with two produce calls, then
committed the input offset. A node drain killed the pod between the two
produces. The replacement re-read the batch from the last committed offset:
every transfer before the kill was booked twice, and the one in the middle
got a second debit while its first debit never got a credit. The ledger no
longer summed to zero, and nobody could say which entries were "real". Two
writes plus a commit are three separate facts in Kafka; at-least-once
delivery replays all of them.

**Story 2: the 15-minute freeze.** Another team did use transactions. They set
`transaction.timeout.ms=900000` "so big batches never time out" and built the
`transactional.id` from a UUID generated at pod start. A pod died mid-
transaction. Its replacement had a new ID, so nothing fenced the old producer,
and its open transaction sat on six partitions until the coordinator timed it
out 15 minutes later. Every `read_committed` consumer of those partitions,
including services owned by other teams, stopped at the **last stable
offset**. Lag alerts fired everywhere except on the service that caused it.
Nothing was lost; it was just invisible for 15 minutes.

## Concepts

```
            consume-transform-produce, one transaction per poll

t12.transfers-1 ──poll──► processor ──produce──► t12.ledger-0   debit  acc-3 -42
  offset 41                (txn id t12-processor-a,              t12.ledger-2   credit acc-7 +42
                            producer id 5000, epoch 87)
                              │
                              ├─TxnOffsetCommit──► __consumer_offsets  t12.processor: transfers-1 → 42
                              └─EndTxn(commit)───► transaction coordinator (a __transaction_state partition)
                                                    writes a COMMIT marker into ledger-0, ledger-2
                                                    and the __consumer_offsets partition
```

- **What is atomic.** Records to any number of partitions *and* the group's
  offset commit become visible together or not at all. That is what makes a
  Kafka-to-Kafka pipeline "exactly once": the input offset only moves if the
  output exists, and the output only counts if the offset moved.
- **`transactional.id` is an identity, not a transaction ID.** On start the
  producer calls `InitProducerId(transactional.id)`. The coordinator returns a
  producer ID and a bumped **epoch**, and aborts whatever the previous epoch
  left open. Anything still talking with the old epoch is a **zombie** (a pod
  that was presumed dead, a 30-second GC pause, a network partition) and gets
  rejected: `INVALID_PRODUCER_EPOCH` on produce, `PRODUCER_FENCED` on EndTxn.
- **KIP-447 (Kafka 2.5+).** Before it, fencing only worked if each
  `transactional.id` was tied to one input partition, so Kafka Streams ran a
  producer per input partition. Now `TxnOffsetCommit` carries the consumer
  group generation and member ID, so the *group coordinator* rejects a member
  that was kicked out (`UNKNOWN_MEMBER_ID`, `ILLEGAL_GENERATION`). And a new
  owner's `OffsetFetch` with `require_stable` waits while a transaction still
  has pending offsets for that partition. Result: **one transactional producer
  per process**, and the ID only has to be unique among live instances.
- **KIP-890, transactions v2** (this cluster runs `transaction.version=2`):
  the broker checks with the coordinator that a partition is really part of
  an ongoing transaction before appending (`transaction.partition.verification.enable=true`),
  and **every commit or abort bumps the epoch**; EndTxn returns the new one. A
  produce request delayed from transaction N can no longer land inside
  transaction N+1. You'll see the epoch climbing by one per transaction in
  the processor's franz-go logs and in the dump below.
- **The log keeps everything.** Records of an aborted transaction stay on
  disk. The coordinator appends a **control record** (COMMIT or ABORT marker)
  to every partition the transaction touched, and each marker takes an
  offset. Consumers never see markers, so **offsets have gaps**.

```
t12.ledger-0        A = processor a, B = processor b, m = a txn marker

offset   100  101  102  103  104  105  106  107
         A1   A1   mA1  B1   A2   A2   mA2  B1  ...
                        ▲                        ▲
                        LSO = 103                HW = 108
                        (first offset of the oldest still-open transaction, B1)

read_uncommitted   returns 100,101,103,104,105,107: B1's records too, even if B1 aborts
read_committed     returns only what is below the LSO: 100,101. A2 is committed,
                   but it waits behind B1. Aborted records are skipped by
                   the abort index the broker sends with each fetch.
```

- **Last stable offset (LSO)** = the first offset of the oldest open
  transaction in the partition (or the HW if none). `read_committed`
  consumers read up to the LSO and stop. One slow or dead transactional
  producer stalls *everyone* who reads that partition with `read_committed`,
  including data from other producers. The log cleaner also stops at the LSO.
- **`transaction.timeout.ms`** is sent by the producer in `InitProducerId`
  and capped by the broker's `transaction.max.timeout.ms` (15 min). The
  coordinator aborts expired transactions on a timer
  (`transaction.abort.timed.out.transaction.cleanup.interval.ms`, 10 s), so the
  worst-case stall is timeout + 10 s. franz-go's default is 40 s, Java's 60 s.
- **What EOS does not cover.** Database writes, HTTP calls, emails, metrics:
  everything outside Kafka runs again when a transaction aborts and the batch
  is reprocessed. For those you still need idempotency (task 10) or an outbox
  (task 11). And every downstream consumer must opt into `read_committed`, or
  it sees aborted records as if they were real.
- **franz-go's `GroupTransactSession`** is a consumer group client plus a
  transactional producer that know about each other. `Begin`, produce, then
  `End(ctx, kgo.TryCommit)` flushes, sends `TxnOffsetCommit` with the polled
  offsets, forces a heartbeat to prove the member is still in the group, then
  `EndTxn`. If partitions were revoked or lost while the transaction was open,
  End **downgrades the commit to an abort** and rewinds the consumer to the
  last committed offsets, so the next poll re-reads the batch. With the Java
  client you write that abort-and-rewind logic yourself.

## Coming from Pulsar

| Pulsar | Kafka | The difference that bites |
|---|---|---|
| Transaction coordinator, a fresh transaction ID per `newTransaction()` | Coordinator = a partition of `__transaction_state`; `transactional.id` is a stable name you pick | Fencing is by name. A random name per pod means a restart fences nothing, and the dead pod's transaction blocks readers until it times out |
| Transaction buffer: uncommitted messages are not dispatched | Uncommitted records are in the log, and `read_uncommitted` is the **default** isolation | Every consumer of a transactional topic must set `read_committed`, or it acts on aborted data |
| Dispatch stops at the first ongoing transaction (`maxReadPosition`) | `read_committed` stops at the LSO | Same stall, same cause: keep transactions short |
| Acks in a transaction are per message (pending ack) | The transaction commits the group's cumulative offset | Order of processing inside a partition still matters |
| Aborted messages are dropped from dispatch | Aborted records and markers stay in the log and take offsets | Offset gaps; `end - start` is not a record count |

## Run it

```bash
make dev T=12-exactly-once R="producer consumer"        # transfers in, auditor (read_committed)
until SESSION_TIMEOUT=10s go run ./tasks/12-exactly-once -role processor; do sleep 1; done   # processor
```

The processor runs from the host in a restart loop because air does not
restart a process that exited: it waits for the next file save. The loop is
what a Kubernetes `restartPolicy` does for you. It stops when the processor
exits cleanly (Ctrl+C). On the host `task.env` is not loaded, so knobs go
inline; `SESSION_TIMEOUT=10s` makes a crashed member's partitions move after
10 s instead of 45 s.

More tools from the host:

```bash
ISOLATION=read_uncommitted go run ./tasks/12-exactly-once -role consumer   # a second auditor
go run ./tasks/12-exactly-once -role txns                 # HW vs LSO per partition, open transactions
EVERY=1s go run ./tasks/12-exactly-once -role txns
```

The auditor reads the whole ledger from offset 0 and prints, every 2 s:

```
msg=audit isolation=read_committed records=530 transfers=265 balanced=265 pending=0 incomplete=0 duplicated=0 offset_gaps=49 money_created=0
```

`pending` = one leg seen less than `GRACE` (5 s) ago, `incomplete` = one leg
for longer, `duplicated` = a leg seen twice, `money_created` = the sum over
all non-pending transfers (should be 0). Knobs are in [`task.env`](task.env).

**Starting over.** The log is immutable, so whatever the starter wrote stays
in the ledger, and the auditor keeps counting it. Stop all t12 processes and
drop the topics and the group before you test your transactional version
(the group delete fails with `GroupNotEmpty` until a crashed member's
`SESSION_TIMEOUT` has passed):

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:29092 --delete --topic 't12\..*'
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:29092 --delete --group t12.processor
```

The starter runs: its processor is deliberately **non-transactional**
(debit, credit and offset commit as three separate steps). Experiment A shows
what that costs; then you fix it. The reference is in
[`solution/`](solution/main.go): `make dev T=12-exactly-once/solution R="producer consumer"`
and `go run ./tasks/12-exactly-once/solution -role processor`.

## Your task

1. **TODO(12.1) Transactional session.** Replace the plain client in
   `process` with `kgo.NewGroupTransactSession(app.ClientOpts(...)...)`:
   `ConsumerGroup`, `ConsumeTopics`, `TransactionalID` (default
   `t12-processor-<INSTANCE>`), `TransactionTimeout(TX_TIMEOUT)`,
   `SessionTimeout(SESSION_TIMEOUT)`, `FetchIsolationLevel(ReadCommitted())`
   and `DefaultProduceTopic`. Then call `sess.Client().ProducerID(ctx)` right
   away: franz-go otherwise runs `InitProducerId` lazily, on the first
   transaction, which comes only after the group gives you partitions, and
   until then your previous incarnation's open transaction keeps the LSO
   stuck.
   *Done when:* the processor logs `transactional processor started ... producer_id=... epoch=N`,
   and after a restart the epoch is higher.
2. **TODO(12.2) One transaction per poll.** In `handle`: `Begin`, produce both
   legs of every transfer with `sess.Produce` and one
   `kgo.AbortingFirstErrPromise`, keep the `CRASH_PCT` exit between the legs
   (flush first, so the half transfer is really on the log when you die),
   then `End(TryCommit)`. Abort instead if any produce failed, or with
   probability `FAIL_PCT`; with `SLOW_TX` set, sleep before ending. Do the
   work with `context.WithoutCancel(ctx)`: canceling End leaves the outcome
   unknown. Return `End`'s error (it is never retryable) and exit; treat
   `kerr.ProducerFenced` as "another instance owns my ID".
   *Done when:* after resetting the topics, two minutes of
   `until CRASH_PCT=10 SESSION_TIMEOUT=10s go run ./tasks/12-exactly-once -role processor; do sleep 1; done`
   leave the `read_committed` auditor at `incomplete=0 duplicated=0 money_created=0`.
3. **TODO(12.3) Isolation.** Make the auditor honor `ISOLATION` with
   `kgo.FetchIsolationLevel`.
   *Done when:* under `FAIL_PCT=30` the two auditors disagree (experiment B).
4. **TODO(12.4) Find the blocker.** Fill the `txns` tool:
   `ListEndOffsets` (HW), `ListCommittedOffsets` (LSO), `DescribeProducers`
   (which producer has an open transaction from which offset) and
   `DescribeTransactions` (its transactional ID, state and age).
   *Done when:* with `SLOW_TX=20s`, `-role txns` shows `HW-LSO > 0` and names
   the open transaction and how long it has been open.

## Experiments

**A. The naive processor (starter).** With the starter, run the processor in
the restart loop with crashes: `until CRASH_PCT=10 SESSION_TIMEOUT=10s go run ./tasks/12-exactly-once -role processor; do sleep 1; done`.
In 75 s on this laptop it crashed 7 times and the auditor ended at
`duplicated=9 money_created=-222`, with lines like:

```
msg="bad transfer" id=CUBVV4MFYA problem="missing leg" debits=1 credits=0 net=-57
msg="bad transfer" id=PEU2LWECEB problem="duplicated legs" debits=2 credits=2 net=0
```

Each restart re-reads the batch from the last commit. Why does a "missing
leg" usually turn into `debits=2 credits=1` a bit later, and why does
processing pause for about 10 s after every crash? (Hint: the dead member is
still in the group until its session times out; task 07.)

**B. Aborted records are still there.** Reset, then run your processor with
`FAIL_PCT=30` and both auditors. After 15 s here: `read_committed`
`records=394 transfers=197 balanced=197 duplicated=0`, `read_uncommitted`
`records=464 transfers=197 balanced=173 duplicated=24`. The aborted attempts
are complete pairs (`money_created=0` in both), yet a `read_uncommitted`
consumer that charged cards would have charged 24 of them twice.

**C. Look at the markers.** Any broker has a replica:

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-dump-log.sh --print-data-log \
  --files /var/lib/kafka/data/t12.ledger-0/00000000000000000000.log | tail -6
```

```
baseOffset: 97 lastOffset: 97 count: 1 baseSequence: 0 ... producerId: 5000 producerEpoch: 87 ... isTransactional: true isControl: false ...
| offset: 97 ... key: acc-7 payload: {"transfer_id":"6QQFRVRRIH","leg":"debit","account":"acc-7","amount":-42}
baseOffset: 98 lastOffset: 98 count: 1 baseSequence: -1 ... producerId: 5000 producerEpoch: 88 ... isTransactional: true isControl: true ...
| offset: 98 ... endTxnMarker: COMMIT coordinatorEpoch: 0
baseOffset: 99 lastOffset: 99 count: 1 baseSequence: 0 ... producerId: 5000 producerEpoch: 88 ... isTransactional: true isControl: false ...
| offset: 99 ... key: acc-6 payload: {"transfer_id":"DTYPDPVJO2","leg":"debit","account":"acc-6","amount":-97}
```

Find an `endTxnMarker: ABORT` after B (`grep -c ABORT` counted 33 next to 639
COMMIT here). Why is the marker's `producerEpoch` one higher than the data it
commits, and why does `baseSequence` restart at 0 in every transaction? Why
does the auditor report `offset_gaps` even in `read_uncommitted` mode?

**D. One open transaction stalls everyone.** Reset not needed. Two
processors in the same group, one slow:

```bash
INSTANCE=a SLOW_TX=20s go run ./tasks/12-exactly-once -role processor    # terminal 1
INSTANCE=b go run ./tasks/12-exactly-once -role processor                # terminal 2
EVERY=1s go run ./tasks/12-exactly-once -role txns                       # terminal 3
```

b commits every few hundred ms, yet the `read_committed` auditor moves in
20-second steps (`records=886` for 16 s, then `1052`), while the
`read_uncommitted` one keeps flowing. `txns` shows the reason:

```
PARTITION  HW   LSO  HW-LSO  OPEN TRANSACTIONS (producer id/epoch from offset)
1          307  282  25      5000/585 from 282
TRANSACTIONAL ID  STATE    PRODUCER ID  EPOCH  TIMEOUT  OPEN FOR  PARTITIONS
t12-processor-a   Ongoing  5000         585    40s      12.5s     t12.ledger-1 t12.ledger-2
```

Watch `incomplete` in the `read_committed` auditor too: a transfer that b
committed can show its debit (partition 1) without its credit (partition 2,
stuck behind a). What does that tell you about cross-partition atomicity?

**E. Fencing.** Two live processors with the same transactional ID:

```bash
INSTANCE=a go run ./tasks/12-exactly-once -role processor                                  # terminal 1
INSTANCE=a2 TRANSACTIONAL_ID=t12-processor-a go run ./tasks/12-exactly-once -role processor  # terminal 2
```

The second one's `InitProducerId` bumps the epoch; the first dies on its next
transaction:

```
msg="exited with error" inst=a err="fenced: a newer instance owns our transactional ID: PRODUCER_FENCED: There is a newer producer with the same transactionalId which fences the current one."
```

With `KAFKA_LOG_LEVEL=info` you also see the partition leaders reject its
produce first: `batch errored, failing the producer ID ... err="INVALID_PRODUCER_EPOCH: Producer attempted an operation with an old epoch."`.
What would happen if the processor retried instead of exiting?

**F. The zombie.** Freeze a processor mid-transaction, the way a long GC
pause or a VM stall would:

```bash
go build -o /tmp/t12 ./tasks/12-exactly-once
export SESSION_TIMEOUT=10s
INSTANCE=b /tmp/t12 -role processor &                       # the healthy peer
INSTANCE=a SLOW_TX=3s KAFKA_LOG_LEVEL=info /tmp/t12 -role processor & A=$!
sleep 15; kill -STOP $A; sleep 20; kill -CONT $A
kill -INT %1 %2                                             # when you're done
```

During the freeze b takes over a's input partitions after 10 s
(`SESSION_TIMEOUT`), but the ledger LSO stays at a's open transaction, so
`read_committed` readers stall again. On `SIGCONT`, a's heartbeat gets
`UNKNOWN_MEMBER_ID`, the session logs `transact session in on_lost; aborting next commit`,
`End` aborts (`transaction aborted by the session (rebalance)`), a rejoins and
carries on. No duplicates: that is KIP-447 fencing by group membership. Now
repeat with `TX_TIMEOUT=10s` and a 30 s freeze: the coordinator has aborted
the transaction and bumped the epoch, and a dies with
`unable to commit transaction offsets: ... INVALID_PRODUCER_EPOCH`.

**G. Random transactional IDs.** Crash your processor once
(`CRASH_PCT=100 INSTANCE=x go run ./tasks/12-exactly-once -role processor`),
then start it with `INSTANCE=y`. `txns` shows `t12-processor-x Ongoing`
growing past 40 s, and `read_committed` readers stalled, until the coordinator
aborts it (40 s + up to 10 s). Start it with `INSTANCE=x` instead: the
`ProducerID` call at startup aborts it immediately (`Empty`, epoch +2).
Which naming scheme would you use for pods in a Deployment? In a StatefulSet?

## Edge cases and gotchas

- **`read_uncommitted` is the default** in franz-go and Java, and in the
  console consumer, kcat and most sink connectors. A transactional producer
  protects nobody whose consumers did not opt in.
- **`read_committed` is not a snapshot.** It guarantees no aborted data and
  per-partition order. Two partitions of one committed transaction can become
  visible at different times (experiment D). If a consumer must see "both or
  neither", put both in the same partition or make the consumer tolerate a gap.
- **Side effects run again.** A DB write, an HTTP call, a log line or a
  metric inside the processor happens once per *attempt*, not once per
  committed transaction.
- **Keep transactions short.** The open duration of your longest transaction
  is the latency floor of every `read_committed` reader of those partitions,
  and `transaction.timeout.ms` is the worst case after a crash. Do not raise
  it to "be safe".
- **Stable, unique transactional IDs.** Unique among live instances
  (otherwise they fence each other in a loop), stable across restarts (so the
  restart aborts the predecessor's transaction at once). An ID unused for
  `transactional.id.expiration.ms` (7 days) is forgotten.
- **Tiny transactions are expensive.** Each one costs a `TxnOffsetCommit`,
  an `EndTxn`, a marker write per partition (and an offset each). Here every
  poll became a 1-transfer transaction at 4 transfers/s; at high rates,
  batch more per transaction (`MAX_POLL`).
- **Do not call `End` from `OnPartitionsRevoked`/`Lost`** with a
  `GroupTransactSession`: the session's callback wrappers hold the lock End
  needs (franz-go documents the deadlock).
- **Hanging vs long-open.** A *hanging* transaction is one the partition
  thinks is open but the coordinator does not (pre-KIP-890 bugs, late
  produces); it never times out. `kafka-transactions.sh find-hanging` and
  `abort` exist for that. A merely long-open transaction is visible in
  `kafka-transactions.sh list` / `describe` and ends at the timeout.
- **Producer-only transactions** (no group) are fine too: atomic multi-topic
  writes with `BeginTransaction` / `EndTransaction` on a plain client.

## Check yourself

<details><summary>1. What exactly does "exactly once" mean for this processor?</summary>

For every input record, the effects *inside Kafka* (the ledger entries and the
input offset commit) happen once as seen by `read_committed` consumers. The
processing code may run several times (aborts, rebalances, crashes), and any
side effect outside Kafka happens as many times as it runs.
</details>

<details><summary>2. A read_committed consumer of a topic you don't own stopped advancing. HW keeps growing. Where do you look?</summary>

At the LSO and the open transactions on the stuck partitions:
`kafka-transactions.sh describe-producers --topic T --partition P` (or this
task's `txns`) shows the producer ID and the offset its transaction started
at; `kafka-transactions.sh list` / `describe` maps it to a transactional ID.
Usually it is a crashed producer whose ID nobody reused, so you wait for the
timeout or `abort` it.
</details>

<details><summary>3. Two pods accidentally share a transactional.id. What do you see?</summary>

Each `InitProducerId` fences the other: produce batches fail with
`INVALID_PRODUCER_EPOCH`, `EndTxn` with `PRODUCER_FENCED`. If the pods restart
on error, they take turns killing each other, and every transaction one of
them had open is aborted.
</details>

<details><summary>4. Why is one transactional producer per process enough since Kafka 2.5?</summary>

KIP-447. The offset commit inside the transaction carries the consumer group
generation, so a member that lost its partitions is rejected by the group
coordinator even though its producer epoch is still valid, and a new owner
waits for pending transactional offsets before it starts reading. Fencing no
longer depends on binding transactional IDs to input partitions.
</details>

<details><summary>5. Partition end offset 1000, start 0, but a full read returns 870 records. Data loss?</summary>

Not necessarily. Transaction markers take an offset each and are never
returned, and with `read_committed` the records of aborted transactions are
skipped too. Compaction (task 15) makes more gaps. Offsets are positions,
not counts.
</details>

## franz-go ↔ Java client names

| franz-go | Java / docs | Note |
|---|---|---|
| `kgo.TransactionalID` | `transactional.id` | |
| `kgo.TransactionTimeout` | `transaction.timeout.ms` | franz-go 40 s, Java 60 s, broker cap `transaction.max.timeout.ms` 15 min |
| `kgo.FetchIsolationLevel(kgo.ReadCommitted())` | `isolation.level=read_committed` | default `read_uncommitted` in both |
| `Client.ProducerID(ctx)` | `producer.initTransactions()` | Java requires it up front; franz-go does it lazily unless you call it |
| `GroupTransactSession.Begin` | `beginTransaction()` | |
| `GroupTransactSession.End(ctx, kgo.TryCommit)` | `sendOffsetsToTransaction(offsets, consumer.groupMetadata())` + `commitTransaction()` | End also checks group membership and aborts after a rebalance |
| `End(ctx, kgo.TryAbort)` | `abortTransaction()` | |
| `kerr.ProducerFenced`, `kerr.InvalidProducerEpoch` | `ProducerFencedException`, `InvalidProducerEpochException` | |
| `kadm.ListCommittedOffsets` | `Admin.listOffsets(..., new ListOffsetsOptions(IsolationLevel.READ_COMMITTED))` | the LSO |
| `kadm.DescribeProducers`, `kadm.DescribeTransactions` | `Admin.describeProducers`, `Admin.describeTransactions`, `kafka-transactions.sh` | |
| `kgo.KeepControlRecords` | (none) | returns markers to you; rarely useful |

## Further reading

- Kafka docs: [Message delivery semantics](https://kafka.apache.org/documentation/#semantics)
- [KIP-98: Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)
- [KIP-447: Producer scalability for exactly once semantics](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics)
- [KIP-890: Transactions Server-Side Defense](https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense)
- [KIP-664: Provide tooling to detect and abort hanging transactions](https://cwiki.apache.org/confluence/display/KAFKA/KIP-664%3A+Provide+tooling+to+detect+and+abort+hanging+transactions)
- Confluent: [Transactions in Apache Kafka](https://www.confluent.io/blog/transactions-apache-kafka/)
- franz-go: [transactions and KIP-447 notes](https://github.com/twmb/franz-go/blob/master/docs/transactions.md), [`GroupTransactSession`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#GroupTransactSession)
