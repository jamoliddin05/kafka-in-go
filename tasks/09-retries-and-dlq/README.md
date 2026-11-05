# 09 · Retries and DLQ

> **Goal:** a record that can't be processed must never stop the records
> behind it. Build what Pulsar gives you with `negativeAcknowledge` and a
> `DeadLetterPolicy`: an error taxonomy, bounded retries, a retry topic with
> delays, a DLQ with enough headers to debug, and a redrive tool.

## Why this matters in prod

**Story 1: six hours on one record.** An upstream team shipped a new
version that sent `amount_cents` as `"19.99"` instead of `1999`. The payments
consumer was written for "the database blinked": on any error, retry in
place with backoff, forever. The first string amount landed on partition 4 at
02:10. That partition stopped. The other 11 kept flowing, so the dashboards
built on throughput looked fine, and the consumer group was `Stable` with all
members heartbeating. At 08:15 support asked why one in twelve customers had
not been charged since the night. The lag alert existed, but on total lag,
which one stuck partition didn't push over the threshold.

**Story 2: the DLQ that ate an outage.** Another team had a DLQ, and it
worked. Then the card processor was down for 40 minutes. Every order failed
three quick retries and went to the DLQ, 180k records. Nobody had built a
redrive tool, so someone wrote one at 3 AM. It re-published everything
without the original keys, so orders for the same customer were spread over
all partitions, and refunds raced ahead of the charges they refunded.

## Concepts

```
                     t09.orders (3 partitions, key = customer)
                               │
                               ▼
           ┌───────── consumer (group t09.orders) ─────────┐
           │ process; transient → retry in place 2x (200ms) │
           └───┬───────────────┬───────────────────────┬────┘
            ok │     permanent │                       │ still transient
               ▼               ▼                       ▼
             done      t09.orders.dlq  ◄────┐   t09.orders.retry   retry-attempt=1
                       (1 partition)        │          │           retry-at=now+1s
                               │            │          ▼
                               │            │   processor (group t09.orders.retry)
                               │            │   waits until retry-at, then process
                               │            │   fail → attempt+1, retry-at=now+2s, 4s, 8s
                               │            └── permanent, or attempt 4 failed
                               ▼
                  -role dlq: list, depth       -role redrive: back to t09.orders
                                                  (after the fix; group t09.redrive)
```

- **Why Kafka can't "skip and come back".** A group's progress on a
  partition is one committed offset. There are no per-record acks. If you
  commit past a failed record, it's gone for this group. If you don't, nothing
  behind it gets processed. A record that keeps failing therefore either
  blocks its partition or has to be **copied somewhere else** before you
  commit past it.
- **Error taxonomy** decides where a failure goes:
  - *Transient*: timeouts, 503s, connection refused, lock conflicts. The same
    record will probably succeed later.
  - *Permanent* (poison): can't decode, fails validation, references
    something that will never exist. Retrying only burns time.
  - *Unknown*: treat as transient. The attempt limit still ends it in the
    DLQ, only later.
- **Blocking retry in place** keeps per-key order and is the right answer for
  short blips (a 50 ms DB failover), as long as it is **bounded**. Unbounded,
  it's Story 1.
- **Non-blocking retry topic.** After a few local tries, the record is
  produced to `t09.orders.retry` with a `retry-at` header, and the original is
  committed. A separate worker consumes the retry topic and does not process
  a record before its `retry-at`. Kafka has no delayed delivery, so the worker
  waits, and *how* it waits matters:
  - Sleeping inside the poll loop holds up every other partition of that
    worker. With `BlockRebalanceOnPoll` it also holds up rebalances for the
    whole group, until the member is kicked after the rebalance timeout
    (franz-go default 60 s). Without it, the rebalance happens while you
    sleep and you may commit partitions you no longer own.
  - **Pause instead.** When the head record of a partition isn't due:
    `PauseFetchPartitions` that partition, `SetOffsets` it back to that
    record, remember when it's due, and keep polling with a deadline at the
    earliest due time. Then `ResumeFetchPartitions` and the record is
    fetched again. The poll loop keeps running, so rebalances and other
    partitions proceed.
  - The delays in one retry topic aren't monotonic (attempt 3 waits 4 s,
    attempt 1 waits 1 s), so a record behind a long-delayed one waits longer
    than it has to. Production systems use one retry topic per delay tier
    (`retry-5s`, `retry-1m`, `retry-10m`), where FIFO is also due-time order.
- **Ordering trade-off.** Once a record for customer `c-10` goes to the retry
  topic, the next `c-10` order on the main topic is processed first. If order
  matters per key (charge before refund), **park the key**: while a key has a
  record in retry, send its later records to the retry topic too, behind it,
  and unpark when the retry succeeds or dead-letters. The parked set has to
  survive restarts and rebalances (a compacted topic or a table), which is
  why many teams only do this for the topics that truly need it.
- **DLQ headers** are what make the DLQ debuggable and redrivable:
  `original-topic/partition/offset` and `first-failure-at` (set once, at the
  first failure), `error`, `error-class`, `retry-attempt` (replaced on every
  hop), plus **every original header**, `traceparent` included, so one click
  in Jaeger shows every attempt. Keep the original key too: it decides the
  partition when the record comes back.
- **Commit semantics.** A record is done only when it is processed, or when
  its copy in the retry topic or DLQ is **acked**. So: `ProduceSync` the copy,
  then commit. If that produce fails, don't commit and don't move on: exit,
  restart, and the record is read again. With an async produce you could
  commit before the copy is durable and lose it on a crash.
- **Redrive** is a tool you run after the fix is deployed. It reads the DLQ
  from where the last redrive stopped (offsets of the `t09.redrive` group),
  re-publishes to the main topic with the same key, value and headers, clears
  the retry bookkeeping, adds `redrive-count`, and then commits. Skip records
  that already failed `MAX_REDRIVES` times, or a permanently broken record
  bounces between the main topic and the DLQ forever.
- **DLQ depth is an alert.** Records in the DLQ that the redrive group hasn't
  passed yet are orders nobody has charged. Alert when it's above zero (or
  growing), not just on lag.

## Coming from Pulsar

| Pulsar | Kafka | The difference that bites |
|---|---|---|
| `negativeAcknowledge(msg)` + `negativeAckRedeliveryDelay` | Nothing. One committed offset per partition | A failing record blocks everything behind it in that partition |
| `ackTimeout` | Nothing, and franz-go has no `max.poll.interval.ms` either | A stuck handler keeps its partitions silently, forever, while the group looks `Stable` |
| `DeadLetterPolicy{maxRedeliverCount, deadLetterTopic}` | Your DLQ topic plus an attempt counter in a header | You define what's permanent, which headers go along, and how to redrive |
| `enableRetry(true)` + `reconsumeLater(msg, delay)` (retry letter topic) | A retry topic, a `retry-at` header, pause/resume | Retried records lose per-key order unless you park the key |
| `getRedeliveryCount()` | A header you maintain yourself | Share groups (task 16) bring a broker-side delivery count |
| `deliverAfter` / `deliverAt` | None | The consumer has to wait, and must keep polling while it does |

In Pulsar, a nack on a Shared subscription only delays that one message
because acks are individual. In Kafka the unit of progress is the partition,
which is why "retry" in Kafka always means "copy the record somewhere else".

## Run it

```bash
make dev T=09-retries-and-dlq R="producer consumer processor"
make scale T=09-retries-and-dlq R=consumer N=3        # one consumer per partition (Experiment A)
```

From the host (the producer, consumer and processor send spans to Jaeger as
`t09-*`):

```bash
go run ./tasks/09-retries-and-dlq -role producer
go run ./tasks/09-retries-and-dlq -role consumer      # once per terminal, up to 3
go run ./tasks/09-retries-and-dlq -role processor
```

One-shot tools:

```bash
WATCH=5s go run ./tasks/09-retries-and-dlq -role lag          # committed / end / lag per partition
GROUP=t09.orders.retry go run ./tasks/09-retries-and-dlq -role lag
go run ./tasks/09-retries-and-dlq -role dlq                    # every DLQ record + depth
DRY_RUN=true go run ./tasks/09-retries-and-dlq -role redrive
go run ./tasks/09-retries-and-dlq -role redrive
make tail TOPIC=t09.orders.dlq                                 # raw headers
```

Knobs live in [`task.env`](task.env). The starter is **deliberately naive and
runs**: the consumer retries every error in place, forever, and the processor
handles retry-topic records the moment they arrive. Run it first and watch a
partition die (Experiment A). `dlq` and `redrive` exit with
`not implemented yet` until steps 5 and 6. The reference is in
[`solution/`](solution/main.go):
`make dev T=09-retries-and-dlq/solution R="producer consumer processor"`.

## Your task

1. **TODO(09.1) Classify** (`handle.go`). Return `permanent` for errors that
   fail the same way every time (`*json.SyntaxError`,
   `*json.UnmarshalTypeError`, `errInvalidOrder`) and `transient` otherwise.
   *Done when:* after step 3, a poison pill goes to the DLQ on its first
   failure with `error-class=permanent` and no in-place retries in the log.
2. **TODO(09.2) Reroute** (`route.go`). Copy all headers of the source record
   and set the bookkeeping headers: `error`, `error-class`, `retry-attempt`
   every time, `original-*` and `first-failure-at` only once.
   *Done when:* `make tail TOPIC=t09.orders.dlq` shows `traceparent` and
   `original-topic:t09.orders`, also for records that went through the retry
   topic first.
3. **TODO(09.3) Bounded retries** (`main.go`). Write `handleMain` and call it
   instead of `retryForever`: up to `LOCAL_RETRIES` in-place retries for
   transient errors, then `park` the record in the retry topic (attempt 1)
   or the DLQ. Commit only after the batch is fully processed or parked.
   *Done when:* with **one** consumer and `POISON_PCT=5`, `-role lag` shows
   every partition near 0, and transient orders log `rerouted` with
   `to=t09.orders.retry attempt=1`.
4. **TODO(09.4) Wait until due** (`retry.go`). For a record whose `retry-at`
   is in the future: pause its partition, commit what's marked, `SetOffsets`
   back to the record, remember the due time, stop processing that
   partition. Before each poll, resume what's due, and poll with a deadline
   at the earliest due time.
   *Done when:* a 3 s outage resolves as `order paid ... attempt=2`, and in
   Jaeger that order's trace shows the main attempt at ~0 s and retry
   attempts about 1.4 s and 3.8 s after the send.
5. **TODO(09.5) Inspect the DLQ** (`dlq.go`). Print every DLQ record with its
   bookkeeping headers, `traceparent` and value, then the total and how many
   are at or above the `t09.redrive` group's committed offset.
   *Done when:* the last line reads like
   `14 records in t09.orders.dlq, 14 pending redrive`.
6. **TODO(09.6) Redrive** (`dlq.go`). Re-publish pending DLQ records to
   `t09.orders` (same key, value, headers; retry bookkeeping removed;
   `redrive-count` + 1; skip at `MAX_REDRIVES`), wait for all acks, then
   commit the DLQ end offsets for `t09.redrive` with `kadm.CommitAllOffsets`.
   *Done when:* after deploying `LENIENT_AMOUNT=true`, a redrive gets every
   poison order paid, a second redrive reports `redriven=0`, and
   `GROUP=t09.redrive ... -role lag` shows lag 0.

## Experiments

**A. One pill, one partition.** Starter, three consumers (one per partition),
`WATCH=5s ... -role lag`. A run from this course: the first poison pill hit
partition 1 at 10:35:27. Partition 1's lag went 8, 18, 30 … 152 over the next
70 s while partition 2 stayed at 0 until its own pill arrived at 10:36:27.
The stuck consumer logs `retrying in place ... attempt=27` with
`cannot unmarshal string into Go struct field Order.amount_cents`. Now run a
single consumer instead: it owns all three partitions, hits the first pill,
and **nothing** is processed any more (our run: 0 orders paid, lag growing on
all three). Why does `make group G=t09.orders` still show a healthy group? What would a Java consumer do after
`max.poll.interval.ms` (5 min), and where would the pill go next?

**B. A transient blip.** In the same starter run, a 503 order stalls its
partition for about 3 s and then recovers. That's the case blocking retry is
good at. What `TRANSIENT_FOR` turns it into Story 1?

**C. Retrying without waiting.** After steps 1-3 but before step 4, the
processor handles retry records as soon as they arrive. Measured: transient
orders with a 3 s outage burned all 4 attempts and reached the DLQ 3 to
37 ms after the first reroute. A retry without a delay is just a slower way
to dead-letter.

**D. Outage longer than the budget.** With the solution, run the producer
with `POISON_PCT=0 TRANSIENT_PCT=20 TRANSIENT_FOR=40s` for a few seconds.
The budget is 1+2+4+8 s plus jitter, so after about 16 s the orders land in
the DLQ with `class=transient attempt=4`. Wait until the "outage" is over,
then `-role redrive`: they're paid. Now imagine the outage is an hour and
every order fails. What should the consumer do instead of filling the DLQ?

**E. Hotfix and redrive.** Let some poison pills reach the DLQ, restart the
consumer and processor with `LENIENT_AMOUNT=true`, then
`DRY_RUN=true -role redrive`, `-role redrive`, `-role dlq`. The redriven
records keep their original `traceparent`: open one in Jaeger and see the
new processing span appear under a send span from minutes or hours ago.
Task 08 explains why a link would be the better shape here.

**F. Order breaks.** grep the solution's logs by one key, e.g. `key=c-10`:
a transient order paid by the processor at 23:34:15.8 came *after* a later
`c-10` order that the consumer paid at 23:34:14.8. A 30 s run had 12 such
inversions. Sketch the "park the key" change: where would the parked set
live so it survives a rebalance?

**G. Rebalance with paused partitions.** Run two processors with
`TRANSIENT_PCT=40 TRANSIENT_FOR=5s`, stop one after 10 s. In our run, 100
orders were produced and 100 paid, none twice: the survivor took over the
partition, and the revoke callback had resumed it. Delete the
`ResumeFetchPartitions` call in `OnPartitionsRevoked` and think through what
happens when a paused partition comes back to the same member later.

## Edge cases and gotchas

- **Pauses outlive assignments.** franz-go keeps a partition paused until you
  resume it, across rebalances. Resume in `OnPartitionsRevoked` and
  `OnPartitionsLost`, or a partition that comes back is never fetched.
- **Paused partitions drop buffered data without losing it.** Records already
  fetched for a paused partition are stripped from the next poll and the
  fetch position isn't advanced, so they're fetched again on resume.
- **`SetOffsets` also resets what the client thinks is committed.** Commit
  the marked progress before rewinding, or a crash redoes it.
- **Use marks when records live across polls.** The retry worker uses
  `AutoCommitMarks` + `MarkCommitRecords`: default autocommit would commit
  records you rewound and haven't processed.
- **`ProduceSync` already retries.** franz-go retries retriable produce
  errors without limit by default, so an error from it is fatal for that
  record or a cancelled ctx. Exiting without commit is the honest reaction.
- **Bigger record, new failure.** Headers add bytes. A record close to the
  topic's `max.message.bytes` can fail to enter the DLQ with
  `MESSAGE_TOO_LARGE`. Truncate the error header, and know your limit.
- **Retention.** The retry topic must keep records longer than the longest
  delay. The DLQ needs a retention measured in "how long until someone
  looks": the cluster default of 7 days is often too short.
- **The DLQ is not for outages.** When every record fails, stop consuming
  (circuit breaker, pause the main topic, page someone). Dead-lettering 100%
  of traffic converts an incident into a data-repair project.
- **Clock skew.** `retry-at` is written by one host and read by another.
  Seconds of skew shift your delays by seconds.
- **Duplicates.** A crash between producing the copy and committing the
  original, or between redrive and its commit, processes a record twice.
  Task 10 makes that harmless.

## Check yourself

<details><summary>1. Why can't a Kafka consumer skip a bad record and come back to it later, the way a Pulsar nack does?</summary>

Progress is a single committed offset per partition. Committing past the bad
record means "everything below this offset is done", so the record is lost
for this group. Not committing blocks everything behind it. The only way to
skip it safely is to copy it elsewhere (retry topic or DLQ), wait for the
ack, and then commit past it.
</details>

<details><summary>2. Your retry worker sleeps 30 s inside the poll loop. What goes wrong?</summary>

Every other partition of that worker waits too. With `BlockRebalanceOnPoll`
the whole group's rebalance waits for you and you may be kicked after the
rebalance timeout. Without it, a rebalance can complete while you sleep and
you then commit partitions you no longer own. A Java consumer is kicked
after `max.poll.interval.ms`. Pausing the partition and keeping the poll loop
running avoids all of it.
</details>

<details><summary>3. Why produce to the DLQ synchronously before committing?</summary>

If you commit first (or produce asynchronously and commit without waiting for
the ack) and the process dies before the DLQ write is acknowledged, the
record is neither processed nor in the DLQ, and the committed offset says
it's done. Lost without a trace.
</details>

<details><summary>4. The payment gateway is down for an hour. What does this pipeline do, and what should it do?</summary>

As built, every order exhausts its retries and lands in the DLQ: an hour of
traffic to redrive. Better: detect that the error rate is near 100% for a
dependency and stop consuming (pause the main topic or let in-place retries
block with a long, bounded backoff) and alert. Lag grows, but the data stays
in the topic, in order, and resumes on its own when the dependency is back.
</details>

<details><summary>5. A redriven record fails again. How do you avoid an infinite main ↔ DLQ loop?</summary>

Count redrives in a header (`redrive-count`) and have the redrive tool skip
records that reached a limit, leaving them for a human. Redrive only after a
fix is deployed, and look at `-role dlq` first to see what you're about to
send.
</details>

## franz-go ↔ Java client names

| franz-go | Java / docs | Note |
|---|---|---|
| `cl.PauseFetchPartitions` / `ResumeFetchPartitions` | `consumer.pause()` / `resume()` | Java also keeps pauses until resumed, but not across a rebalance that revokes the partition |
| `cl.SetOffsets` | `consumer.seek()` | |
| `kgo.BlockRebalanceOnPoll` + `cl.AllowRebalance` | default Java behavior: rebalance callbacks run inside `poll()` | |
| (none) | `max.poll.interval.ms` (default 300000) | Java also sends it as the rebalance timeout |
| `kgo.RebalanceTimeout` (default 60s) | the JoinGroup rebalance timeout | |
| `kgo.DisableAutoCommit` | `enable.auto.commit=false` | |
| `kgo.AutoCommitMarks` + `cl.MarkCommitRecords` | `commitAsync(offsets)` of what you processed | |
| `kgo.OnPartitionsRevoked` / `OnPartitionsLost` | `ConsumerRebalanceListener.onPartitionsRevoked` / `onPartitionsLost` | |
| your retry topic + DLQ | Spring Kafka `@RetryableTopic`, `DeadLetterPublishingRecoverer`; Kafka Connect `errors.deadletterqueue.topic.name` | What most Java blog posts mean by "Kafka DLQ" |

## Further reading

- Uber Engineering: [Building Reliable Reprocessing and Dead Letter Queues with Apache Kafka](https://www.uber.com/blog/reliable-reprocessing/)
- Pulsar: [Messaging concepts](https://pulsar.apache.org/docs/next/concepts-messaging/) (negative acknowledgement, retry letter topic, dead letter topic)
- franz-go: [kgo.Client.PauseFetchPartitions](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.PauseFetchPartitions), [kgo.BlockRebalanceOnPoll](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#BlockRebalanceOnPoll)
- Kafka docs: [Consumer configs](https://kafka.apache.org/documentation/#consumerconfigs) (`max.poll.interval.ms`)
- [Kafka Improvement Proposals](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals): KIP-298 (error handling and DLQ in Connect), KIP-932 (share groups, task 16)
