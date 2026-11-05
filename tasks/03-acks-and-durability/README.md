# 03 · Acks and durability

> **Goal:** know what an ack from Kafka actually promises. `acks=all` waits
> for the *current* in-sync replicas, `min.insync.replicas` decides when the
> leader refuses to accept writes, and durability comes from copies on
> different machines, not from fsync.

## Why this matters in prod

**Story 1: the acked payments that vanished.** A payments producer ran with
`acks=1` "for latency". A broker hit a kernel panic during peak. The
controller elected a follower as the new leader, and when the old broker
came back it truncated its log to match the new leader. The producer had
acks for 212 records that no longer existed anywhere. No error was logged:
from the client's point of view every write had succeeded. The gap showed up
two days later in reconciliation.

**Story 2: the rolling restart that took writes down.** A team set
`min.insync.replicas=3` on RF=3 topics "to be extra safe". The next routine
rolling restart of the brokers made every partition on the restarting broker
reject `acks=all` writes with `NOT_ENOUGH_REPLICAS`, one broker at a time,
for the whole maintenance window. franz-go retried silently (its default is
to retry forever), so the API's `ProduceSync` calls hung until the load
balancer timed them out.

## Concepts

```
partition 0, RF=3, min.insync.replicas=2

leader   (broker 1)  │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │   LEO 7  (log end offset: next offset to write)
follower (broker 2)  │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │       LEO 6
follower (broker 3)  │ 0 │ 1 │ 2 │ 3 │ 4 │           LEO 5
                                         ▲
                     HW 5 = what every ISR member has. Consumers see offsets < 5.

acks=0    "sent": the client doesn't wait for any reply
acks=1    the leader appended it (offset 6 above): one copy, in one page cache
acks=all  the HW passed it: every replica *currently in the ISR* has it
```

- **ISR (in-sync replicas)** is the set of replicas that are caught up with
  the leader. A follower drops out when it hasn't caught up for
  `replica.lag.time.max.ms` (30s), or when the KRaft controller fences its
  broker: on controlled shutdown immediately, after a crash once
  `broker.session.timeout.ms` (9s) passes without heartbeats. It rejoins
  after catching up. The ISR is dynamic, so `acks=all` means "all of the ISR
  right now", which may be one replica.
- **`min.insync.replicas`** is the floor. With ISR smaller than it, the leader
  rejects `acks=all` produce requests with **`NOT_ENOUGH_REPLICAS`** (code 19)
  *before* appending, so nothing is written and a retry is safe. If the ISR
  shrinks *after* the append while the leader waits for followers, the reply
  is **`NOT_ENOUGH_REPLICAS_AFTER_APPEND`** (code 20): the record is in the
  leader's log, but the ack condition wasn't met. Outcome unknown.
  Kafka's own default is `min.insync.replicas=1`; this cluster sets 2 at the
  cluster level. The effective value is `min(min.insync.replicas, RF)`: an
  RF=1 topic accepts `acks=all` (you'll see it in experiment F).
- **RF=3, min ISR=2** survives one broker loss for writes and two for reads.
  **min ISR = RF** turns any single restart into a write outage (Story 2).
- **Leader failover:** the controller elects a new leader, and the old leader
  later truncates anything above what the new leader has. That truncated tail
  is exactly the `acks=1` loss in Story 1. `unclean.leader.election.enable`
  (false here and by default) would allow an out-of-sync replica to become
  leader, which trades acked data for availability.
- **Producers notice a move** through `NOT_LEADER_FOR_PARTITION` (code 6,
  which Kafka now calls `NOT_LEADER_OR_FOLLOWER`; kerr keeps the old name) or
  a dead connection. franz-go refreshes metadata, or uses the new-leader hint
  in the produce response (KIP-951), and retries. Your code never sees the
  error unless retries are bounded.
- **Durability = replication, not fsync.** `log.flush.interval.messages` and
  `log.flush.interval.ms` are effectively off by default (check
  `kafka-configs --describe --all`). An ack means "in memory on N brokers".
  You survive power loss by putting those N brokers in different failure
  domains: `broker.rack` = availability zone, and RF=3 spreads across 3 AZs.
- **Eligible Leader Replicas (KIP-966).** This cluster runs Kafka 4.3 with
  `eligible.leader.replicas.version=1`, which the docs say is on by default
  for new clusters from 4.1. It changes three things:
  1. **The HW cannot advance while the ISR is smaller than min ISR.** `acks=1`
     and `acks=0` writes are still appended by the leader, but consumers
     don't see them until the ISR is back to min ISR. Before ELR, those
     writes became visible and were the first to be lost.
  2. **When the ISR drops below min ISR**, the replicas that were in it are
     recorded as *eligible leader replicas*: replicas guaranteed to have
     everything up to the HW. The ISR itself may now shrink to empty.
  3. **Leader election order:** an ISR member, else an unfenced ELR member,
     else the last known leader once it is unfenced. Previously, losing the
     last ISR member meant waiting for *that* broker; now any ELR member can
     take over without losing committed data.

  Side effects: a cluster-level `min.insync.replicas` is created (you can
  see `DYNAMIC_DEFAULT_BROKER_CONFIG` in `inspect`), broker-level overrides
  are rejected, and changing a topic's `min.insync.replicas` clears its ELR
  state. The 4.3 docs describe ELR as part 1 of KIP-966.
  `unclean.leader.election.enable` is still the switch for unclean
  elections. `NOT_ENOUGH_REPLICAS` for `acks=all` works as before.
  `kafka-topics --describe` and `-role inspect` show `Elr` and `LastKnownElr`
  per partition.
- **KRaft quorum.** In this compose cluster every node is broker *and*
  controller. Leader elections and ISR changes need an active controller,
  which needs 2 of the 3 voters. Lose two nodes and the cluster can't change
  metadata at all. Prod clusters run dedicated controllers for exactly this
  reason.

## Coming from Pulsar

| Pulsar | Kafka | The difference that bites |
|---|---|---|
| Ledger ensemble E / write quorum Qw / ack quorum Qa (`managedLedgerDefault*`, 2/2/2 in broker.conf) | RF / ISR / `min.insync.replicas` + `acks=all` | When a bookie fails, BookKeeper replaces it in the ensemble and keeps acking at Qa. Kafka's `acks=all` waits for whoever is in the ISR now, and `min.insync.replicas` is the only floor |
| Bookies write a journal and fsync it before acking | Leader and followers ack from the page cache | Kafka's guarantee is "N copies on N machines". Spread them across racks/AZs, or a simultaneous power loss eats acked data |
| Producer always waits for Qa acks | `acks=0` / `acks=1` exist | `acks=1` is a latency knob that gives up the acked tail on every leader failover (experiment E) |
| Brokers are stateless; ownership moves, ledgers are fenced | Partition leader election from ISR/ELR, then truncation of the old leader's divergent tail | Pulsar fences the old writer. Kafka rewrites the old leader's log, and that's where `acks=1` data dies |
| Nothing elects a stale copy | `unclean.leader.election.enable=true` | Availability over data. Keep it false for anything with money in it |

## Run it

```bash
make dev T=03-acks-and-durability R="producer consumer"
WATCH=1s go run ./tasks/03-acks-and-durability -role inspect     # leader / ISR / ELR / HW, refreshed every second
make describe TOPIC=t03.payments                                 # the same from the Kafka CLI
```

From the host:

```bash
ACKS=leader RATE=5000 go run ./tasks/03-acks-and-durability -role producer
TOPIC=t03.strict go run ./tasks/03-acks-and-durability -role producer
GROUP=t03.audit-2 go run ./tasks/03-acks-and-durability -role consumer
```

The producer prints one `acks` line per second:
`sent`, `acked`, `failed` (by error), `buffered` (records waiting inside the
client), `p50/p99/max` ack latency. The consumer prints `audit` lines:
`missing_total` counts sequence numbers that were never seen (per producer
run and partition), `duplicates_total` counts sequence numbers seen twice.
`missing` with `failed=""` on the producer side is **acked data loss**.

The starter runs with `ACKS=all`. `ACKS=leader|none`, the consumer and the
partition table of `inspect` exit with `not implemented yet`, and the
starter producer never fails a record (see step 2). The reference is in
[`solution/`](solution/main.go)
(`make dev T=03-acks-and-durability/solution R="producer consumer"`).

## Your task

1. **TODO(03.1) acks=1 and acks=0.** In `producerOpts`, add
   `kgo.RequiredAcks(kgo.LeaderAck())` and `kgo.NoAck()`. Try it without
   anything else first: `kgo.NewClient` fails with
   `idempotency requires acks=all`. The broker can only deduplicate retries
   of writes the whole ISR agreed on, so franz-go makes you say
   `kgo.DisableIdempotentWrite()` explicitly.
   *Done when:* `ACKS=leader` and `ACKS=none` producers run and their p50 is
   clearly lower than `ACKS=all` (experiment A).
2. **TODO(03.2) Make failures visible.** franz-go's defaults are an unlimited
   delivery timeout and unlimited retries, so a retriable error like
   `NOT_ENOUGH_REPLICAS` never reaches your promise. Add
   `kgo.RecordDeliveryTimeout` (`DELIVERY_TIMEOUT`) and `kgo.RecordRetries`
   (`RECORD_RETRIES`). Then make `classify` return `ke.Message` for a
   `*kerr.Error` (`errors.AsType`) and short labels for
   `kgo.ErrRecordTimeout`, `kgo.ErrRecordRetries`, `kgo.ErrClientClosed` and
   context errors.
   *Done when:* in experiment C the stats say `failed="RECORD_TIMEOUT=..."`
   with the default 15s timeout, and
   `failed="NOT_ENOUGH_REPLICAS=..."` with `DELIVERY_TIMEOUT=0 RECORD_RETRIES=5`.
3. **TODO(03.3) Loss detector.** In `lossDetector.observe`, compare the
   record's seq with the next expected seq for its (producer, partition):
   equal → fine; smaller → duplicate; larger → a hole of `seq - want`
   records starting at `want`. The first record of a stream is never a hole,
   because the group may join mid-run.
   *Done when:* an `ACKS=all` run shows `missing_total=0 duplicates_total=0`
   in every `audit` line, and experiment E can show a hole.
4. **TODO(03.4) ISR and ELR.** The Metadata API doesn't return ELR, so
   `describePartitions` sends `kmsg.DescribeTopicPartitionsRequest` (API key
   75, added by KIP-966) with `req.RequestWith(ctx, cl)`.
   *Done when:* `-role inspect` prints LEADER, EPOCH, REPLICAS, ISR, ELR,
   LAST KNOWN ELR and HW, and matches `make describe TOPIC=t03.payments`.

## Experiments

The broker experiments below stop and kill containers. Always
`make broker-start B=N` afterwards and wait for `inspect` to show `ok`
everywhere.

**A. The price of acks.** Run the producer for ~10s with each setting
(`LINGER=0`, `RATE=500`):

```
ACKS=all     p50=1.42ms  p99=5.21ms
ACKS=leader  p50=730µs   p99=1.71ms
ACKS=none    p50=160µs   p99=430µs
```

These numbers are from one laptop with three brokers on one disk; across AZs
`acks=all` adds a network round trip to the slowest ISR follower. Now try
`LINGER=10ms` (the franz-go default) with `ACKS=all`: what dominates the
latency now?

**B. One broker down, acks=all.** Run producer + consumer, keep
`WATCH=1s -role inspect` open, then `make broker-stop B=2`. Leadership moves
off broker 2, it disappears from every ISR (controlled shutdown), and the
producer barely blinks. Bring it back with `make broker-start B=2` and watch
it rejoin the ISR after catching up. Leaders move back later
(`auto.leader.rebalance.enable`, checked every 300s). Repeat with
`make broker-kill B=2` (SIGKILL): nothing tells the controller, so `acks=all`
latency spikes until the broker is fenced (`broker.session.timeout.ms`, 9s).
Did the consumer report anything missing in either case?

**C. min.insync.replicas = RF.** `TOPIC=t03.strict` for producer and
consumer, then `make broker-stop B=2`. Every partition has a replica on
broker 2, so every partition is now under min ISR. The producer's `acked`
drops to 0, `buffered` grows, and after `DELIVERY_TIMEOUT` the promises fail.
With the starter's step 2 not done, they never fail: `buffered` climbs to
50,000 (`MaxBufferedRecords`, ~100s at `RATE=500`) and then `Produce` blocks,
so `sent` drops to 0 too. Run it again with `KAFKA_LOG_LEVEL=debug` and find
`NOT_ENOUGH_REPLICAS` in the produce response logs. That's the only place
the cause appears when the promise says `RECORD_TIMEOUT`.

**D. Two brokers down.** `make broker-stop B=2`, then `make broker-stop B=3`,
with the producer started *before* the first stop (creating topics needs the
controller). After the second stop only one node is left: under min ISR
everywhere, and below KRaft quorum, so no further ISR change or election can
happen until a node returns.
- `ACKS=all`: `NOT_ENOUGH_REPLICAS` (or `RECORD_TIMEOUT`), as in C.
- `ACKS=leader`: the producer keeps getting acks, but the consumer's
  `records` drops to 0. The HW is frozen (ELR rule 1), so the consumer
  group's lag looks like zero. `inspect` shows `UNDER_MIN_ISR` and the
  stopped broker in `ELR`.
- Reads of data below the HW still work: `GROUP=t03.audit-2 ... -role consumer`
  reads the topic from the start.

`make broker-start B=3`: the backlog arrives at the consumer at once, and
`e2e_max` is roughly the outage. Was any of it lost? What would have happened
to those `acks=1` records if broker 1 had died before 2 and 3 returned?

**E. acks=1 + a dead leader.** `ACKS=leader RATE=5000`, consumer running.
Find the leader of partition 0 in `inspect`, then `make broker-kill B=<it>`.
Look for `records missing` warnings while the producer reports `failed=""`.
The window is the few milliseconds of data the followers hadn't fetched yet,
so a kill may lose nothing. Repeat a few times. Then do the same with
`ACKS=all`: you should never get a hole.

**F. RF=1.** `TOPIC=t03.rf1`. First note that `acks=all` works on it:
effective min ISR is `min(2, RF=1) = 1`, so "all" means one copy. Stop the
broker that leads partition 1. `inspect` shows it `OFFLINE`, franz-go logs
`produce partition load error` warnings, and partition 1's records stop
being acked while the other two carry on. This producer picks partitions
itself; with the default partitioner, null-key records would be routed
around the offline partition and keyed ones would wait (task 02). Usually
partition 1's records fail with `RECORD_TIMEOUT`. But if a request was in
flight when the broker died, the idempotent producer can't know whether that
batch was written and holds the partition until the broker answers, past
the delivery timeout. Repeat with `ACKS=leader` (idempotence off) and
compare. The data comes back with the broker. If that disk had died, it
would be gone for good.

## Edge cases and gotchas

- **franz-go never gives up by default.** No delivery timeout, unlimited
  retries, then `Produce` blocks at 50,000 buffered records. A `ProduceSync`
  in an HTTP handler hangs. Set `RecordDeliveryTimeout`, or put a deadline
  on the context you pass to `Produce`.
- **`ErrRecordTimeout` hides the cause.** It doesn't wrap the last broker
  error. `RecordRetries` fails with the broker error itself; debug logs show
  every retry.
- **Idempotent producers won't fail a batch they aren't sure about.** After a
  `REQUEST_TIMED_OUT`, a `NOT_ENOUGH_REPLICAS_AFTER_APPEND` or a request
  whose response never arrived, franz-go keeps retrying past the delivery
  timeout: failing it could let a later retry create a duplicate the broker
  can't detect. `kgo.AllowIdempotentProduceCancellation` flips that trade-off.
- **min.insync.replicas above RF is silently capped to RF** (try an RF=2
  topic with `min.insync.replicas=3`: `acks=all` succeeds). Lint topic
  configs in whatever creates them.
- **With min ISR 1, `acks=all` decays to `acks=1`** as soon as the ISR shrinks
  to the leader, with no error anywhere. That's Kafka's default.
- **A frozen HW looks like a healthy, idle topic.** Consumer lag is measured
  against the HW, so it stays 0. Alert on under-min-ISR partitions and on
  producer `buffered`, not only on lag.
- **Leader moves are invisible to `acks=all` producers but not to latency.**
  A SIGKILLed broker costs up to `broker.session.timeout.ms` per affected
  partition. Graceful shutdown is cheap.
- **Rack awareness:** `broker.rack` places replicas in different AZs.
  Consumers can read from the closest replica with `kgo.Rack` (KIP-392),
  which saves cross-AZ traffic, not durability.

## Check yourself

<details><summary>1. RF=3, min ISR=2, one follower has been out of the ISR for a minute. You get an ack for an <code>acks=all</code> write. How many copies exist?</summary>

Two: the leader and the remaining in-sync follower. `acks=all` waits for the
current ISR, not for RF.
</details>

<details><summary>2. Why does min ISR=2 keep a topic writable through a rolling restart, while min ISR=3 doesn't?</summary>

A restarting broker leaves the ISR of its partitions. With RF=3 the ISR is
then 2, which still meets 2 but not 3, so with min ISR=3 every partition
with a replica on that broker rejects `acks=all` writes until it catches up.
</details>

<details><summary>3. The producer got <code>NOT_ENOUGH_REPLICAS_AFTER_APPEND</code>. Is the record in the topic?</summary>

Maybe. The leader appended it, then the ISR shrank before enough replicas
confirmed. Whether it survives depends on what happens to that leader. A
retry with idempotence is safe (a duplicate is detected); without
idempotence it can create a duplicate. Contrast `NOT_ENOUGH_REPLICAS`:
nothing was written.
</details>

<details><summary>4. ELR is on and two brokers are down. An <code>acks=1</code> producer keeps getting acks. What do consumers see, and what happens if the last broker dies before the others come back?</summary>

Consumers see nothing new: the HW can't advance below min ISR. If the last
broker dies, a replica from the ELR (it has everything up to the HW) can be
elected. The `acks=1` records above the HW existed only on the dead leader
and are truncated when it returns. No `acks=all` write was accepted in that
window, so none is lost.
</details>

<details><summary>5. Kafka acks from the page cache. What does that imply for where you put the brokers?</summary>

Durability comes from independent copies, so the copies must not share a
failure: different racks or AZs (`broker.rack`), different power. Three
brokers on one hypervisor host with RF=3 survive a process crash, but not
losing that host.
</details>

## franz-go ↔ Java client names

| franz-go | Java / docs | Note |
|---|---|---|
| `kgo.RequiredAcks(kgo.AllISRAcks())` | `acks=all` (`-1`) | default in both (Java since 3.0) |
| `kgo.LeaderAck()` / `kgo.NoAck()` | `acks=1` / `acks=0` | |
| `kgo.DisableIdempotentWrite()` | `enable.idempotence=false` | franz-go refuses `acks≠all` without it; Java turns idempotence off unless you set it explicitly |
| `kgo.RecordDeliveryTimeout` | `delivery.timeout.ms` | franz-go: unlimited, Java: 120s |
| `kgo.RecordRetries` | `retries` | both effectively unlimited |
| `kgo.ProduceRequestTimeout` | `request.timeout.ms` | franz-go 10s for produce, Java 30s |
| `kgo.AllowIdempotentProduceCancellation` | (no equivalent) | |
| `kgo.Rack` | `client.rack` | follower fetching, KIP-392 |
| `kmsg.DescribeTopicPartitionsRequest` | `Admin.describeTopics` (4.x) | carries ELR / LastKnownELR |

## Further reading

- Kafka docs: [Replication](https://kafka.apache.org/documentation/#replication)
- Kafka 4.3 docs: [Eligible Leader Replicas](https://kafka.apache.org/43/operations/eligible-leader-replicas/)
- KIP-966: [Eligible Leader Replicas](https://cwiki.apache.org/confluence/display/KAFKA/KIP-966%3A+Eligible+Leader+Replicas)
- KIP-392: [Allow consumers to fetch from closest replica](https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica)
- franz-go: [RequiredAcks and producer options](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#RequiredAcks)
- Pulsar: [Architecture overview (BookKeeper)](https://pulsar.apache.org/docs/next/concepts-architecture-overview/)
