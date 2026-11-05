# 02 · Keys and ordering

> **Goal:** know exactly where a record goes and why. Kafka orders records
> only inside a partition, the *producer* picks the partition from the key,
> and the mapping silently changes when someone adds partitions.

## Why this matters in prod

**Story 1: "shipped" before "paid".** During a sale the fulfillment consumers
lagged, so someone ran `kafka-topics --alter --partitions 24` on
`order-events` (it had 12) to "add parallelism". For the next half hour the
fulfillment service rejected thousands of `shipped` events as invalid
transitions from `created`, and the orders got stuck. Nothing was lost. Every
event was in the topic, but half of the order IDs now hashed to a different
partition. The old events of an order sat in a partition with a backlog,
while its new events landed in a fresh, empty partition that a consumer read
immediately. The same orders trip the check again on every replay.

**Story 2: the tenant that ate a partition.** Events were keyed by
`merchant_id` "so that each merchant's events stay in order". One marketplace
merchant was 60% of Black Friday traffic: one partition, one consumer at 100%
CPU, lag measured in hours, eleven other consumers idle. Scaling the
deployment did nothing, because one key can only ever be in one partition.
The invariant the service needed was per *order*, not per merchant.

## Concepts

```
producer                                  topic t02.orders (3 partitions)
  key = "ord-3"                           ┌──────────────────────────────┐
  murmur2(key) = 0x3c4c65d5          ───► │ p0 │ created │ paid │ ...    │ ─► consumer A
  & 0x7fffffff % 3 = 0                    ├──────────────────────────────┤
                                          │ p1 │ ...                     │ ─► consumer B
  the broker never hashes anything:       ├──────────────────────────────┤
  it stores the batch where it's told     │ p2 │ ...                     │ ─► consumer C
                                          └──────────────────────────────┘
```

- **The client picks the partition.** The broker appends a batch to whatever
  partition the producer sent it to. So "which partition does key K live in"
  is a property of *every producer's* partitioner, not of the topic.
- **franz-go's default partitioner** (v1.22) is
  `UniformBytesPartitioner(64KiB, adaptive=true, keys=true, nil)`, a port of
  the Java client's KIP-794 partitioner:
  - **keyed record:** `murmur2(key) & 0x7fffffff % numPartitions`. This is the
    Java client's formula bit for bit (`Utils.toPositive(Utils.murmur2(key))`).
    You verify it against the Java console producer in experiment D.
  - **null key:** "sticky". It stays on one partition for ~64 KiB of records,
    then picks another one, preferring partitions with less buffered data. It
    makes large batches and is fair over time, but it is not round-robin.
  - `StickyKeyPartitioner(nil)` is the older KIP-480 behavior: same keyed
    hashing, sticky per batch for null keys.
- **Keyed records insist on their partition.** If that partition has no
  leader, franz-go buffers keyed records until it comes back. It routes
  null-key records to partitions that are writable (`RequiresConsistency` in
  `kgo/partitioner.go`). Ordering costs availability.
- **Ordering = same key + one partition + one consumer at a time.** Within a
  partition the producer keeps order even across retries (idempotence, task
  04), and a group gives each partition to one member (task 05). Across
  partitions there is no order at all.
- **Adding partitions remaps keys.** `h % 3` becomes `h % 6`. Existing records
  never move; only new writes follow the new mapping.

```
before: ord-3  → murmur2 % 3 = 0    p0: created  paid   ·          ·
after:  ord-3  → murmur2 % 6 = 3    p3:                   shipped  delivered

consumer of p0 has lag ──► reads "created, paid" late
consumer of p3 is idle  ──► reads "shipped, delivered" first  ⇒ out of order
```

  How much moves (`go run ./tasks/02-keys-and-ordering -role partition-of`
  with `COUNTS=...`): 3→4: 75%, 3→6: 50%, 4→6: 67%, 6→12: 50%, 8→12: 67%.
  The transition isn't an instant either. Each producer instance switches
  when *its* metadata refreshes (`metadata.max.age.ms`, 5 minutes by default
  in franz-go and Java), so for a while two instances write the same key to
  two partitions.
- **Choosing a key.** Use the entity whose invariant you protect, at the
  finest grain that still protects it: an order's state machine needs
  `order_id`, not `merchant_id`. A coarser key gives you nothing but skew.
- **Hot keys.** One key's throughput is capped by one partition and one
  consumer, and more partitions don't change that. The options:
  - a finer key;
  - salting with a *stable* sub-entity (`merchant#hash(order)%8`), which
    keeps per-order order and gives up per-merchant order;
  - a dedicated topic for the giant tenant.

  A *random* salt destroys ordering for that key entirely.
- **Custom partitioners** are a fleet-wide contract. Every producer, in every
  language, must compute the same partition: Go, Java, the Python service, the
  Kafka Connect sink that republishes, and Kafka Streams. librdkafka-based
  clients (confluent-kafka-python/-go/.NET) default to `consistent_random`
  (CRC32), and Sarama defaults to FNV-1a. Neither matches murmur2.

## Coming from Pulsar

| Pulsar | Kafka | The difference that bites |
|---|---|---|
| Producer hashes the key for partitioned topics; Java client `hashingScheme` default `JavaStringHash` (`String.hashCode()`), `Murmur3_32Hash` opt-in | Producer hashes with murmur2 (Java-compatible); other ecosystems differ (librdkafka CRC32, Sarama FNV-1a) | In both, the partition is chosen client-side, so mixed-language producers must agree on the hash or per-key order is gone |
| No key → `RoundRobinPartition` routing (the Java default) | Null key → sticky partition for ~64 KiB, then switch | Sticky placement keeps neighbours together, so a low-traffic test with null keys can look "ordered" (experiment A) |
| `Key_Shared` subscription: many consumers on **one** partition, the broker hashes keys into ranges and keeps per-key order | Per-key order only through partitions: one group member per partition | Kafka's per-key parallelism is capped by the partition count; past that you need in-process worker pools (task 13) |
| `update-partitioned-topic`: partitions can only be increased | `kafka-topics --alter --partitions`: only increase | Same remap problem on both sides. Pulsar Key_Shared also has to juggle hash ranges when consumers join; Kafka has nothing equivalent for repartitioning |

## Run it

```bash
make dev T=02-keys-and-ordering R="producer consumer"
make scale R=consumer N=3          # three group members, each with its own per-order state
make logs R=consumer
```

From the host (defaults point at `localhost:9092-9094`):

```bash
go run ./tasks/02-keys-and-ordering -role inspect                       # per-partition load and skew
go run ./tasks/02-keys-and-ordering -role partition-of                  # where keys go for 3 and 6 partitions
CHECK_TOPIC=t02.orders go run ./tasks/02-keys-and-ordering -role partition-of
go run ./tasks/02-keys-and-ordering -role consumer                      # an extra group member
```

Knobs live in [`task.env`](task.env). `METADATA_MAX_AGE=10s` is set so the
experiments don't take 5 minutes. The starter is deliberately wrong: the
producer **ignores `KEY_BY=order` and sends null keys**, and the consumer
exits with `not implemented yet` until you write the checker. The reference
is in [`solution/`](solution/main.go)
(`make dev T=02-keys-and-ordering/solution R="producer consumer"`).

## Your task

1. **TODO(02.1) Detect violations.** Implement `tracker.observe`: compare the
   event's `Seq` with the last seq stored for the order and classify it as
   `firstSeen` (no state yet, e.g. this instance just got the partition),
   `ok` (last+1), `duplicate` (== last), `late` (< last: an older state after
   a newer one) or `gap` (> last+1: a predecessor hasn't arrived). Store the
   *highest* seq seen, so one late event doesn't turn the next one into a
   false `gap`.
   *Done when:* with the starter producer (null keys) and three consumers
   (`make scale R=consumer N=3`), the `stats` lines show `gap>0` and the
   warnings print `got=delivered(4) after=created(1)`.
2. **TODO(02.2) Key by order.** Return the order ID as the key in `keyFor`.
   *Done when:* after a restart of the producer, every `stats` line of every
   member shows `late=0 gap=0`, and `inspect` shows roughly equal shares.
3. **TODO(02.3) murmur2.** Port Kafka's `Utils.murmur2` (seed `0x9747b28c`,
   `m = 0x5bd1e995`, `r = 24`, 4-byte little-endian blocks, a tail of 1-3 bytes,
   final mix `h ^= h>>13; h *= m; h ^= h>>15`). `partitionFor` already masks the
   sign bit and takes the modulo.
   *Done when:* `-role partition-of` prints `true` in `MATCHES FRANZ-GO` for
   every key and exits 0, and experiment D agrees with the Java producer.
4. **TODO(02.4) Salt the hot merchant.** `KEY_BY=merchant-salted`: key =
   `merchant#n` with `n = crc32(order ID) % SALT`. n comes from the order,
   never from `rand`.
   *Done when:* with `HOT_MERCHANT_PCT=60`, `inspect` skew drops from ~4 with
   `KEY_BY=merchant` to ~1.3, and the consumer still reports `late=0 gap=0`.

## Experiments

**A. Null keys look fine in a quiet test.** `KEY_BY=none ACTIVE_ORDERS=50`
with three consumers: the producer's `per_partition` shows one or two
partitions per 5s window, and violations are rare, because an order's four
events are written close together inside one sticky window. Now set
`ACTIVE_ORDERS=1000` (each order lives ~15s): `gap` counts appear on every
member. Same code, different traffic shape. Which one does prod look like?

**B. Add partitions while producing.** This is Story 1 on your laptop, with
`KEY_BY=order`, where you saw zero violations:

```bash
make dev T=02-keys-and-ordering R=producer                    # terminal 1
go run ./tasks/02-keys-and-ordering -role consumer            # terminal 2, let it run, then Ctrl+C
docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:29092 \
  --alter --topic t02.orders --partitions 6
# wait ~20s: the producer's per_partition switches from p0-p2 to p0-p5
go run ./tasks/02-keys-and-ordering -role consumer            # terminal 2 again
```

The restarted consumer has a backlog on p0-p2 and reads p3-p5 from the
start, in parallel. You'll see something like:

```
level=WARN msg="ordering violation" kind=late order=ord-44RM-3298 got=created(1) after=shipped(3) partition=0 prev_partition=3
level=INFO msg=stats ok=4114 first_seen=2434 duplicate=0 late=181 gap=0 ...
```

`prev_partition` is always `partition+3`: the order moved from `h%3` to `h%6`.
Did the consumer that was never stopped (lag 0) have a problem? What changes
if the consumer is merely slow (`PROCESS_TIME=10ms`, ~100 records/s)?

**C. The log remembers.** `CHECK_TOPIC=t02.orders go run ./tasks/02-keys-and-ordering -role partition-of`:

```
t02.orders has 6 partitions now. Records by the partition count that explains them, newest first:
  murmur2 % 6    20319 records
  murmur2 % 3    6699 records
  none of [6 3]  0 records
  null key       8027 records
```

The `% 3` records stay where they are until retention deletes them. Replay
with a new group (`GROUP=t02.replay go run ... -role consumer`) and the same
orders are reported again: a backfill or a new service reading from the
start hits the remap every time.

**D. A Java producer agrees with you.**

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:29092 \
  --create --topic t02.java-probe --partitions 6
printf 'ord-1:a\nord-2:b\nord-3:c\nord-4:d\nm-000:e\n' | docker compose exec -T kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:29092 --topic t02.java-probe \
  --reader-property parse.key=true --reader-property key.separator=:
CHECK_TOPIC=t02.java-probe COUNTS=6 go run ./tasks/02-keys-and-ordering -role partition-of
```

Every record should be `murmur2 % 6`, matching the `P@6` column. What would
a Python service on confluent-kafka with default settings have produced?

**E. The hot merchant.** `KEY_BY=merchant HOT_MERCHANT_PCT=60`, run for a
minute, then `WINDOW=30s go run ./tasks/02-keys-and-ordering -role inspect`:

```
PARTITION  LEADER  RECORDS  LAST 30s  SHARE
2          3       13597    4020       67.2%  #################################
...
skew (hottest / mean) over the last 30s: 4.03
```

Run three consumers with `PROCESS_TIME=10ms` and watch one member's lag grow
(`make group G=t02.fulfillment`) while the others idle. Then switch to
`KEY_BY=merchant-salted`. Skew drops, but not to 1.0. Look at
`KEYS=m-000#0,m-000#1,m-000#2,m-000#3,m-000#4,m-000#5,m-000#6,m-000#7 COUNTS=6 go run ./tasks/02-keys-and-ordering -role partition-of`: 8 salts on 6
partitions put two salts on some partitions and none on others. What did
you give up compared to `KEY_BY=merchant`, and does this service care?

## Edge cases and gotchas

- **`auto.offset.reset=latest` + new partitions = silent skip.** If a
  producer starts writing to a new partition before your group notices it,
  a consumer that starts "at the end" skips those records. franz-go starts
  partitions without a commit at the *earliest* offset by default
  (`ConsumeStartOffset`). The Java consumer defaults to `latest`.
- **Key bytes, not key values, are hashed.** `"123"`, the int64 `123` and the
  JSON `"\"123\""` go to different partitions. A refactor that changes the
  key serializer is a repartitioning.
- **An empty key is not a null key.** `[]byte{}` is hashed (murmur2 of zero
  bytes), so every empty-key record goes to one fixed partition. Only `nil`
  gets sticky placement.
- **You can't decrease partitions**, and there is no "rehash in place". To
  change the partition count of a keyed topic safely, create a new topic,
  move producers, drain the old topic, then move consumers (or accept the
  window and make the consumer tolerate it, e.g. by version checks, task 10).
- **Ordering inside a partition also needs the producer.** With idempotence
  off and more than one request in flight, a retried batch can land after a
  later one. franz-go's default (idempotent, up to 5 in flight) keeps order;
  task 04.
- **Kafka Streams/ksqlDB co-partitioning** assumes both topics use the same
  partitioner and partition count. A custom partitioner breaks joins silently.
- **Partition count is a capacity decision made up front.** Size it for the
  peak parallelism you'll need in two years, not for today's consumer count.

## Check yourself

<details><summary>1. Where is the decision "key K goes to partition P" made, and what does the broker validate about it?</summary>

In the producer's partitioner. The broker only checks that the partition
exists and that it is the leader. Two producers with different partitioners
can put the same key in two partitions, and Kafka will never notice.
</details>

<details><summary>2. A topic goes from 8 to 12 partitions. Roughly what share of keys change partition, and which records move?</summary>

About two thirds (h%8 == h%12 only for h mod 24 in 0..7, so 8 of 24 stay).
No existing record moves; only new writes follow the new mapping, and each
producer instance switches at its own metadata refresh.
</details>

<details><summary>3. Your consumers had zero lag when partitions were added. Are you safe?</summary>

Mostly, for the live path: the old partitions' records were already
processed when the new ones started filling. But any lag spike in the next
minutes (a deploy, a rebalance) and every future replay or new consumer
group will see the remapped keys out of order.
</details>

<details><summary>4. Why is a random salt worse than <code>merchant#hash(order)%8</code>?</summary>

A random salt spreads one order's events across partitions, so even
per-order ordering is gone. A salt derived from the order ID keeps all of
one order's events on one key. You lose only cross-order (per-merchant)
ordering, which this service doesn't need.
</details>

<details><summary>5. A Python service starts producing to the same keyed topic with confluent-kafka defaults. What breaks?</summary>

librdkafka's default partitioner is `consistent_random` (CRC32), so the same
order ID lands in a different partition than the Go and Java producers put
it. Set `partitioner=murmur2_random` there, which is the Java-compatible
one.
</details>

## franz-go ↔ Java client names

| franz-go | Java / docs | Note |
|---|---|---|
| `kgo.RecordPartitioner(...)` | `partitioner.class` | unset in Java = built-in partitioner |
| `kgo.UniformBytesPartitioner(64<<10, true, true, nil)` (default) | built-in partitioner (KIP-794), `batch.size`, `partitioner.adaptive.partitioning.enable=true`, `partitioner.ignore.keys=false` | Java switches sticky partitions per `batch.size` (16 KiB), franz-go per 64 KiB |
| `kgo.StickyKeyPartitioner(nil)` | `DefaultPartitioner` (sticky since 2.4, deprecated in 3.3) | KIP-480 |
| `kgo.RoundRobinPartitioner()` | `RoundRobinPartitioner` | |
| `kgo.ManualPartitioner()` + `Record.Partition` | `new ProducerRecord(topic, partition, key, value)` | |
| `kgo.KafkaHasher(murmur2)` | `Utils.toPositive(Utils.murmur2(key)) % n` | librdkafka: `partitioner=murmur2_random` |
| `kgo.MetadataMaxAge` | `metadata.max.age.ms` | 5 min default in both |
| `kgo.ConsumeStartOffset` / `ConsumeResetOffset` | `auto.offset.reset` | franz-go starts new partitions at earliest |

## Further reading

- KIP-480: [Sticky Partitioner](https://cwiki.apache.org/confluence/display/KAFKA/KIP-480%3A+Sticky+Partitioner)
- KIP-794: [Strictly Uniform Sticky Partitioner](https://cwiki.apache.org/confluence/display/KAFKA/KIP-794%3A+Strictly+Uniform+Sticky+Partitioner)
- Kafka docs: [Producer configs](https://kafka.apache.org/documentation/#producerconfigs)
- franz-go: [Partitioner docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Partitioner)
- librdkafka: [CONFIGURATION.md (`partitioner`)](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
- Pulsar: [Messaging concepts, Key_Shared](https://pulsar.apache.org/docs/next/concepts-messaging/)
