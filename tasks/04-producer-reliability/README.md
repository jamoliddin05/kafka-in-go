# 04 · Producer reliability

> **Goal:** know where a record is between `Produce` and the ack, what makes
> it fail, and what you lose when the process stops. `Produce` is async, the
> client buffers, `Close` doesn't flush, and idempotence dedupes far less
> than its name suggests.

## Why this matters in prod

**Story 1: the deploy that dropped orders.** A Go service produced order
events with `cl.Produce(r.Context(), rec, nil)` and `defer cl.Close()`.
Every rolling deploy lost whatever sat in the client's buffer at SIGTERM:
a few hundred events per pod. Nothing logged an error, because the promise
was `nil`. A monthly reconciliation found 0.1% of orders without events,
clustered at deploy times. Two bugs: no `Flush` before `Close`, and the
records carried a context that was canceled before they were sent.

**Story 2: "idempotence is on, so no duplicates".** During a broker failover
a payment handler's `ProduceSync` took 6 seconds. The API gateway in front
gave up at 3 seconds and retried the HTTP call, and the second request
produced the same payment event again. Both writes succeeded. The broker
saw two different records from the producer's point of view: different
sequence numbers, same business event. Idempotence only dedupes the
client's own retries of one batch.

## Concepts

```
Produce(ctx, rec, promise)
   │ returns immediately (blocks only if the buffer is full)
   ▼
partitioner ──► per-partition buffer ── batch closes when: linger expires (10ms)
                 ▲                                         batch is full (1,000,012 B uncompressed)
                 │ MaxBufferedRecords 50,000                client is flushing / backed up
                 │                                    ▼
                 │                     compress (snappy) ──► produce request ──► leader ──► ISR
                 │                     ≤5 requests in flight per broker (idempotent)          │
                 └──────────── promise(rec, err) ◄──── ack, or an error once retries/timeout give up
```

- **Async by design.** `Produce` hands the record to a buffer and returns.
  The promise is the *only* place you learn whether it was written.
  `ProduceSync` is `Produce` + wait, one round trip per call (and it cuts the
  linger short). A `nil` promise means "I don't care if this is lost".
- **Batching.** One open batch per partition. In franz-go v1.22 it is sent
  when `ProducerLinger` expires (default **10ms**), when it reaches
  `ProducerBatchMaxBytes` (default **1,000,012 bytes**, uncompressed), or
  when the client is flushing or backed up. Linger only matters *below*
  saturation. When the producer is backed up, batches are full anyway
  (experiment A).
- **Compression** is per batch. The default is **snappy** (falling back to
  none). Bigger batches compress better, and a batch that would grow is
  sent uncompressed. The broker stores batches as received
  (`compression.type=producer`), and consumers decompress.
- **Buffer and backpressure.** `MaxBufferedRecords` defaults to **50,000**;
  `MaxBufferedBytes` is unlimited by default. When the buffer is full,
  `Produce` blocks until there's room or its ctx is done, and a blocked
  `Produce` also tells every partition to stop lingering. `TryProduce` fails
  the record with `kgo.ErrMaxBuffered` instead, and it does *not* cut lingers
  short (experiment E).
- **Retries and timeouts.** franz-go's defaults are unlimited
  `RecordRetries` and **no** `RecordDeliveryTimeout`. `ProduceRequestTimeout`
  (10s) is how long the *broker* may take per request. A failure fails every
  record buffered for that partition, so ordering stays gapless. Task 03 has
  the broker-side errors.
- **Idempotence** (on by default, requires `acks=all`). `InitProducerID`
  gives the client a producer ID and epoch. Every batch carries a
  per-partition sequence number, and the broker remembers the last 5 batches
  per (producer ID, partition): a resent batch is recognized and acked
  instead of written twice, and a gap is rejected. That's what lets franz-go
  keep 5 requests in flight per broker without reordering. It covers **the
  client's retries of the same batch**. It does not cover:
  - your second `Produce` of the same event (new sequence number);
  - a restarted process (new producer ID, experiment G);
  - another instance.

  For those you need event IDs and an idempotent consumer (task 10), or
  transactions (task 12).
- **Ambiguous outcomes.** "Timed out" is not "not written". franz-go's
  idempotent producer refuses to fail a batch whose fate it doesn't know (a
  request was sent and no definitive answer came back): context cancel and
  `RecordDeliveryTimeout` only fail batches that were never sent or got a
  definitive error. `Close` is the exception: it fails buffered *and*
  in-flight records with `kgo.ErrClientClosed`, and an in-flight one may
  still have been written.
- **`Flush` vs `Close`.** `Flush(ctx)` stops lingering and waits until every
  buffered record has an outcome. `Close` does **not** flush: it fails
  everything still buffered with `ErrClientClosed` (experiment C). SIGKILL
  loses the buffer with no promise at all. Java's `KafkaProducer.close()`
  and Pulsar's `close()` wait for pending sends; franz-go's doesn't.
- **The record's ctx.** A record inherits the ctx you pass to `Produce`. When
  that ctx is done, buffered records that haven't been sent fail with
  `context.Canceled`. Pass the signal ctx and your flush has nothing left to
  flush (experiment D).
- **Size limits.** The client rejects a record larger than
  `ProducerBatchMaxBytes` immediately with `MESSAGE_TOO_LARGE`. The broker
  checks the *compressed* batch against the topic's `max.message.bytes`
  (default 1,048,588, from the broker's `message.max.bytes`). Large payloads
  belong in object storage, with a reference in the event (claim check).
- **Hooks.** `kgo.WithHooks` with `HookProduceBatchWritten` reports every
  acked batch as `ProduceBatchMetrics` (records, uncompressed and compressed
  bytes, codec). franz-go calls it from a goroutine of its own, after the
  promises.

## Coming from Pulsar

| Pulsar | Kafka | The difference that bites |
|---|---|---|
| `sendAsync()` → `CompletableFuture` | `Produce(ctx, rec, promise)` | Promises run serially on one client goroutine. A promise that blocks on the client (a blocking `Produce`, `Flush`) can deadlock it |
| `batchingMaxPublishDelay` (1ms), `batchingMaxMessages` (1000) | `ProducerLinger` (10ms), `ProducerBatchMaxBytes` (~1 MB) | franz-go batches by bytes per partition, not by message count |
| `maxPendingMessages` / memory limit + `blockIfQueueFull` (default false: send fails when full) | `MaxBufferedRecords` / `MaxBufferedBytes`; `Produce` blocks, `TryProduce` fails with `ErrMaxBuffered` | Opposite defaults: Pulsar fails fast, franz-go blocks the caller |
| `sendTimeout` (30s) | `RecordDeliveryTimeout`, **unset** by default | A franz-go producer waits forever unless you set it |
| Broker dedup by `producerName` + sequence ID, off unless enabled (`brokerDeduplicationEnabled=false`) | Idempotent producer, on by default, scoped to one client session | Pulsar dedup can survive a producer restart (same name, resumed sequence IDs). Kafka idempotence can't: the new process gets a new producer ID |
| `close()` waits for pending sends | `Close()` fails pending records | Call `Flush` with a deadline first |
| `compressionType` NONE | snappy | |
| `chunkingEnabled` for large messages | nothing: `MESSAGE_TOO_LARGE` | claim-check pattern |

## Run it

```bash
make dev T=04-producer-reliability R=producer
go run ./tasks/04-producer-reliability -role count        # records per producer run that actually arrived
go run ./tasks/04-producer-reliability -role bench        # throughput, records/batch, compression (t04.bench)
```

Host runs take knobs inline, e.g.
`LINGER=1s RATE=5000 go run ./tasks/04-producer-reliability -role producer`.
Every producer run has a `run` ID and gapless `seq`; its last log line is a
`summary` (sent / acked / failed by error). Compare it with `count`.

The starter is deliberately naive: the promise does nothing (so `acked`
stays 0), records carry the signal ctx, nothing is flushed on exit, and
`bench` and `BACKPRESSURE=shed` exit with `not implemented yet`. The
reference is in [`solution/`](solution/main.go)
(`make dev T=04-producer-reliability/solution R=producer`).

## Your task

1. **TODO(04.1) Count outcomes.** In `tally.promise`: on success add to
   `acked`; otherwise count `classify(err)` in `failed` (under the mutex) and
   keep the first error. Keep it cheap: promises run serially.
   *Done when:* `stats` lines show `acked` following `sent`, and a run with
   `LINGER=1s RATE=5000` stopped with Ctrl+C prints a summary with
   `CLIENT_CLOSED=~2000`. Those are Story 1's lost records, now visible.
2. **TODO(04.2) Graceful shutdown.** Give records their own context
   (`context.WithoutCancel(ctx)` plus a cancel that fires `SHUTDOWN_TIMEOUT`
   after the signal, `context.AfterFunc` helps), so a signal doesn't cancel
   them. On exit, with `FLUSH_ON_EXIT=true`, `Flush` under that same deadline
   and only then `Close`.
   *Done when:* `LINGER=1s RATE=5000`, Ctrl+C after a few seconds prints
   `flushing buffered=~2400` and a summary with `acked == sent`, and `count`
   shows exactly that many records for the run.
3. **TODO(04.3) Batch metrics hook.** Implement `OnProduceBatchWritten` with
   atomics.
   *Done when:* `-role bench` prints batches, records/batch, KB/batch, the
   codec and the compression ratio, and with no knobs set the codec is
   `snappy`.
4. **TODO(04.4) Shed load.** With `BACKPRESSURE=shed`, use `cl.TryProduce`.
   *Done when:* `BACKPRESSURE=shed MAX_BUFFERED=1000 LINGER=50ms RATE=30000`
   shows `failed="MAX_BUFFERED=..."` and `buffered` never above 1000, while
   the same with `BACKPRESSURE=block` loses nothing but slows `sent` down.

## Experiments

**A. Linger only matters below saturation.** Unthrottled
(`-role bench`, 200,000 records), `LINGER=0`, `5ms` and `50ms` all give
~2,500-2,800 records per batch: the buffer is always backed up, so batches
fill to the byte limit. Throttled (`BENCH_RATE=5000 BENCH_N=25000`):

```
LINGER=0     avg 9.4 records/batch   ratio=2.93x  codec=none,snappy  p50=1.5ms
LINGER=5ms   avg 16.7 records/batch  ratio=3.17x  codec=snappy       p50=7.3ms
LINGER=50ms  avg 91.9 records/batch  ratio=3.68x  codec=snappy       p50=31.4ms
```

(The pacer sends in 10ms bursts, which is why `LINGER=0` still batches a
little.) `codec=none,snappy`: tiny batches that would grow under snappy went
out uncompressed. Which do you pay for in prod: broker CPU per request, or
30ms of latency?

**B. Compression.** `COMPRESSION=none|snappy|lz4|zstd|gzip -role bench`:

```
none    834.7 KB/batch on the wire  ratio=1.00x
snappy  197.4 KB/batch              ratio=3.85x
lz4     224.8 KB/batch              ratio=3.29x
zstd    131.8 KB/batch              ratio=5.35x
gzip    153.8 KB/batch              ratio=5.42x
```

Throughput numbers on a laptop with three brokers are noise; the bytes on
the wire (network, disk and retention) are what you pay for. Check what the
broker stored:
`docker compose exec kafka-1 /opt/kafka/bin/kafka-dump-log.sh --files /var/lib/kafka/data/t04.bench-0/00000000000000000000.log | grep -o "compresscodec: [a-z]*" | sort | uniq -c`.

**C. Stop the pod three ways.** `LINGER=1s RATE=5000` (a large buffer on
purpose), run ~5s, then `-role count`:
- `FLUSH_ON_EXIT=false`, Ctrl+C:
  `summary sent=27400 acked=25150 failed=2250 by_error="CLIENT_CLOSED=2250"`,
  and `count` says 25150. Close threw the buffer away, and only the promise
  told you.
- `kill -9 <pid>` (or `docker compose kill producer`): no summary at all.
  The last `stats` said `sent=24950 buffered=4800`, and `count` finds what
  made it.
- default (flush): `flushing buffered=2385`, then
  `summary sent=27500 acked=27500 failed=0`, and `count` agrees.

Does Kubernetes' `terminationGracePeriodSeconds` cover your `SHUTDOWN_TIMEOUT`
plus the time to stop accepting traffic?

**D. The signal ctx bug.** `PRODUCE_CTX=signal LINGER=1s RATE=5000`, Ctrl+C:

```
msg=flushing buffered=2450 timeout=10s
msg="flush did not finish" err="context canceled" buffered=2450
msg=summary sent=27600 acked=25150 failed=2450 by_error="CANCELED=2450"
```

The flush is there and does nothing: the records died with the ctx. Run the
*starter* the same way and look at `count`: `MISSING BELOW MAX` is sometimes
not 0 (run it a few times). Each partition's batches are cut at different
moments, so a later seq in one partition can make it while an earlier seq in
another doesn't. The loss isn't always a clean tail.

**E. Block vs shed.** `MAX_BUFFERED=1000 LINGER=50ms RATE=30000`, ~5s each:

```
BACKPRESSURE=block  summary sent=135300 acked=135300 failed=0
BACKPRESSURE=shed   summary sent=135600 acked=75600  failed=60000 by_error="MAX_BUFFERED=60000"
```

The brokers could take 30k/s. A *blocked* `Produce` makes franz-go stop
lingering and send right away. `TryProduce` doesn't, so the 1,000-record
buffer sits out each 50ms linger while new records are shed. Size the buffer
for at least `rate × linger`.

**F. A 2 MB record.** `VALUE_SIZE=2000000 BENCH_N=3 -role bench`:

```
default:                                 MESSAGE_TOO_LARGE: ... (uncompressed_bytes=2000080)            ← client-side, instant
BATCH_MAX_BYTES=4000000:                 records=3 ... avg 515.6 KB/batch on the wire  codec=snappy     ← accepted!
BATCH_MAX_BYTES=4000000 COMPRESSION=none MESSAGE_TOO_LARGE: ... (uncompressed_bytes=2000093, compressed_bytes=2000093)  ← broker
```

The broker limit applies to the compressed batch, so the same record passes
or fails depending on how well its content compresses. Is that a limit you
want to depend on?

**G. Idempotence is per session.** After a few producer runs:

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-dump-log.sh \
  --files /var/lib/kafka/data/t04.events-0/00000000000000000000.log | grep -o "producerId: [0-9]*" | uniq -c
```

Every run has its own `producerId`, and `baseSequence` starts again at 0.
A pod that crash-loops while retrying the same events writes them again
every time.

**H. Brokers down (doc only for the shared cluster).** Stop two brokers as in
task 03 experiment D. With the defaults (`DELIVERY_TIMEOUT=` empty),
nothing fails: `buffered` climbs to 50,000, then `Produce` blocks and `sent`
stops. With `DELIVERY_TIMEOUT=30s`, records fail with `RECORD_TIMEOUT` 30s
after they were buffered. With `BACKPRESSURE=shed` they are shed as soon as
the buffer is full. Which of the three does your API want?

## Edge cases and gotchas

- **`Close` doesn't flush.** Always `Flush(ctx)` with a deadline first, and
  log `BufferedProduceRecords()` if the deadline hits.
- **Don't produce with a request or signal ctx** unless canceling the record
  is what you want. The ctx is part of the record.
- **Never block inside a promise.** No `Flush`, no blocking `Produce`, no I/O.
  Hand work to another goroutine.
- **`MaxBufferedRecords` counts records, not bytes.** 50,000 × 1 MB is 50 GB
  of RAM. Set `MaxBufferedBytes` when payloads are large.
- **A timeout fails a whole partition's buffer**, not one record, to keep
  ordering gapless. Expect bursts of failures.
- **franz-go only fails idempotent batches it's sure about.** Under a network
  partition a record can stay pending far beyond `RecordDeliveryTimeout`.
  `AllowIdempotentProduceCancellation` trades duplicates for bounded latency.
- **Without idempotence, more than one request in flight can reorder** a
  partition on retry. franz-go defaults to 1 in flight when idempotence is
  disabled (`MaxProduceRequestsInflightPerBroker`).
- **`Timestamp` is set when you call `Produce`** (if unset), not when the
  batch is sent. Buffering time is part of the record's age.
- **Broker-side recompression.** If a topic sets `compression.type` to a
  codec other than what the producer sends, the broker recompresses every
  batch with its own CPU. Leave it at `producer`.
- **The batch hook is asynchronous.** Right after `Flush` returns, its totals
  can still be catching up (`bench` waits for them).

## Check yourself

<details><summary>1. SIGTERM arrives; the code does <code>defer cl.Close()</code> and returns. 3,000 records are buffered. What happens to them?</summary>

They are never sent. `Close` fails them with `kgo.ErrClientClosed`, so you
only know if you have promises. With SIGKILL you don't even get that. The
fix: a detached record ctx, `Flush` with a deadline, then `Close`.
</details>

<details><summary>2. A produce response is lost on the network and franz-go resends the batch. Duplicate? And if your code calls <code>Produce</code> again for the same order after a timeout?</summary>

The resend is not a duplicate: same producer ID, epoch and sequence, and the
broker recognizes it. Your second `Produce` is a new record with a new
sequence number, so the broker writes it again. Only an event ID plus an
idempotent consumer (or transactions) handles that.
</details>

<details><summary>3. Why does linger barely change batch size in the unthrottled bench, but grow it 10x at 5,000 records/s?</summary>

Unthrottled, the buffer is always ahead of the network, so a batch fills to
the byte limit before any linger would expire. At a modest rate a batch only
collects what arrives during the linger, so linger decides the batch size,
and with it latency, compression ratio and broker requests per second.
</details>

<details><summary>4. A service needs to send 3 MB JSON documents. Which limits are involved, and what would you do instead?</summary>

The client's `ProducerBatchMaxBytes` (uncompressed), the topic's
`max.message.bytes` / broker `message.max.bytes` (compressed), and the
consumers' fetch sizes and memory. Raising all of them works until the
payload grows again. The claim-check pattern stores the document in object
storage or a DB and publishes a small event with its key and checksum.
</details>

<details><summary>5. An HTTP handler calls <code>Produce</code> (blocking) and two brokers are down. What happens with franz-go defaults, and what would you change?</summary>

Records retry forever. After 50,000 records the buffer is full, and every
handler blocks in `Produce` until the request ctx is done (if you passed it)
or forever. Set `RecordDeliveryTimeout`, choose between blocking with a
bounded ctx and `TryProduce` plus 503, and alert on `BufferedProduceRecords`.
</details>

## franz-go ↔ Java client names

| franz-go | Java / docs | Note |
|---|---|---|
| `kgo.ProducerLinger` | `linger.ms` | franz-go 10ms; Java 5ms since 4.0 (KIP-1030), 0 before |
| `kgo.ProducerBatchMaxBytes` | `batch.size` (16 KiB target) / `max.request.size` (1 MiB) | franz-go has one cap per batch, 1,000,012 bytes |
| `kgo.ProducerBatchCompression` | `compression.type` | franz-go snappy, Java none |
| `kgo.MaxBufferedRecords` / `kgo.MaxBufferedBytes` | `buffer.memory` (32 MiB) | Java bounds bytes, franz-go records |
| blocking `Produce` | `max.block.ms` (60s) | franz-go blocks until the ctx is done |
| `kgo.TryProduce` | `max.block.ms=0` (roughly) | |
| `kgo.RecordDeliveryTimeout` | `delivery.timeout.ms` | franz-go unlimited, Java 120s |
| `kgo.RecordRetries` | `retries` | |
| `kgo.ProduceRequestTimeout` | `request.timeout.ms` | |
| idempotent by default / `kgo.DisableIdempotentWrite` | `enable.idempotence=true` (default since 3.0) | |
| 5 in flight when idempotent | `max.in.flight.requests.per.connection=5` | |
| `cl.Flush(ctx)` | `producer.flush()` | |
| `cl.Close()` (no flush) | `producer.close()` (waits for sends) | |
| `kgo.WithHooks` + `HookProduceBatchWritten` | `batch-size-avg`, `compression-rate-avg`, `record-queue-time-avg` metrics | |

## Further reading

- Kafka docs: [Producer configs](https://kafka.apache.org/documentation/#producerconfigs)
- KIP-98: [Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging) (the idempotent producer)
- KIP-679: [Producer will enable the strongest delivery guarantee by default](https://cwiki.apache.org/confluence/display/KAFKA/KIP-679%3A+Producer+will+enable+the+strongest+delivery+guarantee+by+default)
- KIP-1030: [Change constraints and default values for various configurations](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1030%3A+Change+constraints+and+default+values+for+various+configurations)
- franz-go: [Client.Produce](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.Produce), [hooks](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#HookProduceBatchWritten)
- Pulsar: [Message deduplication](https://pulsar.apache.org/docs/next/cookbooks-deduplication/)
