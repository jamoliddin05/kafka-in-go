# 11 · Transactional outbox

> **Goal:** write a business row and its event so that either both happen or
> neither does, without a distributed transaction. The event goes into an
> outbox table in the same Postgres transaction as the order, and a relay
> publishes it to Kafka afterwards. Learn what that costs (latency,
> duplicates, ordering rules) and how the trace survives the trip through a
> table.

## Why this matters in prod

**Story 1: paid, never shipped.** Checkout committed the order, then
published `order.created`. Every deploy lost a few events: SIGTERM arrived
between the commit and the produce, the produce was cancelled with the
request context, the process exited. The API even returned 500, so clients
retried and created *second* orders. A reconciliation job found about 0.1%
of orders that the warehouse had never heard of, weeks later.

**Story 2: the phantom order.** The team "fixed" it by publishing first and
committing second. Now, whenever the insert failed (a constraint violation
on a retried request, a serialization failure, a statement timeout), the
event was already out. Downstream services emailed customers and reserved
stock for orders that did not exist. A lost event can be re-sent. **A
phantom event can't be taken back.**

## Concepts

**The dual-write problem.** Two independent systems, two commits, and a
process that can die between them. No order of the two is safe:

```
db-first                            kafka-first
BEGIN; INSERT order; COMMIT  ✓      produce order.created  ✓
        ✗ crash / produce fails             ✗ crash / INSERT fails
produce order.created               BEGIN; INSERT order; COMMIT
→ order without an event (lost)     → event without an order (phantom)
```

Kafka transactions don't help: they cover Kafka partitions and consumer
offsets only, and Postgres can't take part in them.

**The outbox.** Make the event part of the database transaction, and move
it to Kafka later:

```
api                                  relay (polling publisher)                         consumers
BEGIN                                BEGIN
  INSERT INTO t11_orders ...           SELECT ... FROM t11_outbox
  INSERT INTO t11_outbox                 WHERE published_at IS NULL
    (event_id, payload,                  ORDER BY id LIMIT 100
     trace_context)                      FOR UPDATE SKIP LOCKED
COMMIT     both or neither           ProduceSync(batch)   acks=all, idempotent  ──────►  t11.order-events
                                     UPDATE t11_outbox SET published_at = now() ...
                                     COMMIT
```

Where the relay can die, and what happens:

- **Before the produce:** nothing happened. The rows are still unpublished
  and the next poll takes them.
- **After the produce, before the commit:** the records are in Kafka, the
  rows are still unpublished, and the next poll publishes them again.
  **Duplicates.** The outbox turns "lost or phantom" into "at least once",
  and consumers dedupe by `event_id` (task 10).

So: mark a row only after Kafka acked it (`ProduceSync` with `acks=all`,
both franz-go defaults), never before.

**Poll by state, not by cursor.** IDs come from a sequence at INSERT time,
but transactions commit in a different order. A relay that remembers
`last_id` and asks for `id > last_id` misses a row whose transaction
committed after a higher ID was already published, and misses it forever.
`WHERE published_at IS NULL` (with a partial index) finds it on the next
poll.

**More than one relay.** Without a locking clause both relays read the same
rows and publish everything twice. `FOR UPDATE` makes the second relay wait
for the first one's transaction, then it skips the rows that are now
published: no duplicates, but the relays take turns. `FOR UPDATE SKIP
LOCKED` lets them work on different rows at the same time. The price:
relay A holds rows 1–100, relay B takes 101–200 and may publish first. If
one order had `created` at 100 and `paid` at 101, consumers see `paid`
first. Per-aggregate order needs one active relay (an advisory lock or
leader election) or relays that split the work by aggregate
(`WHERE hashtext(record_key) % 2 = 0`). Within one relay, order holds: rows
go out in ID order, keyed by order ID, and the next batch waits for the
previous one's acks.

**The trace goes through the table.** The api stores its trace context
(W3C `traceparent` `00-<trace-id>-<span-id>-<flags>`, plus `tracestate` and
`baggage` if present) in the row's `trace_context` column, using a
`propagation.MapCarrier`. The relay extracts it, starts its `send` span as a
child and injects that into the Kafka headers (task 08). One trace per
order:

```
t11-api       POST /orders ─┐
t11-api        create t11.order-events ─┐   context stored in t11_outbox.trace_context
t11-relay                                └─ send t11.order-events        (≈ POLL_INTERVAL later)
t11-consumer                                  └─ process t11.order-events
```

The gap between `create` and `send` is your outbox latency, right there in
Jaeger. The trade-off: if the relay was down for an hour, that trace has an
hour-long hole. Some teams start a new trace in the relay and *link* it to
the stored context instead, as the OTel messaging conventions describe for
batch sends. A republished row shows up as a second `send` and a second
`process` span in the same trace.

**CDC instead of polling.** Debezium reads the Postgres WAL through logical
replication and publishes outbox rows (its Outbox Event Router). No polling
load, lower latency, and WAL order is commit order, so the late-commit
problem disappears. The price is Kafka Connect plus a replication slot to
operate: an abandoned slot keeps WAL around until the disk fills. CDC on
your *business* tables is the tempting shortcut; it makes your table schema
the public event contract. CDC on an outbox keeps the contract explicit.

**Kafka leaves the request path.** In outbox mode the api never talks to
Kafka. A broker outage becomes a growing outbox instead of failed requests.
Postgres is now the only thing an order depends on, and the outbox's oldest
unpublished row is the number to alert on.

## Coming from Pulsar

| Pulsar | Kafka | The difference that bites |
|---|---|---|
| Transactions span Pulsar topics and acks | Transactions span Kafka partitions and offsets | Neither can include your Postgres commit. You need an outbox with both |
| Broker dedup with a relay-supplied `sequenceId` (for example the outbox row ID) and a stable producer name | Idempotence lives in one producer session: a restarted relay gets a new producer ID or epoch, and its sequence numbers start over | A Pulsar relay can make its republishes after a crash disappear at the broker. On Kafka they always reach consumers, so consumers must dedupe |
| Pulsar IO Debezium source connectors | Debezium on Kafka Connect | The same CDC option on both sides |
| Message properties carry the trace context | Record headers carry it | Same propagation code, different carrier (task 08) |

## Run it

```bash
make up                                                       # once: brokers, UI, Jaeger, Postgres
make dev T=11-transactional-outbox R="api consumer"           # the starter's api does a dual write
go run ./tasks/11-transactional-outbox -role load             # 200 POST /orders against localhost:8080
go run ./tasks/11-transactional-outbox -role check            # orders in Postgres vs events on the log
go run ./tasks/11-transactional-outbox -role reset            # truncate t11_* and empty the topic (stop api and relay first)
curl -s -XPOST localhost:8080/orders -d '{"customer":"c-1","amount":1999}'
make psql                                                     # SELECT * FROM t11_outbox ORDER BY id DESC LIMIT 5;
```

After TODOs 11.1 and 11.2, set `WRITE_MODE=outbox` in [`task.env`](task.env)
and run `make dev T=11-transactional-outbox R="api relay consumer"`.

`check` reads `t11.order-events` from the start and compares it with
`t11_orders`: orders without an event are **lost**, events without an order
are **phantom**, and outbox rows not published yet are in flight, not lost.
Duplicates are reported but are not an error.

**Traces.** Jaeger UI at http://localhost:16686, services `t11-api`,
`t11-relay`, `t11-consumer`. `-role load` prints a link to one trace, and
every log line written inside a span carries `trace_id`. From a terminal:
`curl -s localhost:16686/api/traces/<trace_id>` or
`/api/v3/traces/<trace_id>`. The Jaeger 2.21 in this stack answers 404 to the
old `/api/traces?service=...` search.

The roles also run from the host (debugger), with the same names in Jaeger.
Use another port if the compose api holds 8080:
`WRITE_MODE=outbox HTTP_ADDR=:18080 go run ./tasks/11-transactional-outbox -role api`
and `API_URL=http://localhost:18080` for `load`.

**The starter's api is deliberately naive and it runs.** `WRITE_MODE=db-first`
commits the order and then produces, and `FAIL_PCT=10` makes 10% of
requests die in between: `check` shows lost orders. `kafka-first` does the
opposite and shows phantoms. `outbox` answers 500 `not implemented yet`, and
the relay exits with the same message, until you write them. The reference
is in [`solution/`](solution/):
`make dev T=11-transactional-outbox/solution R="api relay consumer"` (with
`WRITE_MODE=outbox`).

## Your task

1. **TODO(11.1) Outbox write.** In `withOutbox`: inject the trace context
   into a `propagation.MapCarrier`, then in one `pgx.BeginFunc` transaction
   insert the order and the outbox row (`insertOutbox` is given). Keep the
   `a.crash()` check between the two inserts.
   *Done when:* with `WRITE_MODE=outbox`, `load` gets about 10% 500s, and
   with no relay running `check` prints `OK so far: N events are still
   waiting in the outbox` with 0 lost and 0 phantom. In `make psql`,
   `SELECT trace_context->>'traceparent' FROM t11_outbox LIMIT 1;` shows a
   `00-...-01` value.
2. **TODO(11.2) The relay.** `publishBatch`: lock a batch of unpublished
   rows with `FOR UPDATE SKIP LOCKED`, build the records, start a `send` span
   per record from the stored context (`startSend`), `ProduceSync` them under
   `PRODUCE_TIMEOUT`, then `crashBeforeMark()`, mark and commit.
   *Done when:* `check` prints `OK: every order has its event and every event
   has its order` with 0 duplicates; one trace in Jaeger holds spans from
   `t11-api`, `t11-relay` and `t11-consumer`; the consumer's `event_age`
   stays below about `POLL_INTERVAL` (runs here logged 314ms and 488ms with
   the default 500ms).
3. **TODO(11.3) Dedupe in the consumer.** `firstDelivery` claims the
   `event_id` in `t11_inbox` and reports whether it was new.
   *Done when:* with `CRASH_BEFORE_MARK=3 BATCH=20` on the relay, `check`
   reports duplicates and still `OK`, and the consumer logs exactly that many
   `duplicate event skipped` lines.

## Experiments

Each run: `-role reset` (with the api and relay stopped), start the roles,
`-role load`, wait a few seconds, `-role check`.

**A. Lost events.** `WRITE_MODE=db-first`. Expect about 20 of 200 orders
without an event (one run here: `orders without an event (lost): 19`). The
api logs `create order failed ... after the DB commit, before the produce`
and the client got a 500 for an order that exists. What does a client that
retries on 500 do to you now? (An idempotency key on `POST /orders` is the
API-side twin of the consumer's inbox.)

**B. Phantom events.** `WRITE_MODE=kafka-first`. Now `check` shows about 10%
`events without an order (phantom)`, and the consumer happily "notified"
those customers. Which of A and B would you rather explain to finance?

**C. Atomic.** `WRITE_MODE=outbox` with the relay running. Still about 10%
500s, but `check` prints `OK`: a request that died left neither an order nor
an event.

**D. Kafka down is a backlog, not an outage.** Stop the relay (or don't
start it), run `load` with `ORDERS=300`: every request still gets its 201 or
its simulated 500, because Kafka isn't on the request path. `check` shows
`outbox rows not published yet: <every order>` and 0 lost. Start the relay:
it drains in batches of 100, and the consumer's `event_age` equals the time
the relay was down (10 seconds down here gave `event_age=10.198s`). What
would you alert on: the row count, or the age of the oldest unpublished row?

**E. Crash before mark.** Relay with `CRASH_BEFORE_MARK=3 BATCH=20`. Every
third batch is produced and then rolled back, so it goes out again: one run
here gave `256 (178 distinct event_ids, 78 duplicates)` and the consumer
skipped all 78. Open the trace of a skipped event (the log line has its
`trace_id`): one `POST /orders`, two `send` spans at different offsets, two
`process` spans, the second one tagged `duplicate=true`.

**F. Two relays.** `make scale T=11-transactional-outbox R=relay N=2` (or two
host relays with different `INSTANCE`), `ORDERS=1000`. Both log `published`,
`check` shows 0 duplicates. Now delete `FOR UPDATE SKIP LOCKED` from the
relay's query and repeat: one run here published 1772 records for 911
orders. With plain `FOR UPDATE` it's 0 duplicates again, but the relays
alternate. If each order had several events, what would two `SKIP LOCKED`
relays do to their order?

**G. Brokers down (do it on your own machine).** `make broker-stop B=2` and
`make broker-stop B=3`: with `min.insync.replicas=2`, `acks=all` writes can't
succeed. Expect the relay to log `batch failed, its rows stay in the
outbox` roughly every `PRODUCE_TIMEOUT` while the api keeps answering 201. `make broker-start B=2`
and `B=3`, and the backlog drains. Without `PRODUCE_TIMEOUT`, how long would
the relay hold the row locks? (franz-go retries forever by default.)

**H. Look inside the outbox.**

```sql
SELECT id, event_id, trace_context->>'traceparent' AS traceparent,
       published_at - created_at AS outbox_latency
FROM t11_outbox ORDER BY id DESC LIMIT 5;
```

## Edge cases and gotchas

- **The relay holds a transaction across a network call.** That's fine
  while Kafka answers in milliseconds. `PRODUCE_TIMEOUT` bounds the bad
  case; without it franz-go retries forever (no `RecordRetries` or
  `RecordDeliveryTimeout` limit by default) and the rows stay locked.
- **A failed batch is republished whole.** If 99 of 100 records were acked,
  the rollback still publishes all 100 again. More duplicates, never a gap.
- **Write the aggregate row before the outbox row.** Two transactions
  touching the same order then serialize on the order's row lock *before*
  taking outbox IDs, so ID order matches commit order for that order.
- **Outbox bloat.** Every row is inserted, updated and eventually deleted.
  Delete published rows soon (`DELETE ... WHERE published_at < now() -
  interval '1 hour'`, in batches), or have the relay `DELETE ... RETURNING`
  instead of marking, or partition by day. Watch autovacuum on this table.
- **The payload is JSONB.** Postgres normalizes it (key order, whitespace),
  so the bytes on Kafka are not the bytes the api marshaled. Use `bytea` if
  consumers hash or sign payloads.
- **The outbox payload is a public contract** (task 14). Write an event, not
  a dump of the table row.
- **A 500 doesn't mean "nothing happened"**, in any mode: the response can
  fail after COMMIT. Clients that retry need an idempotency key.
- **The consumer's side effect can't join its inbox transaction.** This one
  claims the event first, so a crash right after the claim skips the
  notification (at most once for that record). Real senders pass `event_id`
  to the provider as an idempotency key.
- **Sampling.** The SDK's default sampler is parent-based, so the flags in
  the stored `traceparent` decide whether the relay and consumer spans are
  recorded. A row written while sampling was off stays unsampled downstream.
- **Latency.** Polling adds up to `POLL_INTERVAL`. `LISTEN/NOTIFY` from the
  api can wake the relay early; keep the poll as the safety net.

## Check yourself

<details><summary>1. Why not wrap the DB commit and the produce in a Kafka transaction?</summary>

A Kafka transaction makes writes to Kafka partitions (and consumer offset
commits) atomic. Postgres can't take part, and Kafka has no XA/2PC with
external databases. The crash between "Postgres committed" and "Kafka
committed" is still there.
</details>

<details><summary>2. The relay crashes after ProduceSync returned but before COMMIT. What do consumers see?</summary>

The same events again on the next poll: same `event_id`, new offsets.
Nothing is lost. Consumers that dedupe by `event_id` (task 10) see no
difference; the others process them twice.
</details>

<details><summary>3. Why poll with `published_at IS NULL` instead of `id > last_id`?</summary>

Sequence values are taken at INSERT, but transactions commit in any order.
A row with a lower ID can become visible after a higher one was already
published; a cursor has moved past it and never sees it. Polling by state
picks it up on the next round.
</details>

<details><summary>4. You run two relays with SKIP LOCKED. What guarantee is gone?</summary>

Per-aggregate ordering across batches. The relays publish different
batches concurrently, so a later event of an order can reach Kafka before an
earlier one held by the other relay. Use one active relay or split the work
by aggregate key.
</details>

<details><summary>5. In outbox mode the brokers are down for 10 minutes. What do users and consumers notice?</summary>

Users notice nothing: the api only needs Postgres. Consumers get 10 minutes
of events late, in a burst, once the relay drains the backlog. Alert on the
age of the oldest unpublished row, not only on Kafka metrics.
</details>

## franz-go ↔ Java client names

| franz-go | Java / docs | Note |
|---|---|---|
| `cl.ProduceSync(ctx, recs...)` | `send()` each record, then `flush()` or `get()` every future | returns once every record is acked or failed |
| `kgo.RequiredAcks(kgo.AllISRAcks())` (default) | `acks=all` (default since 3.0) | |
| idempotence on by default, `kgo.DisableIdempotentWrite()` | `enable.idempotence=true` | dedupes retries within one producer session only |
| `kgo.RecordDeliveryTimeout` (default: none) | `delivery.timeout.ms` (default 120000) | franz-go retries until the ctx or this timeout ends it |
| `kgo.RecordRetries` (default: unlimited) | `retries` (default `Integer.MAX_VALUE`) | |
| `kgo.Record.Headers` | `ProducerRecord.headers()` | where `traceparent` travels |
| `kgo.ConsumerGroup`, autocommit every 5s | `group.id`, `enable.auto.commit`, `auto.commit.interval.ms` | |

## Further reading

- Chris Richardson: [Pattern: Transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html), [Polling publisher](https://microservices.io/patterns/data/polling-publisher.html), [Transaction log tailing](https://microservices.io/patterns/data/transaction-log-tailing.html)
- Debezium: [Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html) and [Reliable Microservices Data Exchange With the Outbox Pattern](https://debezium.io/blog/2019/02/19/reliable-microservices-data-exchange-with-the-outbox-pattern/)
- PostgreSQL: [The locking clause (FOR UPDATE, SKIP LOCKED)](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE)
- W3C: [Trace Context](https://www.w3.org/TR/trace-context/)
- OpenTelemetry: [Semantic conventions for messaging spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/)
- Kafka docs: [Message delivery semantics](https://kafka.apache.org/documentation/#semantics)
