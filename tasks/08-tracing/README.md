# 08 · Tracing and correlation

> **Goal:** follow one HTTP request through two Kafka hops under a single
> `trace_id`, know when a consumer span should be a child and when it should
> be a link, and know every place where the chain silently breaks.

## Why this matters in prod

**Story 1: the trace that ends at the topic.** Checkout's "order placed →
confirmation e-mail" p99 went from 2 s to 40 s. Jaeger showed the HTTP request
finishing in 30 ms, and the e-mail service had thousands of short traces per
minute that started from nowhere. HTTP was instrumented, Kafka was not: nothing
put `traceparent` into the record headers. Two engineers spent a day joining
logs by order id. After a 40-line fix the answer was one click away: a 39 s
gap between `send` and `process`, which was consumer lag on one partition.

**Story 2: the helpful re-publisher.** An enrichment service consumed events,
added fields and produced them again by building a fresh record, so every
trace downstream of it started over. A second team "fixed" it by copying
all headers and then appending a new `traceparent`. Kafka allows duplicate
header keys. Go code that read the first one and Java code that read
`headers.lastHeader(...)` now disagreed about who the parent was, and the
traces in Jaeger made no sense.

## Concepts

```
one trace_id end to end (LINK=false)

t08-api        POST /orders                      SERVER
t08-api          └─ send t08.orders              PRODUCER   injects its own context into the headers
                      ┊   the record waits in the partition: consumer lag is this gap
t08-processor         └─ process t08.orders      CONSUMER   extracted from the headers, parent = send
t08-processor              ├─ payment-gateway charge  CLIENT
t08-processor              └─ send t08.payments  PRODUCER   injects again: same trace, new parent
t08-consumer                    └─ process t08.payments  CONSUMER
```

- **W3C Trace Context in headers.** The propagator writes
  `traceparent: 00-<trace-id 32 hex>-<parent span id 16 hex>-<flags>` and
  optionally `tracestate`. Flag `01` means sampled. `baggage: k=v,...`
  (W3C Baggage) rides along in its own header. `app.SetupTracing` installs a
  composite TraceContext + Baggage propagator. On Kafka these are plain record
  headers: an ordered list of (string key, bytes value), duplicates allowed.
- **The carrier** is the adapter between the propagator and your transport:
  `Get`, `Set`, `Keys` over `[]kgo.RecordHeader`. `Set` must replace an
  existing key, otherwise a forwarded record ends up with two `traceparent`s.
- **Producer span**, kind `PRODUCER`, named `send <topic>`. Inject **this**
  span's context, not the HTTP span's, or the consumer becomes a sibling of
  the send instead of its child. The span ends when the broker acks, so its
  duration is the produce latency, batching included.
- **Consumer span**, kind `CONSUMER`, named `process <topic>`. Its parent
  context comes from the record headers, never from the poll loop's `ctx`: one
  `PollFetches` returns records from many different traces.
- **Child or link?** A span has exactly one parent but any number of links.
  The OTel messaging conventions say a process span should link to the
  message's creation context, and may also use it as the parent when it
  processes a single message. Use the parent when the hop is part of the same
  user-visible operation (this pipeline, milliseconds to seconds). Use a link
  when the gap is unbounded or the relationship isn't 1:1:
  - batch processing: 500 records in one DB transaction is one span with 500
    links, it can't have 500 parents;
  - DLQ redrive hours later (task 09), replaying 3 hours of the log (task 01):
    as children they would graft new spans onto traces that ended long ago;
  - fan-in, where one output is built from many inputs.
  The cost of links: no single waterfall, and `grep trace_id` stops working
  across hops. Correlate by business id instead (below).
- **Messaging attributes** (semconv v1.43, the newest in otel-go v1.47:
  `go.opentelemetry.io/otel/semconv/v1.43.0`):

  | Attribute | Example | Where |
  |---|---|---|
  | `messaging.system` | `kafka` | both |
  | `messaging.operation.type` / `.name` | `send` / `process` | both |
  | `messaging.destination.name` | `t08.orders` | both |
  | `messaging.kafka.message.key` | `o-sa4eopnxci` | both; your business id becomes searchable in Jaeger |
  | `messaging.destination.partition.id` | `"2"` (a string) | producer after the ack, consumer |
  | `messaging.kafka.offset` | `17` | producer after the ack, consumer |
  | `messaging.consumer.group.name` | `t08.billing` | consumer |

- **Sampling travels in the flags.** The SDK default is
  `ParentBased(AlwaysSample)`: each hop obeys the sampled bit it received.
  An unsampled request still gets real IDs that flow everywhere (your logs
  have them), but no hop records spans. With links, each hop is a new root and
  decides again on its own.
- **Logs.** The kit's slog handler adds `trace_id` and `span_id` to any log
  call made with a context that carries a span. That's why every log line in
  this task uses `InfoContext(ctx, ...)` with the span's ctx.
- **Business id vs trace id.** `trace_id` identifies one execution: it's
  sampled, kept for days, and changes on retry or replay. `order_id` lives as
  long as the order: in the key, the payload, the DLQ, the replay, the
  support ticket. Log both, and put the business id on spans (here it's the
  key) so you can search traces by it.
- **What breaks propagation:** a producer that isn't instrumented (Kafka
  Connect, `kafka-console-producer`, a legacy service), code that builds a new
  record without copying or injecting headers, async hand-offs that drop the
  ctx (`go func()` with `context.Background()`), batching that merges many
  inputs into one output, and anything that rewrites records such as a
  mirroring tool with header filtering. `docker compose logs | grep` by
  trace id across services is the fastest way to find the gap.
- **kotel** (`github.com/twmb/franz-go/plugin/kotel`) does all of this with
  client hooks: a `publish` span from produce-buffered to ack, a `receive`
  span from fetch-buffered to poll (time the record sat in the client's
  buffer), and a `process` span you start with `WithProcessSpan(rec)`. v1.7.1
  names spans `<topic> publish|receive|process` and uses the older semconv
  v1.18 attributes (`messaging.source.name`, `messaging.kafka.source.partition`,
  `messaging.kafka.consumer.group`, `messaging.operation=publish`). Dashboards
  built on current names won't match its spans.

## Coming from Pulsar

| Pulsar | Kafka | The difference that bites |
|---|---|---|
| Message properties (`Map<String,String>`) | Record headers: ordered list, `[]byte` values, duplicate keys allowed | Your carrier decides which duplicate wins; make `Set` replace |
| Client interceptors / OTel Java agent instrumentation | `kgo.WithHooks` (the interceptor equivalent); kotel is the ready-made OTel hook | Nothing is instrumented unless you add the hook or the code |
| `reconsumeLater` / DLQ topics carry the properties for you | Retry and DLQ topics are your code (task 09) | Forget to copy headers and the trace ends at the retry topic |
| `batchReceive()` is opt-in | `PollFetches` always returns a batch from many traces | Never use the poll ctx as the parent; extract per record |
| Message `eventTime` / `publishTime` | Record timestamp (`CreateTime` by default) | Use it to measure time-in-topic; span gaps show the same thing |

## Run it

```bash
make dev T=08-tracing R="api processor consumer"
curl -s -X POST localhost:8080/orders -d '{"customer":"c-42","amount_cents":1999}'
# {"order_id":"o-sa4eopnxci","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"}
```

Open Jaeger at http://localhost:16686, pick service `t08-api`, Find Traces.
Or ask its API (Jaeger 2.21 serves search under `/api/v3`; the old
`/api/traces?service=...` search returns 404):

```bash
T=$(curl -s -X POST localhost:8080/orders | jq -r .trace_id); sleep 6
curl -s localhost:16686/api/v3/traces/$T | jq -r '.result.resourceSpans[]
  | (.resource.attributes[] | select(.key=="service.name") | .value.stringValue) as $svc
  | .scopeSpans[].spans[] | [$svc, .name, .spanId, (.parentSpanId // "-")] | @tsv'
```

Spans are exported in batches every 5 s (`OTEL_BSP_SCHEDULE_DELAY`), hence
the `sleep`.

From the host, run each role in its own terminal. kit names the services
`t08-*` after the package and exports plaintext gRPC to `localhost:4317`:

```bash
go run ./tasks/08-tracing -role processor
go run ./tasks/08-tracing -role consumer
go run ./tasks/08-tracing -role api            # HTTP_ADDR=:18080 if 8080 is taken
```

Knobs live in [`task.env`](task.env). The starter runs end to end: orders
flow api → processor → consumer, but **the trace stops at the first topic**.
Jaeger shows a lone `POST /orders` span, and the processor's and consumer's
logs have no `trace_id`. That's the bug you're fixing. The reference is in
[`solution/`](solution/main.go):
`make dev T=08-tracing/solution R="api processor consumer"`.

## Your task

All of it happens in [`tracing.go`](tracing.go), except step 5.

1. **TODO(08.1) Carrier.** Implement `Get`, `Set` (replace an existing key)
   and `Keys` on `headerCarrier`.
   *Done when:* after step 2, `make tail TOPIC=t08.orders` prints a
   `traceparent:00-...-01` header on new records.
2. **TODO(08.2) Producer span.** In `produceTraced`: start a `PRODUCER` span
   `send <topic>` with the attributes from the table, inject the span's ctx
   with `otel.GetTextMapPropagator().Inject`, produce, then set partition and
   offset (or record the error and set the status) and end the span.
   *Done when:* the api's trace shows `POST /orders → send t08.orders` with
   partition and offset attributes, and the parent id inside the record's
   `traceparent` is the **send** span's id.
3. **TODO(08.3) Consumer span.** In `startProcessSpan`: `Extract` from the
   record headers into ctx, then start a `CONSUMER` span `process <topic>`
   with the consumer attributes.
   *Done when:* one trace holds 6 spans from `t08-api`, `t08-processor` and
   `t08-consumer`, and
   `docker compose logs api processor consumer | grep <trace_id>` prints the
   "order accepted", "order charged" and "customer notified" lines.
4. **TODO(08.4) Links.** With `link=true` start the span with
   `trace.WithNewRoot()` and `trace.WithLinks(trace.LinkFromContext(ctx))`.
   *Done when:* with `LINK=true` the api's trace ends at `send t08.orders`,
   the processor's `process` span is the root of its own trace with a link
   to the api's send span (the Jaeger UI shows it as a `FOLLOWS_FROM`
   reference), and the consumer's trace links to the processor's send.
5. **TODO(08.5) Optional: kotel.** In `newPipe`, when `TRACER=kotel`, add
   `kgo.WithHooks(kotel.NewKotel(kotel.WithTracer(t)).Hooks()...)` with
   `t := kotel.NewTracer(kotel.ConsumerGroup(group))`. Make `send` a plain
   `ProduceSync(ctx, rec)` (the hook starts the span from `rec.Context`, which
   franz-go sets to the produce ctx) and `startProcess` return
   `t.WithProcessSpan(rec)`. `kotel.LinkSpans()` is the link mode.
   *Done when:* `TRACER=kotel` produces one trace with
   `t08.orders publish → t08.orders receive → t08.orders process → ...`.

## Experiments

**A. Grep across services.** Post an order, then
`docker compose logs api processor consumer | grep <trace_id>`. Now restart
processor and consumer with `LINK=true` and post again. The same grep finds
only the api line. `grep <order_id>` still finds all three. Which id do you
want in your on-call runbook?

**B. Bring your own trace.** Send a sampled parent, then an unsampled one:

```bash
curl -s -X POST localhost:8080/orders -H 'traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'
curl -s -X POST localhost:8080/orders -H 'traceparent: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00'
make tail TOPIC=t08.payments
```

The first trace is complete in Jaeger, and its root `POST /orders` has a
parent id (`00f067aa0ba902b7`) that Jaeger never received a span for. The second has `trace_id=0af76519...` in all three
services' logs and a `traceparent:...-00` header on `t08.payments`, yet
`/api/v3/traces/0af7651916cd43dd8448eb211c80319c` returns 404. The decision
not to sample was made at the edge and obeyed by every hop.

**C. Sample at the edge.** Uncomment the two `OTEL_TRACES_SAMPLER` lines in
`task.env` and post 12 orders. About a quarter show up in Jaeger, and each
one is complete across all three services, because the processor and
consumer follow the parent's flag. Now add `LINK=true`: the processor and
consumer roll their own dice on their own new trace ids. What happens to the
odds of seeing a complete request?

**D. Baggage.** `curl -s -X POST localhost:8080/orders -H 'baggage: tenant=acme'`.
The consumer logs `tenant=acme` although the processor's code never mentions
baggage: it extracted it with the trace context and injected it again on the
next produce. `make tail TOPIC=t08.payments` shows the header. What happens
when someone puts a JWT or an e-mail address into baggage?

**E. The gap is lag.** Stop the processor, post a few orders, wait 20 s,
start it again. In Jaeger the `send t08.orders` span ends at +3 ms and
`process t08.orders` starts at +20 s. The consumer logs `e2e=20.3s` or so.

**F. kotel vs hand-rolled.** Run the solution with `TRACER=kotel`. Compare
span names, kinds and attribute names with your own. Then add `LINK=true`:
`receive` becomes a new root linked to `publish`, and `process` becomes
*another* new root linked to `receive`, so one hop is now two traces.

## Edge cases and gotchas

- **TLS by default.** `otlptracegrpc` uses TLS unless the endpoint has an
  `http://` scheme or `OTEL_EXPORTER_OTLP_INSECURE=true`. Point it at a
  plaintext collector with `OTEL_EXPORTER_OTLP_ENDPOINT=jaeger:4317` and every
  export fails after the 10 s timeout with
  `tls: first record does not look like a TLS handshake`, logged once per
  batch, and the spans are dropped. The app itself keeps working, so nobody
  notices until they need a trace. Compose sets `http://jaeger:4317`, and kit
  falls back to plaintext `localhost:4317` when the variable is unset.
- **Shutdown flush.** The batch processor holds up to 5 s of spans. A role
  that exits without calling the shutdown func from `SetupTracing` loses
  exactly the spans you need after a crash-looping deploy.
- **Inject after `Start`.** Injecting the incoming ctx instead of the
  producer span's makes the consumer a sibling of `send`.
- **Async produce.** With `Produce` + promise, end the producer span in the
  promise, not after `Produce` returns, or the span measures nothing.
- **Forwarding records.** `&kgo.Record{Value: v}` has no headers: the trace
  ends there. Copying `rec.Headers` into the new record and then injecting
  is fine only because `Set` replaces.
- **Non-instrumented producers.** No `traceparent` header means `Extract`
  returns ctx unchanged: the process span becomes a new root, and in link
  mode the empty link is dropped by the SDK.
- **Replays.** Re-consuming 3 hours of the log in child mode attaches fresh
  spans to traces that ended hours ago. Backends with trace-duration limits
  split or drop them. Use links for replays and redrives.
- **Clock skew.** Spans from different hosts use different clocks. A child
  can appear to start before its parent. Don't compute lag from span
  timestamps across hosts; use consumer-group lag (task 06).
- **kotel's process ctx** descends from `rec.Context` (set by its fetch hook),
  not from your poll ctx, so cancellation doesn't reach it.
- **Cost.** One span per record at 50k records/s is real money. Sample at the
  edge, and for bulk consumers prefer one span per batch with links.

## Check yourself

<details><summary>1. Why can't the processor just use the ctx it passed to PollFetches as the parent of its process span?</summary>

That ctx is the process's lifetime ctx, it has no span. One fetch returns
records produced by many different requests, each with its own
`traceparent`. The parent has to come from each record's headers.
</details>

<details><summary>2. A consumer writes 500 records to Postgres in one transaction. How do you model the spans?</summary>

One `process` span for the batch with 500 links, one per record's creation
context (optionally with `messaging.batch.message_count`). It can't be a child
of 500 parents. If you also need per-record detail, add short per-record spans
that are children of the batch span and link to their producer.
</details>

<details><summary>3. Your consumer logs carry a trace_id, but Jaeger returns 404 for it. Name three causes.</summary>

The upstream sent the unsampled flag (`-00`), so nobody recorded spans. The
exporter can't reach the collector (TLS mismatch, wrong endpoint, and the
spans were dropped). The process died before the batch processor flushed, or
the spans were never ended.
</details>

<details><summary>4. When is order_id a better correlation key than trace_id?</summary>

Whenever the work isn't one continuous execution: retries through a retry
topic, DLQ redrive, replays, batch jobs, link-mode hops, unsampled requests,
and any search older than the trace backend's retention. The trace id is for
"what happened in this execution", the business id is for "what happened to
this order".
</details>

<details><summary>5. Where does the time between "send" ending and "process" starting go?</summary>

Into the partition: replication to the ISR before the high watermark moves
(acks=all), then waiting until the consumer group's member polls that
offset. When the consumer is caught up it's milliseconds. When it isn't, the
gap is exactly that record's consumer lag in time.
</details>

## franz-go ↔ Java client names

| franz-go | Java / docs | Note |
|---|---|---|
| `kgo.Record.Headers` (`[]kgo.RecordHeader`) | `ProducerRecord.headers()` / `Headers.add`, `lastHeader` | Java code usually reads the last duplicate |
| `kgo.WithHooks(...)` (`HookProduceRecordBuffered`, `HookFetchRecordBuffered`, ...) | `interceptor.classes` (`ProducerInterceptor`, `ConsumerInterceptor`) | |
| `kotel.NewKotel(kotel.WithTracer(...)).Hooks()` | OTel Java agent, or `KafkaTelemetry.wrap(producer/consumer)` from `opentelemetry-kafka-clients-2.6` | |
| `kgo.ClientID` | `client.id` | `messaging.client.id` |
| `kgo.ConsumerGroup` | `group.id` | `messaging.consumer.group.name` |

## Further reading

- W3C: [Trace Context](https://www.w3.org/TR/trace-context/) and [Baggage](https://www.w3.org/TR/baggage/)
- OpenTelemetry: [Messaging spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/) and [Kafka conventions](https://opentelemetry.io/docs/specs/semconv/messaging/kafka/)
- otel-go: [propagation package](https://pkg.go.dev/go.opentelemetry.io/otel/propagation), [OTLP gRPC exporter](https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc)
- franz-go: [plugin/kotel](https://pkg.go.dev/github.com/twmb/franz-go/plugin/kotel)
- Kafka: [KIP-82: Add Record Headers](https://cwiki.apache.org/confluence/display/KAFKA/KIP-82+-+Add+Record+Headers)
- [Jaeger documentation](https://www.jaegertracing.io/docs/)
