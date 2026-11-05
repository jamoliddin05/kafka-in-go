# 14 · Schema evolution

> **Goal:** change an event's shape without breaking anyone: know which
> changes are safe, in which order to deploy producers and consumers, how to
> read every version still sitting in the topic, and how to make the build
> fail before a breaking change reaches prod.

## Why this matters in prod

**Story 1: the first refund.** The orders service added a `refunded` status.
Purely additive, reviewed, shipped on a Tuesday. The billing consumer, two
releases behind, validated the status enum strictly, because a careful
developer once wrote "reject what you don't know". The first refunded order
was a decode error. Billing's retry loop retried it forever, the partition
stopped, and invoices for a third of the customers stopped with it (task 09
is about that loop). The schema change was fine; the deploy order was not.

**Story 2: revenue went to zero, quietly.** Six months after adding
`amount_minor` (cents, an integer) next to the old `amount` (dollars, a
float), the producer team removed `amount`: "everyone migrated". The monthly
finance report, deployed once a quarter, still read `amount`. JSON gives a
missing number the value 0, without an error. The report showed a 100%
revenue drop for that day, and that was the *lucky* part: a consumer that
sums into a database would have written zeros for a week before anyone
asked.

## Concepts

**Two versions always run at once.** A rolling deploy replaces replicas one
by one, so v1 and v2 producers and consumers share the topic for minutes;
canaries and rollbacks stretch that to days. And the topic itself keeps
every version ever written, for as long as retention (forever on a
compacted topic): a consumer that replays, or a new service that starts
from the beginning, reads them all.

```
t14.orders, partition 0
│ v1 │ v1 │ v1 │ v2 │ v1 │ v2 │ v2 │ v3 │ v3 │     ← mixed during every rollout,
  ▲ written months ago, still here               and old records stay until retention
```

**Compatibility, from the reader's point of view:**

| | means | lets you deploy first | example |
|---|---|---|---|
| backward | the new reader reads old data | consumers | v2 reader reads v1 records (upcasting) |
| forward | the old reader reads new data | producers | v1 reader reads v2 records (ignores unknown fields) |
| full | both | either | additive change with defaults, readers tolerant |

Confluent Schema Registry calls these BACKWARD (its default), FORWARD, FULL
and NONE, plus `_TRANSITIVE` variants that check against *all* previous
versions instead of only the last one. Replays need transitive.

**JSON rules of thumb.** Add optional fields freely; never rename, remove or
retype a field in place; readers ignore unknown fields (Go's `encoding/json`
does by default, `DisallowUnknownFields` turns that off and breaks forward
compatibility; Jackson's plain `ObjectMapper` fails on unknown properties by
default). A missing field is the zero value, so "absent" and "0" look the
same unless you decode into pointers.

**Enums are the trap.** Adding a value is "additive" for the writer and a
breaking change for every reader that validates the set or maps unknown
values to a default. A reader must keep an unknown value as unknown (count
it, alert, skip or park it) and never guess.

**Expand, then contract.** A breaking change becomes a sequence of
compatible ones:

```
v1  writes amount                    readers need amount
v2  writes amount + amount_minor     deploy readers that prefer amount_minor and upcast amount
    (expand)                         ... until no reader needs amount, and no rollback target does
v3  writes amount_minor only         (contract)
```

Deploy order: when adding (a field, an enum value) deploy **consumers
first**, then let producers use it. When removing, make every consumer stop
reading the field first, then stop writing it. When you can't make it
compatible (a semantic change, a type change), create a **new topic**
(`orders.v2`), dual-publish or run a translator while consumers migrate one
by one, then retire the old topic.

**Envelope.** The payload alone can't say what it is. Record headers carry
`event-type` (several event types on one topic), `content-type` (JSON today,
Protobuf tomorrow) and `schema-version` (which writer produced it, so you can
meter "who still sends v1" and refuse what you can't read). A record without
headers is a pre-envelope record: treat it as the oldest version.

**Upcast at the edge.** Convert every wire version into one in-memory model
in the decoder, right after `PollFetches`. The business logic only sees the
current model; the decoder is the one place that knows history.

**Schema Registry** (Confluent, Apicurio, Karapace) is a separate service;
the Kafka broker never looks inside your bytes. A registry-aware serializer
registers the schema, gets an ID and prefixes each payload with the
Confluent wire format: magic byte `0`, a 4-byte big-endian schema ID, then
the Avro/Protobuf/JSON-Schema bytes. The registry rejects a new schema that
breaks the subject's compatibility mode, which moves this task's contract
test to registration time. In Protobuf: never change or reuse a field
number, mark removed fields and names `reserved`, keep the zero enum value
as `UNSPECIFIED`; proto3 enums are open, so unknown values arrive as numbers.
In Avro: a field you add needs a default for old data, and an enum symbol
you add needs the enum's `default`, or old readers fail.

## Coming from Pulsar

| Pulsar | Kafka | The difference that bites |
|---|---|---|
| Built-in schema registry per topic, stored by the cluster | No registry in Apache Kafka; an external service plus a client serializer, or plain bytes plus discipline | Nothing stops a producer from writing anything to a topic |
| Compatibility strategy checked when a producer/consumer connects; configurable per namespace (with a broker default) | Checked by the registry at schema registration, if you use one; otherwise only by your tests | Without a registry, a breaking change fails at runtime, in the consumer |
| `Schema.AUTO_CONSUME()`: decode each message with the schema version it was written with | You read bytes; the version comes from your envelope or the registry ID prefix | You write the version dispatch and the upcasting yourself |
| Message properties | Record headers | Same idea; in Kafka they're your only envelope |
| Schema versions visible per topic (`pulsar-admin schemas`) | Visible only if you put them in headers or registry IDs | Without it you can't even ask "who still writes v1?" |

## Run it

One binary plays all three releases; `VERSION` picks which one, and there is
no default. The dev producer takes it from `task.env` (`VERSION=v1`), the
workers from `make ... V=`.

```bash
make up                                                    # once
make dev T=14-schema-evolution R=producer                  # terminal 1: release from task.env, an order every 500ms
make workers T=14-schema-evolution V=v1                    # 3 billing consumers, prod image, release v1
make logs R="worker-a worker-b worker-c"                   # terminal 2
make rollout T=14-schema-evolution V=v2 PAUSE=15           # roll the consumers to v2, one by one
VERSION=v2 go run ./tasks/14-schema-evolution -role replay # every record in the topic through the v2 reader
go test ./tasks/14-schema-evolution/...                    # the contract tests
```

Without Docker, use the host: `VERSION=v1 INSTANCE=billing-a go run
./tasks/14-schema-evolution -role consumer` in two terminals and
`VERSION=v2 go run ./tasks/14-schema-evolution -role producer` in a third.
All consumers share the group `t14.billing`, so with mixed versions each
partition is billed by whichever version owns it.

The consumer logs one line per order (`billed`, or a warning/error) with the
`schema` header it saw and `newer_than_me=true` when the writer is newer
than the reader, plus a `summary` every 10s: records per schema version,
decode errors, unknown statuses, zero amounts and revenue per currency.
Knobs are in [`task.env`](task.env).

In the starter, release v1 works; v2 and v3 return `not implemented yet` and
`go test` fails **on purpose**, including two tests that pass only when you
decide v1 is gone (step 4). The reference is in
[`solution/`](solution/schema.go): `make workers T=14-schema-evolution/solution`.

## Your task

The code has one `release` per version: what it writes and which reader it
runs. v1 is frozen: its bytes are in the topic and its reader runs in prod
during every rollout. `testdata/v1.json`, `v2.json`, `v3.json` are the
agreed payload of each version (the producer and its consumers signed off
on them), and `schema_test.go` checks both sides against them.

1. **TODO(14.1) Write v2 (expand).** `encodeV2`: add `amount_minor` (cents,
   int64) and `currency`, and keep writing `amount`.
   *Done when:* `go test ./tasks/14-schema-evolution -run
   TestWritersMatchGolden/v2` passes.
2. **TODO(14.2) Read everything (tolerant reader + upcast).**
   `decodeCurrent`: check `content-type` and `event-type` if present,
   decode into a struct that has every field any version wrote (pointers, so
   absent ≠ 0), prefer `amount_minor`, upcast `amount` from v1 records,
   default `currency` to USD, keep unknown statuses as they are, and return
   an error when there is no amount at all.
   *Done when:* `TestCurrentReaderEdges` and every `TestContract/v2_reads_*`
   pass, and `VERSION=v2 ... -role replay` shows 0 decode errors for every
   schema version in the topic.
3. **TODO(14.3) Write v3 (contract).** `encodeV3`: no `amount`.
   *Done when:* `TestWritersMatchGolden/v3` passes. `TestContract/v1_reads_v3`
   now fails with `got {ID:o-3 AmountMinor:0 ...}, want {... AmountMinor:4200 ...}`,
   and `v1_reads_v2` with `unknown status "refunded"`. That is the gate
   doing its job: v1 readers are still listed as deployed.
4. **TODO(14.4) Retire v1.** Run experiments A and B first. Only when no v1
   reader is running (and none is a rollback target) set `deployedReaders`
   to what prod runs now.
   *Done when:* `go test ./tasks/14-schema-evolution/...` passes, and you can
   say which experiment would have caught each failure without the test.

## Experiments

**A. Producer first (the wrong order).** Workers on v1, producer on v1, then
set `VERSION=v2` in `task.env` and re-run `make dev T=14-schema-evolution
R=producer`. Within seconds the v1 consumers log

```
level=ERROR msg="cannot decode, skipping (in prod: DLQ, task 09)" ... schema=2 newer_than_me=true err="unknown status \"refunded\"" value="{\"id\":\"iofz-00000\",\"amount\":177.84,\"amount_minor\":17784,\"currency\":\"EUR\",\"status\":\"refunded\"}"
```

for every refund. Now look at the `billed` lines of EUR orders: `amount="10.58 USD"`
for an order whose payload says `"currency":"EUR"`. The v1 reader ignored the
unknown field, as it should, and billed euros as dollars. Forward compatible
on paper, wrong in money. Which of the two problems would a schema registry
in FORWARD mode have caught?

**B. Consumers first, mixed versions.** `make rollout T=14-schema-evolution
V=v2 PAUSE=15` while the v2 producer runs. During the rollout some
partitions are billed by v1 (errors, euros as dollars) and some by v2
(`amount="19.79 EUR"`, refunds subtracted); the `inst` field tells you which.
After it, no errors. Then switch the producer to `VERSION=v3` and roll one
worker back to v1 (`make rollout ... V=v1` and Ctrl+C after worker-a, or a
v1 consumer from the host). It bills v3 orders with amount 0 and no error;
only the business sanity check notices:

```
level=WARN msg="order with zero amount" ... schema=3 newer_than_me=true order=3xj4-00014 status=paid value="{\"id\":\"3xj4-00014\",\"amount_minor\":1858,\"currency\":\"USD\",\"status\":\"paid\"}"
```

A rollback is a deploy too: the contract has to hold for every version you
might roll back to.

**C. Strict readers.** `STRICT=true VERSION=v1 go run
./tasks/14-schema-evolution -role replay`, then without `STRICT`. Measured
on a topic with 33 v1, 80 v2 and 38 v3 records:

```
replay of t14.orders through the v1 reader (strict=true): 151 records
SCHEMA  RECORDS  DECODE ERRORS  UNKNOWN STATUS  ZERO AMOUNT  FIRST ERROR
1       33       0              0               0
2       80       80             0               0            unknown status "refunded"
3       38       38             0               0            json: unknown field "amount_minor"
```

Non-strict v1: 18 errors on schema 2 (only refunds) and 32 zero amounts on
schema 3. The v2 and v3 readers: zeros everywhere. Strictness turned an
additive change into a total outage for old readers.

**D. Replay before you deploy.** Run `-role replay` with the version you are
about to ship, against the real topic. It's the cheapest pre-deploy check
there is: it reads exactly the versions retention still holds, including
ones your golden files forgot. Add a pre-envelope record by hand and replay
again: `docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-producer.sh
--bootstrap-server kafka-1:29092 --topic t14.orders` and type
`{"id":"legacy-1","amount":5.0,"status":"paid"}` (schema 0 in the table).

**E. Your own breaking change.** Make v4 rename `currency` to `ccy` in a new
golden file and writer, add `"v4"` to `releases`, and watch which tests
fail. Then do it right: expand (write both), migrate readers, contract.

## Edge cases and gotchas

- **"Additive" can still be semantic breakage.** Adding `currency` broke
  nothing syntactically and still made v1 bill euros as dollars. A field that
  changes the meaning of existing fields is a breaking change. Ship it as
  "consumers first", or as a new event type.
- **Zero vs absent.** Decoding into plain values hides a removed field as 0,
  `""`, `false`. Decode into pointers (or `json.RawMessage`) where absence
  matters, and fail loudly when a required field is missing.
- **Rollbacks.** v3 ships, v3 has a bug, you roll back to v2: v2 must read
  what v3 already wrote. Keep the last version's reader compatible with the
  next version's writer, and keep rollback targets in `deployedReaders`.
- **Compacted topics never forget.** Every key's latest value can be years
  old (task 15). Upcasters there live forever, or you rewrite the topic.
- **Don't mutate a shipped golden file.** `go test -update` exists for the
  version you're developing. If a test asks you to update `v1.json`, the
  writer broke compatibility.
- **Float money.** `amount` as float64 dollars is why v2 exists:
  `0.1+0.2` doesn't round-trip. Integer minor units plus a currency, always.
- **Envelope in the payload vs headers.** Headers keep the payload clean and
  are readable without decoding (routing, metrics, DLQ triage), but some
  tools (older Kafka Connect transforms, `kafka-console-consumer` without
  `print.headers`) hide them. Pick one convention per topic.
- **One topic, one event type, or not?** Several event types on one topic
  keep their relative order per key (task 02), but every consumer must
  handle (or skip) every type. The `event-type` header makes skipping cheap.
- **Registry gotchas.** `auto.register.schemas=true` (Confluent's default)
  lets any producer register a new version in prod; most teams turn it off
  outside CI. A registry checks syntax compatibility, not meaning (see the
  first bullet).

## Check yourself

<details><summary>1. v2 adds an optional field and a new enum value. In which order do you deploy, and why?</summary>

Consumers first. The new field is harmless to tolerant old readers, but the
new enum value is not: any reader that validates or switches on the enum
must know it (or keep it as "unknown") before the first producer emits it.
That's BACKWARD compatibility: new readers, old data.
</details>

<details><summary>2. Why did the v1 reader return 0 for v3 orders instead of an error, and how does the current reader avoid that?</summary>

`encoding/json` leaves a missing field at its zero value, and v1 decodes
`amount` into a plain `float64`. The current reader decodes into pointers,
so "no `amount_minor` and no `amount`" is detectable and returned as an
error.
</details>

<details><summary>3. Your topic has 30 days of retention. How long must the v1 upcaster stay in the code after the last v1 producer is gone?</summary>

At least 30 days after the last v1 record was written, and longer if anyone
can replay from an older backup, mirror or compacted topic. In practice:
until a replay of the whole topic shows no schema 1 records, and the topic
is not compacted.
</details>

<details><summary>4. Where does a Schema Registry's compatibility check run, and what can it not catch?</summary>

At schema registration (by the serializer or in CI), not on the broker and
not per message. It checks structure against the subject's mode; it can't
see semantics (euros billed as dollars), strict hand-written validators in
consumers, or a producer that bypasses the serializer.
</details>

<details><summary>5. When is a new topic (orders.v2) the right answer instead of a new schema version?</summary>

When no sequence of compatible steps exists: a field changes type or
meaning, a key changes (it changes partitioning and ordering), or the event
is really a different event. Produce both (dual-publish or a translator),
move consumers one at a time, then stop the old topic.
</details>

## franz-go ↔ Java client names

| franz-go | Java / docs | Note |
|---|---|---|
| `kgo.Record.Headers`, `kgo.RecordHeader` | `ProducerRecord.headers()`, `Headers.add` | your envelope |
| your decoder + `encoding/json` | `Deserializer<T>` / Jackson `ObjectMapper` | Jackson fails on unknown properties by default (`FAIL_ON_UNKNOWN_PROPERTIES`) |
| `json.Decoder.DisallowUnknownFields` | `FAIL_ON_UNKNOWN_PROPERTIES=true` | breaks forward compatibility |
| `github.com/twmb/franz-go/pkg/sr` (`sr.Client`, `sr.Serde`) | `CachedSchemaRegistryClient`, `KafkaAvroSerializer`/`KafkaProtobufSerializer` | separate module, not used in this course |
| `sr` compatibility levels | `BACKWARD`, `FORWARD`, `FULL`, `NONE`, `*_TRANSITIVE` | registry setting per subject |
| (serializer config) | `auto.register.schemas`, `use.latest.version` | Confluent serializer configs |

## Further reading

- Confluent: [Schema evolution and compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html) and the [wire format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format)
- Martin Fowler: [Tolerant Reader](https://martinfowler.com/bliki/TolerantReader.html)
- Martin Kleppmann: [Schema evolution in Avro, Protocol Buffers and Thrift](https://martin.kleppmann.com/2012/12/05/schema-evolution-in-avro-protocol-buffers-thrift.html)
- Protocol Buffers: [Updating a message type](https://protobuf.dev/programming-guides/proto3/#updating)
- Apache Pulsar: [Schema evolution and compatibility](https://pulsar.apache.org/docs/next/schema-understand/)
- franz-go: [package sr](https://pkg.go.dev/github.com/twmb/franz-go/pkg/sr), [package kgo](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo)
