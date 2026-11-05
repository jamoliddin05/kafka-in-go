# Kafka in Go: from Pulsar user to the person who knows Kafka cold

Sixteen hands-on tasks against a real 3-broker cluster. Each one is built
around a production failure you'll actually meet: lost writes, duplicate
charges, rebalance storms during deploys, poison pills, silent data skips,
and traces that break at the topic boundary. You fix them in Go with
[franz-go](https://github.com/twmb/franz-go).

Every task has a **"Coming from Pulsar"** section, because most of the
surprises come from things Pulsar does for you that Kafka does not
(individual acks, built-in DLQ, backlog retention), and the reverse (the log
is the database).

## What's in the box

```
docker compose
├── kafka-1, kafka-2, kafka-3   Kafka 4.3 in KRaft mode (no ZooKeeper), RF=3, min.insync.replicas=2
├── kafka-ui                    http://localhost:8085  topics, messages, groups, lag
├── jaeger                      http://localhost:16686 traces (tasks 08, 09, 11)
├── postgres                    localhost:5433 course/course (tasks 10, 11)
├── api/producer/processor/     your Go code in a dev container with air hot reload
│   relay/consumer              (profile "app", started per task)
└── worker-a/b/c                the same code as a prod image: distroless, no air
                                (profile "prod", used for deploy experiments)
```

`Dockerfile` has three targets. `dev` is the Go toolchain plus air and is what
compose runs. `build` compiles one task. `prod` is a static binary on
distroless with no shell and no air; the binary is PID 1, so it receives
SIGTERM directly, like in Kubernetes. A plain `docker build .` builds `prod`,
and BuildKit never even builds the `dev` stage for it.

## Quick start

Requirements: Docker Desktop, Go 1.26+ on the host (for one-shot tools and
the debugger), make.

```bash
cd kafka
make up                                        # cluster + UI + Jaeger + Postgres, waits for healthy
make dev T=01-the-log R="producer consumer"    # run task 01, hot reload on save
```

The first `make dev` downloads the Go modules into the `gomod` volume, which
can take a few minutes. After that, starts take seconds.

Edit any `.go` file under `tasks/01-the-log/` or `internal/`. air rebuilds
and restarts the process with SIGINT first, so a reload is a graceful
shutdown (that matters from task 05 on). Edits to other tasks don't restart
anything.

```bash
make help        # every command
make down        # stop everything (topics are wiped)
make reset       # also drop the Go module/build cache volumes
```

## How a task works

```
tasks/NN-name/
├── README.md     theory → prod story → Pulsar mapping → run → your task → experiments → gotchas → check yourself
├── main.go       starter: compiles and runs, with TODO(NN.k) where the work is
├── task.env      knobs (ACKS=all, MODE=..., FAIL_RATE=...) injected into the containers
└── solution/     reference implementation; run it with T=NN-name/solution
```

- **Long-running processes** use the compose slots `api`, `producer`,
  `processor`, `relay`, `consumer`, so the README tells you e.g.
  `make dev T=05-consumer-groups R="producer consumer"`.
- **Scale** a role with `make scale R=consumer N=3`, follow it with
  `make logs R=consumer`.
- **One-shot tools** (inspect, lag, reset, redrive) run from the host:
  `go run ./tasks/NN-name -role inspect`. The defaults point at
  `localhost:9092-9094`, so the GoLand debugger works too: create a "Go Build"
  run config with program arguments `-role consumer`.
- Topics are `tNN.*` and groups are `tNN.*`, so tasks never collide and Kafka
  UI stays readable.

Do them in order. Each task assumes the previous ones.

## The curriculum

| # | Task | You'll be able to answer… |
|---|---|---|
| 01 | [The log](tasks/01-the-log) | Why does a consumer that was down for 8 days lose data with no error? How do you replay "the last 3 hours"? |
| 02 | [Keys and ordering](tasks/02-keys-and-ordering) | Why did "shipped" arrive before "paid" right after someone added partitions? |
| 03 | [Acks and durability](tasks/03-acks-and-durability) | What exactly does `acks=all` wait for? When can an acked write still be lost? |
| 04 | [Producer reliability](tasks/04-producer-reliability) | Where do records go when the pod is killed? What does idempotence actually dedupe? |
| 05 | [Consumer groups](tasks/05-consumer-groups) | Why does scaling to 10 consumers on 6 partitions do nothing? Eager vs cooperative vs KIP-848? |
| 06 | [Offsets and delivery semantics](tasks/06-offsets-and-delivery) | At-most-once vs at-least-once in code. What does auto-commit really commit? |
| 07 | [Graceful shutdown and deploys](tasks/07-graceful-shutdown) | What happens to partitions during a rolling deploy, an OOM-kill, and with static membership? |
| 08 | [Tracing and correlation](tasks/08-tracing) | How does one trace_id flow through HTTP → Kafka → consumer → Kafka → consumer? |
| 09 | [Retries and DLQ](tasks/09-retries-and-dlq) | One poison pill froze a partition for 6 hours. How do you build what Pulsar gives you for free? |
| 10 | [Idempotent consumer](tasks/10-idempotent-consumer) | Duplicates are guaranteed. How do you make them harmless with an inbox table? |
| 11 | [Transactional outbox](tasks/11-transactional-outbox) | How do you write to Postgres *and* Kafka without losing or inventing events? |
| 12 | [Exactly-once](tasks/12-exactly-once) | What do Kafka transactions guarantee, what don't they, and what is a zombie? |
| 13 | [Concurrency and lag](tasks/13-concurrency-and-lag) | How do you process 10x faster than partitions allow without breaking per-key order? |
| 14 | [Schema evolution](tasks/14-schema-evolution) | v1 and v2 run side by side during every deploy. What can you change safely? |
| 15 | [Compaction and state](tasks/15-compaction-and-state) | How do you use a topic as a table and bootstrap a service's state from the log? |
| 16 | [Share groups](tasks/16-share-groups) | Kafka 4's queues (KIP-932): the closest thing to Pulsar's Shared subscription. When should you use one? |

## Pulsar → Kafka cheat sheet

| Pulsar | Kafka | Where |
|---|---|---|
| Tenant / namespace policies | Topic configs, ACLs, quotas | 01 |
| Partitioned topic; key routing by `JavaStringHash` by default (`Murmur3_32Hash` opt-in) | Topic is always partitioned; key routing by murmur2, identical in franz-go and the Java client | 01, 02 |
| MessageId | (partition, offset) | 01 |
| Bookies (E / Qw / Qa) | Replication factor / ISR / `min.insync.replicas` + `acks` | 03 |
| `sendAsync`, `batchingMaxPublishDelay`, `maxPendingMessages` | `Produce` + promise, `linger`, `MaxBufferedRecords` | 04 |
| Broker dedup (`producerName` + sequenceId) | Idempotent producer (on by default) | 04 |
| Exclusive / Failover subscription | Consumer group (one owner per partition) | 05 |
| Subscription cursor, individual ack | Committed offset per partition, cumulative only | 06 |
| `subscriptionInitialPosition` | `auto.offset.reset` | 06 |
| Subscription expiry | `offsets.retention.minutes` (7 days after the group empties) | 06 |
| Consumer close → messages redispatched | Rebalance (and the deploy pain that comes with it) | 05, 07 |
| OTel interceptors, message properties | Record headers + your own propagation | 08 |
| `negativeAcknowledge`, `ackTimeout`, retry letter topic, `DeadLetterPolicy` | **Nothing built in.** You build retry topics and a DLQ | 09 |
| Pulsar transactions | Kafka transactions + `read_committed` | 12 |
| Shared / Key_Shared subscription | In-process worker pools (13) or share groups (16) | 13, 16 |
| Built-in schema registry + compatibility checks | External registry or contract tests | 14 |
| Compacted view, `readCompacted` | `cleanup.policy=compact` rewrites the log | 15 |
| Table view | Compacted topic + local state | 15 |
| Delayed delivery | None; pause or retry-topic tricks | 09 |

## The senior's checklist (what you'll have internalized by task 16)

**Topics.** RF=3, `min.insync.replicas=2`, auto-create off. Partition count
is chosen up front for target parallelism and is never "just increased" on a
keyed topic. Retention is longer than your worst realistic consumer outage.

**Producers.** `acks=all` and idempotence (both franz-go defaults, so don't
turn them off). Key by the entity whose order matters. Treat `Produce` as
async and handle every failed promise. `Flush` before `Close`. Know what you
do when the delivery timeout fires.

**Consumers.** Commit after processing, never before. The handler is
idempotent because duplicates *will* happen. Graceful shutdown: stop polling,
drain, commit, leave the group. Cooperative-sticky or KIP-848. Static
membership if restarts are frequent. Bounded retries, then a DLQ. Alert on lag
measured in *time*.

**Data.** Every event has an ID, a type and a version, plus trace context in
the headers. Schemas evolve additively. Consumers are deployed before
producers. Dual writes go through an outbox.

**Ops.** Watch under-replicated partitions, ISR shrinks, offline partitions,
consumer lag, DLQ depth and request latency. Know how to reset a group by
timestamp. Know what `kafka-dump-log.sh` is for.

## Troubleshooting

- **air doesn't pick up saves.** Docker Desktop forwards file events from
  macOS bind mounts. If yours doesn't (e.g. colima), set `poll = true` in
  `.air.toml`.
- **Port in use.** UI is on 8085, Postgres on 5433, brokers on 9092-9094, the
  task API on 8080, and Jaeger on 16686/4317/4318.
- **Weird state after many experiments.** `make down && make up` gives you a
  fresh cluster in about 20 seconds.
- **A broker you stopped is still down.** `make broker-start B=2`.
