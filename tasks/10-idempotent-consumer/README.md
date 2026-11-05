# 10 · Idempotent consumer

> **Goal:** stop hoping for "exactly once delivery". Kafka gives you
> at-least-once, so every handler sees duplicates. Make a duplicate harmless
> by recording the event ID in an inbox table in the *same* Postgres
> transaction as the side effect, then go one step further and keep the
> group's offsets in Postgres too.

## Why this matters in prod

**Story 1: the double top-up.** A wallet service consumed `deposits` and ran
`UPDATE wallets SET balance = balance + $1`. Offsets were committed every
5 seconds. During a node drain the pods overran their grace period and were
SIGKILLed: everything applied since the last commit was delivered again to
the new pods. Around 1,900 wallets were credited twice. Kafka did exactly
what at-least-once promises; the handler was the bug.

**Story 2: the replay that paid twice.** The same team had learned and
added an inbox table, cleaned by a nightly job with a 24-hour retention. A
month later someone fixed a projection bug and reset the group to "3 days
ago" (task 01's time machine). The inbox only remembered one day, so two
days of deposits were applied again. **The dedup window has to cover the
replay window.**

## Concepts

**Where duplicates come from.** Most of them are invisible to Kafka:

| Source | Same event_id | Same offset | Who can catch it |
|---|---|---|---|
| Producer retry after a lost ack | yes | (never written twice) | idempotent producer, by default (task 04) |
| App-level resend: an upstream timed out and published again, an outbox relay crashed before marking a row (task 11) | yes | **no**, new offset | only the consumer, by event_id |
| Consumer crash between the side effect and the offset commit | yes | yes | only the consumer |
| Rebalance before the old owner committed (tasks 05, 07) | yes | yes | only the consumer |
| DLQ redrive (task 09), mirroring to another cluster | yes | **no** | only the consumer, by event_id |
| Replay: group reset by timestamp (tasks 01, 06) | yes | yes | only the consumer |

**The window you can't close.** The side effect lives in Postgres, the
position lives in Kafka. Two systems, two commits, no shared transaction:

```
poll ─► apply in Postgres ─► COMMIT ─► ... more records ... ─► commit offsets to Kafka
                               ▲                                        ▲
                               └───────── crash anywhere here ──────────┘
            Postgres has the effect, Kafka still has the old offset:
            after the restart the same records are delivered again
```

Committing more often shrinks the window; it never closes it. So the
handler must make "apply twice" equal "apply once". That is what
*effectively once* means: at-least-once delivery plus an idempotent handler.

**Ways to be idempotent:**

1. **Natural idempotency.** `SET status = 'paid'` or an upsert of the full
   state is safe to repeat. `balance = balance + x` is not. Prefer the first
   kind when the domain allows it.
2. **Inbox (dedup) table.** `t10_inbox(event_id PRIMARY KEY)`. In one
   transaction: insert the event ID, and apply the effect only if the insert
   happened.

   ```
   BEGIN
     INSERT INTO t10_inbox (event_id) VALUES ('7KQ...') ON CONFLICT DO NOTHING  -- 1 row: new, 0: seen
     INSERT ... ON CONFLICT DO UPDATE SET balance = balance + 4200             -- only if new
   COMMIT                                                                    -- both or neither
   ```

   The primary key, not a `SELECT` before the insert, does the dedup: two
   members processing the same record at once (a zombie during a
   rebalance) serialize on the unique index, and the second one inserts
   nothing.
3. **Version checks.** For state that carries a per-entity version:
   `UPDATE ... SET ..., version = $v WHERE id = $id AND version < $v`. That
   rejects both duplicates and stale out-of-order events, with constant
   storage per entity instead of one row per event.
4. **Idempotency keys for external APIs.** An HTTP call can't join your
   transaction. Pass the event ID as the provider's idempotency key
   (Stripe's `Idempotency-Key` header, for example) and let them dedupe.
5. **Offsets in your database.** Store `(group, topic, partition) →
   next_offset` in the same transaction as the effect, and on every
   assignment start from those offsets instead of Kafka's. A crash can no
   longer redeliver anything the database has. Kafka's committed offset
   becomes a monitoring copy. This dedupes *records*, not *events*: an
   event published twice sits at two offsets and is applied twice.

**Identity is the business event ID**, minted once where the fact happens
and carried by every copy. Never (topic, partition, offset): a redrive, a
mirror or a recreated topic gives the same event a new one.

**The inbox is a window, not a history.** It grows by one row per event and
must be trimmed, but anything redelivered after its row is gone gets applied
again. Keep it at least as long as the furthest you would ever replay
(often: the topic's retention).

## Coming from Pulsar

| Pulsar | Kafka | The difference that bites |
|---|---|---|
| Broker dedup (`brokerDeduplicationEnabled` or per namespace), keyed by producer name + sequence ID | Idempotent producer: producer ID + epoch + per-partition sequence, on by default | Both drop retries of the *same send*. A second publish of the same business event goes through in both. Pulsar can dedupe it if you set `sequenceId` yourself from a stable source; Kafka has no equivalent |
| Redelivery of unacked messages: `ackTimeout`, `negativeAcknowledge`, consumer disconnect | Redelivery of everything after the partition's committed offset | Kafka redelivers a *range*, including records you finished. One crash means a batch of duplicates |
| Individual acks, the broker tracks ack holes | One committed offset per partition | You can't ack "all except #17". Dedup has to live in the handler |
| `MessageId` | (partition, offset) | Neither is a business identity: redrive and geo-replication give the same event a new one |
| Transactions with acks | Transactions + offsets in the transaction (task 12) | Both stop at the broker. Your Postgres write is outside both |

Redeliveries exist in both systems; Pulsar just produces them one message at
a time. The fix is the same: an idempotent handler.

## Run it

```bash
make up                                                       # once: brokers, UI, Jaeger, Postgres
make dev T=10-idempotent-consumer R=consumer                  # terminal 1: the consumer, hot reload
go run ./tasks/10-idempotent-consumer -role producer          # terminal 2: 2000 deposits (+~5% re-sent), then exits
go run ./tasks/10-idempotent-consumer -role check             # expected vs actual balances
go run ./tasks/10-idempotent-consumer -role reset             # truncate t10_* and empty the topic between runs
make psql                                                     # SELECT * FROM t10_accounts;
```

`check` rebuilds the expected balances from the log itself: it reads
`t10.deposits` from the start and sums each `event_id` once. Run it after
the producer finished and the consumer went idle; it prints the group's lag
so you can tell. `reset` truncates the tables and moves the topic's log
start to its end with `DeleteRecords`, so stop the producer first (the
consumer can stay up).

Knobs live in [`task.env`](task.env). Compose reads it for the consumer;
from the host pass them inline (`DUP_PCT=0 go run ... -role producer`), or
run the consumer on the host too:
`INBOX=true go run ./tasks/10-idempotent-consumer -role consumer`.

**The starter is deliberately naive and it runs.** Its consumer adds every
record to the balance and commits offsets every 100 records. Every 250
records it simulates a crash right after a DB commit and restarts
in-process. `check` shows inflated balances: that is the bug you fix.
`INBOX=true` and `OFFSETS_IN_DB=true` exit with `not implemented yet` on the
first record until you do. The reference is in [`solution/`](solution/):
`make dev T=10-idempotent-consumer/solution R=consumer`.

## Your task

1. **TODO(10.1) Inbox in the same transaction.** Implement `claimEvent`
   (insert into `t10_inbox`, report whether a row was inserted) and the
   `INBOX=true` branch of `handle`: one `pgx.BeginFunc` transaction that
   claims the event and applies the deposit only if it was new.
   *Done when:* with `INBOX=true`, `DUP_PCT=5` and `CRASH_AFTER_DB=250`,
   `check` prints `OK`, and the number of `duplicate skipped` lines equals the
   re-sent copies plus 50 per crash.
2. **TODO(10.2) Offsets in Postgres.** In the same transaction, `saveOffset`
   upserts the record's `offset + 1` into `t10_offsets`. `loadOffsets` is
   already registered with `kgo.AdjustFetchOffsetsFn`: make it replace
   Kafka's committed offset with the stored one for every assigned
   partition that has a row.
   *Done when:* with `INBOX=false OFFSETS_IN_DB=true`, a producer run with
   `DUP_PCT=0` ends in `OK` although the consumer crashed, every restart logs
   `resuming from the offset stored in Postgres` with `postgres` greater than
   `kafka_committed`, and the consumer's final `handled` equals the number of
   records (nothing redelivered). With `DUP_PCT=5` the result is `WRONG`, and
   the actual total equals the "sum of every record on the log" line: offsets
   know nothing about event identity.
3. **TODO(10.3) Inbox retention.** `-role cleanup` deletes inbox rows older
   than `INBOX_RETENTION`.
   *Done when:* `INBOX_RETENTION=0s go run ./tasks/10-idempotent-consumer -role cleanup`
   logs `inbox cleaned deleted=<every row>`.

## Experiments

Each run: `-role reset`, restart the consumer with the knobs you want, run
the producer, wait for the consumer to go idle, `-role check`.

**A. Two kinds of duplicates, measured.** With the naive starter:

| Consumer | Producer | Result |
|---|---|---|
| `CRASH_AFTER_DB=0` | `DUP_PCT=0` | `OK`. The naive handler passes every test you would write on a laptop |
| `CRASH_AFTER_DB=0` | `DUP_PCT=5` | `WRONG`; the actual total equals "sum of every record on the log" |
| `CRASH_AFTER_DB=250` | `DUP_PCT=0` | `WRONG`; the consumer's `handled` is the record count + 50 per crash |

Why exactly 50 per crash? (Commits happen at 100 and 200; the crash comes at
250.)

**B. Shrink the window, never close it.** `COMMIT_EVERY=1
CRASH_AFTER_DB=250`, `DUP_PCT=0`. Every crash now logs
`records_to_be_redelivered=1`, and the balances are still wrong, just by less.
You paid one commit round trip per record to get there. Which crash point
would still duplicate even with a commit after every record?

**C. Replay the whole topic.** After a run, stop the consumer (Ctrl+C in
terminal 1), rewind its group and start it again:

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:29092 \
  --group t10.ledger --topic t10.deposits --reset-offsets --to-earliest --execute
```

The tool refuses while the group has members (`Assignments can only be
reset if the group 't10.ledger' is inactive`). Naive: every balance
doubles. `INBOX=true`: every record is `duplicate skipped`, `check` stays
`OK`. `OFFSETS_IN_DB=true`:
nothing happens at all, the log says `resuming from the offset stored in
Postgres ... kafka_committed=<the start>`. Your replay runbook now has to
`UPDATE t10_offsets` instead. Who on your team would know that at 3 a.m.?

**D. The window is shorter than the replay (Story 2).** With `INBOX=true`,
do a run, then `INBOX_RETENTION=0s go run ./tasks/10-idempotent-consumer -role cleanup`,
then replay as in C. `check` shows every balance doubled. Only the re-sent
copies inside the replay itself were still deduped. What retention would
you pick if the topic keeps 7 days?

**E. Kafka's lag is now a copy.** Run with `OFFSETS_IN_DB=true DUP_PCT=0`
(2000 records, a crash every 250). The last crash lands right after the last
record, so `check` prints `50 by Kafka commits, 0 by Postgres offsets`. Kafka
UI and `make group G=t10.ledger` show the same 50. What do your lag alerts
read in this setup?

**F. Rebalance duplicates.** Set `INBOX=true` in `task.env`, run
`make scale T=10-idempotent-consumer R=consumer N=3` and the producer.
Every crash of one member moves its partition to another member, which
starts from the last commit: `duplicate skipped` lines show up in the member
that took it over. Try the same with `INBOX=false`.

**G. Look at the tables.** `make psql`, then
`SELECT count(*) FROM t10_inbox;` and `SELECT * FROM t10_offsets;`. Compare
`t10_offsets` with `make group G=t10.ledger`.

## Edge cases and gotchas

- **Same transaction or nothing.** Inbox insert first in its own
  transaction, then the update: a crash in between marks the event done and
  never applies it, a *silent loss*. The other order duplicates. Only one
  transaction is correct.
- **Side effects outside the database** (email, a payment API) can't share
  the transaction. Use the provider's idempotency key, or write the intent
  to an outbox in the same transaction (task 11) and let the sender dedupe.
- **The event ID must be minted once, upstream.** A new UUID on every resend
  defeats the inbox. A content hash is not an ID either: two genuine $10
  deposits in a row have the same content.
- **This consumer never commits on revoke.** A partition that moves in a
  rebalance is redelivered from the last commit. Committing in
  `OnPartitionsRevoked` (tasks 05, 07) shrinks that, but a SIGKILL runs no
  callback.
- **Inbox growth.** One row per event: at 1,000 events/s that's 86 million
  rows a day. Partition the table by day and drop old partitions instead of
  running a huge `DELETE` that bloats the table and works the vacuum hard.
- **Offsets in the database** need the effect and the offsets in the *same*
  database. If you shard, each shard stores the offsets of what it applied.
  New partitions have no row yet, so they fall back to Kafka's committed
  offset and `auto.offset.reset`. Keep committing to Kafka anyway, or every
  lag dashboard goes blank.
- **Dedup is not ordering.** An inbox accepts an old event that arrives after
  a newer one. If order matters, use version checks.
- **Hot rows.** `balance = balance + x` on one account serializes its
  writers on a row lock. Fine here because one partition (one consumer) owns
  each account.

## Check yourself

<details><summary>1. The producer is idempotent (the default). Why do you still need an inbox?</summary>

Producer idempotence dedupes retries of the same batch within one producer
session (producer ID, epoch, sequence). An application that publishes the
same event again, a new producer instance, or a relay that republishes after
a crash produces a brand-new record that the broker happily appends.
</details>

<details><summary>2. Why is (topic, partition, offset) a bad dedup key?</summary>

It identifies a position in one log, not an event. A DLQ redrive, an
upstream resend, mirroring to another cluster or recreating the topic gives
the same event a different offset. It only catches redeliveries of the same
record.
</details>

<details><summary>3. You commit offsets after every single record. Can you still get duplicates?</summary>

Yes. A crash after the DB commit and before the offset commit redelivers that
record (experiment B: one per crash). Rebalances and replays add more. You
only made the window smaller and the consumer slower.
</details>

<details><summary>4. The inbox insert and the balance update are two autocommitted statements, inbox first. What breaks?</summary>

A crash between them leaves the event marked as processed but never applied.
Every redelivery is then skipped as a duplicate: the deposit is lost and no
error is ever logged.
</details>

<details><summary>5. Offsets live in Postgres. Someone resets the group to yesterday with kafka-consumer-groups. What happens?</summary>

Nothing. On the next assignment `AdjustFetchOffsetsFn` replaces Kafka's
offsets with the ones in `t10_offsets`. To replay, change the rows in
Postgres, ideally with a tool that also checks the group is stopped.
</details>

## franz-go ↔ Java client names

| franz-go | Java / docs | Note |
|---|---|---|
| `kgo.DisableAutoCommit()` | `enable.auto.commit=false` | franz-go autocommits every 5s by default |
| `kgo.AutoCommitInterval` | `auto.commit.interval.ms` (5000) | |
| `cl.CommitRecords(ctx, rs...)` | `commitSync(Map<TopicPartition, OffsetAndMetadata>)` | commits `offset+1` of the highest record per partition |
| `kgo.AdjustFetchOffsetsFn` | `ConsumerRebalanceListener.onPartitionsAssigned` + `seek()` | the hook for offsets stored outside Kafka |
| `kgo.NewOffset().At(n)` | `seek(tp, n)` | |
| `kgo.OnPartitionsRevoked` | `onPartitionsRevoked` | where you commit before losing a partition |
| idempotence on by default, `kgo.DisableIdempotentWrite()` to turn it off | `enable.idempotence=true` (default since 3.0) | |
| `kadm.Client.DeleteRecords` | `Admin.deleteRecords()`, `kafka-delete-records.sh` | what `-role reset` uses |

## Further reading

- Kafka docs: [Message delivery semantics](https://kafka.apache.org/documentation/#semantics)
- [KIP-98: Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging) (where the idempotent producer comes from, and what it covers)
- Chris Richardson: [Pattern: Idempotent Consumer](https://microservices.io/patterns/communication-style/idempotent-consumer.html)
- Stripe: [Designing robust and predictable APIs with idempotency](https://stripe.com/blog/idempotency)
- PostgreSQL: [INSERT ... ON CONFLICT](https://www.postgresql.org/docs/current/sql-insert.html#SQL-ON-CONFLICT)
- franz-go: [AdjustFetchOffsetsFn](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#AdjustFetchOffsetsFn)
