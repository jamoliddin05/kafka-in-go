# 05 · Consumer groups

> **Goal:** know what a consumer group really is (a coordinator handing out
> partitions), why the partition count caps your parallelism, and what each
> rebalance protocol costs you during a scale-out or a deploy: eager,
> cooperative-sticky, and Kafka 4's broker-side protocol (KIP-848).

## Why this matters in prod

**Story 1: ten pods, same lag.** Black Friday, the billing consumer is 20
minutes behind. Someone scales the deployment from 6 to 10 pods. Lag doesn't
move. The topic has 6 partitions, and a partition is owned by exactly one
member of a group, so 4 pods sit idle with zero partitions and the other 6
work exactly as hard as before. The real fix (more partitions) was a
decision that had to be made months earlier, because adding partitions to a
keyed topic breaks per-key ordering (task 02).

**Story 2: the deploy that stalled every partition.** 12 replicas of a Java
service on the default assignor (`[RangeAssignor, CooperativeStickyAssignor]`
uses range, which is *eager*). Each pod restart triggers rebalances, and
in an eager rebalance every member drops every partition and waits at the
JoinGroup barrier until all members have rejoined. One replica was in the
middle of a 40-second batch, and a Java consumer only rejoins from `poll()`,
so the whole group processed nothing until that batch finished. A rolling
deploy of 12 pods meant a couple of dozen group-wide stalls. Cooperative
rebalancing, static membership (task 07) and KIP-848 exist because of this.

## Concepts

```
                       group t05.billing (coordinator: broker 2)
topic t05.orders       ┌─────────────────────────────────────────────┐
 p0 ─────────────────► │ member A (t05-consumer-c1)   owns p0 p1     │
 p1 ─────────────────► │                                             │
 p2 ─────────────────► │ member B (t05-consumer-c2)   owns p2 p3     │
 p3 ─────────────────► │                                             │
 p4 ─────────────────► │ member C (t05-consumer-c3)   owns p4 p5     │
 p5 ─────────────────► │ member D (t05-consumer-c4)   owns nothing   │ ← idle: a hot standby
                       └─────────────────────────────────────────────┘
```

- **One owner per partition per group.** The group, not the consumer, is the
  unit of "subscription". Parallelism = number of partitions. More members
  than partitions are idle standbys; they pick up partitions only when an
  owner leaves or dies.
- **Group coordinator.** One broker per group: the leader of the
  `__consumer_offsets` partition that `hash(group.id) % 50` maps to. It
  tracks members, the assignment and committed offsets (task 06).
- **What triggers a rebalance:** a member joins; a member leaves (`Close`
  sends LeaveGroup); a member dies and its **session timeout** expires (45s
  default, nothing moves until then); a subscribed topic gets more
  partitions or a regex subscription matches a new topic. In Java, also a
  member exceeding `max.poll.interval.ms`. franz-go heartbeats from a
  background goroutine, so a slow handler does not get you kicked, unless you
  block rebalances (`BlockRebalanceOnPoll`) longer than the rebalance timeout
  (60s).

### The classic protocol: a barrier, decided by the clients

```
 member ──JoinGroup──►  coordinator waits until EVERY known member has rejoined
        ◄─ leader gets everyone's subscriptions, computes the plan (client-side!)
 leader ──SyncGroup(plan)──► coordinator ──► every member gets its partitions
 members heartbeat every 3s; a heartbeat answered with REBALANCE_IN_PROGRESS
 is how a member learns that a rebalance started
```

- **Eager** (`range`, `roundrobin`, `sticky`): before rejoining, every member
  revokes *everything*. Nobody processes anything until the barrier
  completes. The stall is as long as the slowest member takes to rejoin.
- **Cooperative-sticky** (KIP-429, franz-go's default): members keep what
  they own and rejoin. Round 1 computes the target and *removes* partitions
  that must move; their owners revoke just those and rejoin; round 2 hands
  them to the new owner. Unmoved partitions never stop. Moved partitions pay
  for two barrier rounds.

```
eager        A: [p0 p1] ─revoke all─┐            ┌─► [p0]
             B: [p2 p3] ─revoke all─┼─ barrier ──┼─► [p2 p3]       everything stops
             C: (new)   ────────────┘            └─► [p1]
cooperative  A: [p0 p1] ── round 1: keep p0, give up p1 ── round 2 ─► [p0]
             B: [p2 p3] ── keeps processing the whole time ─────────► [p2 p3]
             C: (new)   ── round 1: nothing ───────────── round 2 ─► [p1]
```

### KIP-848: the broker assigns, members reconcile on their own

`group.protocol=consumer` (GA in Kafka 4.0, enabled in this cluster) removes
JoinGroup/SyncGroup. Each member sends `ConsumerGroupHeartbeat` (every 5s,
the interval comes from the broker: `group.consumer.heartbeat.interval.ms`).
The coordinator computes a **target assignment** with a server-side assignor
(`group.consumer.assignors=uniform,range`) and tells each member what to
give up and what to take. A member revokes, acks in its next heartbeat, and
the partition goes to its new owner on *that* member's next heartbeat. No
barrier: a slow member only delays the partitions it has to give up. Member
epochs fence zombies (a commit with an old epoch is rejected). The session
timeout is broker-side too (`group.consumer.session.timeout.ms`, 45s, and
`group.consumer.min.session.timeout.ms` is also 45s).

In franz-go you opt in with `kgo.ServerSideBalancer()`. It only takes effect
with a sticky balancer (`CooperativeStickyBalancer` or `StickyBalancer`,
which map to the server's `uniform`) or `RangeBalancer` (server `range`);
with `RoundRobinBalancer` the client quietly stays classic. franz-go
recommends it on Kafka 4.3+ (earlier brokers can return `STALE_MEMBER_EPOCH`
on a commit in a way the client cannot handle). The broker converts groups
online (`group.consumer.migration.policy=bidirectional`): when the first
`consumer`-protocol member joins a non-empty classic group, the group becomes
a `consumer` group and the classic members keep working through the classic
API. That is how you migrate with a normal rolling deploy.

### Rebalance callbacks

| Callback | When | What to do |
|---|---|---|
| `OnPartitionsAssigned` | after a rebalance, before fetching. Cooperative/KIP-848: only the *added* partitions | load per-partition state, log |
| `OnPartitionsRevoked` | you are still a member and give partitions up (rebalance, `Close`); cooperative also calls it with an empty map at the end of each session | commit what you processed. **Overriding it replaces franz-go's default, which was exactly that commit** |
| `OnPartitionsLost` | you are *no longer* a member: session expired (frozen pod, long GC, network partition), fenced, `UNKNOWN_MEMBER_ID` | drop state; don't commit, it will fail |

By default a rebalance can happen at any moment while you process a batch,
and a commit afterwards may cover partitions you no longer own.
`BlockRebalanceOnPoll` makes callbacks run only after you call
`AllowRebalance()`, so the revoke callback sees a fully processed batch. The
price: a rebalance now waits for your batch. That is what Java does by
default, and it is exactly what the experiments below measure.

## Coming from Pulsar

| Pulsar | Kafka | The difference that bites |
|---|---|---|
| Exclusive subscription | Group with one member | Same idea |
| Failover subscription: one active consumer per partition, others standby | Consumer group; members beyond the partition count are idle standbys | A standby takes over only after a rebalance: immediately on a clean `Close`, after the 45s session timeout on a crash |
| Shared / Key_Shared: many consumers per partition | Nothing in a classic group. Per-key worker pools (task 13) or share groups (task 16) | Parallelism is capped by partitions, so the partition count is a capacity decision |
| Broker hands partitions over one by one, no group-wide pause | Classic protocol has a JoinGroup barrier (eager: everybody stops; cooperative: two rounds). KIP-848 removes it | Deploys and autoscaling are where Kafka consumers hurt |
| Subscription lives on the broker by name | `group.id` = membership + committed offsets | Rename the group in a deploy and you get a brand-new subscription (task 06) |
| Consumer priority level decides who is active | No priorities; the assignor decides | Static membership (task 07) is the closest you get to "this pod owns these partitions" |

## Run it

```bash
make dev T=05-consumer-groups R="producer consumer"     # 1 consumer owns all 6 partitions
```

In a second terminal, watch the group from the host (a mini
`kafka-consumer-groups --describe` in a loop):

```bash
go run ./tasks/05-consumer-groups -role watch           # EVERY=0 prints once
GROUP=t05.billing EVERY=1s go run ./tasks/05-consumer-groups -role watch
```

Scale and follow the logs. franz-go's own group logs (joins, syncs, revokes)
show up because `task.env` sets `KAFKA_LOG_LEVEL=info`:

```bash
make scale T=05-consumer-groups R=consumer N=3
make logs R=consumer
make group G=t05.billing                                 # the Kafka CLI view
```

From the host instead of compose, give each process its own `INSTANCE` (it
becomes the client ID that `watch` shows):

```bash
INSTANCE=c1 go run ./tasks/05-consumer-groups -role consumer
INSTANCE=c2 BALANCER=range go run ./tasks/05-consumer-groups -role consumer
```

Knobs live in [`task.env`](task.env). Until you finish TODO(05.1) the
consumer exits with `not implemented yet`; `watch` prints the same error
until TODO(05.3). The reference is in [`solution/`](solution/main.go):
`make dev T=05-consumer-groups/solution R="producer consumer"`.

## Your task

1. **TODO(05.1) Balancer and protocol.** In `groupBalancing`, map `BALANCER`
   to `kgo.Balancers(...)` and `GROUP_PROTOCOL=consumer` to
   `kgo.ServerSideBalancer()`. Read `should848` in franz-go's
   `consumer_group_848.go` to find out which balancers it accepts, and warn
   about the one it silently ignores.
   *Done when:* the consumer starts with every `BALANCER` value, and `watch`
   shows `protocol=consumer assignor=uniform` for
   `GROUP_PROTOCOL=consumer BALANCER=cooperative-sticky`.
2. **TODO(05.2) Rebalance callbacks.** Fill `assigned`, `revoked` and `lost`:
   track owned partitions, log every change, and commit in `revoked`.
   *Done when:* scaling from 1 to 3 consumers logs `partitions revoked` on the
   old member and `partitions assigned` + `partition resumed ...
   first_record_waited=...` on the new ones.
3. **TODO(05.3) Describe the group.** Implement `describeGroup` for both
   protocols.
   *Done when:* `watch` lists members, client IDs and their partitions for a
   classic group, and also EPOCH and TARGET for a KIP-848 group.
4. **TODO(05.4) Lag.** Fill committed and end offsets in `printGroup`.
   *Done when:* COMMITTED, END and LAG are filled for every partition, and
   LAG stays small and roughly flat while the consumers keep up.

## Experiments

Experiments C–F use separate groups so you can run them side by side; start
each group at the end of the topic first so a backlog doesn't hide the
effect (the group must not exist yet or be empty):

```bash
G=t05.exp-coop
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:29092 \
  --group $G --topic t05.orders --reset-offsets --to-latest --execute
```

**A. Partitions cap parallelism.** `make scale ... N=3`, then `N=7`. `watch`
shows six members with one partition each and one `(idle)`. Now `N=2`:
compose stops five containers with SIGTERM, each leaves the group cleanly,
and the two survivors get three partitions each within seconds. What would
`N=7` have looked like if the topic had 12 partitions?

**B. Crash vs clean exit.** With 7 consumers, find the owner of p0 in
`watch` and kill it hard (`docker kill -s KILL <container>`, or `kill -9` a
host process). The dead member stays in the list and keeps "owning" p0;
LAG on p0 climbs for ~45s (on this laptop: from ~10 to 370 records at 50
rec/s across 6 partitions), then the idle member takes p0 and catches up in a
few seconds. Repeat with `docker stop` (SIGTERM): the handover is immediate.
Where does the new owner start reading p0, and what happens to the records
the dead member processed after its last autocommit?

**C. Eager vs cooperative, everyone healthy.** Run three consumers with
`BALANCER=range` in one group and three with `BALANCER=cooperative-sticky`
in another, then add a fourth to each and later stop it with Ctrl+C. Compare
`partitions revoked/assigned` and `first_record_waited`. On this laptop
(PROCESS_TIME=5ms, BATCH_TIME=0): eager revoked every partition on every
member, but the stop lasted ~3ms (`owned_nothing_for=3ms`) and first records
waited ~20ms; cooperative moved one partition, which waited ~0.5s when the
member joined and ~3s when it left, and the other five never paused. Why
can eager be this cheap here? (franz-go rejoins from a background
goroutine, and all members' heartbeat timers restart together after every
sync, so they all notice the rebalance at the same moment.)

**D. Eager vs cooperative vs KIP-848 with one slow member.** Same three
groups (add one with `GROUP_PROTOCOL=consumer BALANCER=sticky`), but start
the third member of each with `BATCH_TIME=8s` (one pod doing a slow bulk
write). Add and remove a fourth member. What we saw on this laptop:

- **eager (`range`):** both fast members owned nothing for ~4s on join and
  again on leave (`owned_nothing_for=3.9s`, `4.3s`). Every partition in the
  group waited for the slow member to finish its batch.
- **cooperative-sticky:** partitions that stayed on the fast members never
  paused (record wait stayed ~25ms). The partition that moved from a fast
  member to the newcomer waited ~17s on join and ~16s on leave: two barrier
  rounds, each waiting for the slow member's batch.
- **KIP-848 (`uniform`):** partitions that stayed never paused. The partition
  that moved (off a fast member) waited ~2–3s each way. The slow member was
  not involved at all.

Cooperative protects the partitions that stay put, but still has a barrier,
and with `BlockRebalanceOnPoll` the barrier waits for every member's
in-flight batch. KIP-848 has no barrier.

**E. KIP-848 reconciliation.** One member with `GROUP_PROTOCOL=consumer
BATCH_TIME=8s`, then add a fast one, and run `EVERY=1s ... -role watch`. The
group goes to `state=Reconciling`: the slow member's TARGET is already
`0,1,2` while it still OWNS `0..5`, and the new member owns nothing with
TARGET `3,4,5`. On this laptop that lasted ~16s, until the slow member
finished its batches and released the partitions. KIP-848 doesn't make a
slow owner fast; it just doesn't make everybody else wait.

**F. Rolling migrations.** (1) Run one consumer with `BALANCER=cooperative-sticky`
and start a second with `BALANCER=range` in the same group: the newcomer
loops on `INCONSISTENT_GROUP_PROTOCOL`, the old member is unaffected. This
is what a "harmless" assignor change looks like halfway through a deploy.
(2) Run a classic consumer, then add one with `GROUP_PROTOCOL=consumer`:
`watch` flips to `protocol=consumer assignor=uniform` and both members get 3
partitions. (3) `GROUP_PROTOCOL=consumer BALANCER=roundrobin`: still
`protocol=classic`.

**G. Lost, not revoked.** Freeze a member: `kill -STOP <pid>` of a host
consumer, wait more than 45s, `kill -CONT <pid>`. Its autocommit fails with
`UNKNOWN_MEMBER_ID`, the app logs `partitions lost`, and the member rejoins
under a new member ID. Meanwhile another member took over the frozen
member's partitions from its last commit. The frozen process may still
finish the batch it was holding: records that the new owner processes too.
That is a zombie, and why task 10 makes handlers idempotent.

## Edge cases and gotchas

- **Overriding `OnPartitionsRevoked` drops franz-go's commit on revoke.** The
  default callback commits; yours replaces it. Commit yourself, or the next
  owner re-reads everything since the last autocommit.
- **`kgo.GroupProtocol` is not Java's `group.protocol`.** It sets the protocol
  *type* in JoinGroup (`"consumer"`, the default). KIP-848 is
  `kgo.ServerSideBalancer()`.
- **KIP-848 groups are invisible to classic describe.** `DescribeGroups`
  answers `GROUP_ID_NOT_FOUND` (state `Dead`) for a `consumer`-protocol group,
  and `ConsumerGroupDescribe` answers the same for a classic one. Anything
  built on the classic API, including `kadm.Lag` in v1.19.0 and older lag
  exporters, reports a KIP-848 group as missing. `watch` asks both.
- **Session timeout and heartbeat interval move to the broker with KIP-848.**
  `kgo.SessionTimeout`/`kgo.HeartbeatInterval` (Java `session.timeout.ms`,
  `heartbeat.interval.ms`) apply to classic groups only.
- **Changing the assignor is a migration.** Members with no common protocol
  can't be in one group (`INCONSISTENT_GROUP_PROTOCOL`). Roll out a list
  containing both first, then drop the old one. While members list both an
  eager and a cooperative balancer, one eager-only member joining downgrades
  the whole group to eager (franz-go logs it and revokes everything).
- **This cluster sets `group.initial.rebalance.delay.ms=0`.** The prod default
  is 3s: the first member of an *empty* classic group waits up to 3s for
  others before it gets partitions. Don't mistake that for a bug.
- **Idle members still cost.** Every member heartbeats and takes part in
  every classic rebalance. Two or three standbys are useful; forty are noise.
- **Uneven work isn't fixed by the assignor.** Assignors balance partition
  *counts*. A hot partition (task 02) stays on one member.

## Check yourself

<details><summary>1. You have 6 partitions and 10 consumers. Lag is growing. What does adding 5 more consumers change?</summary>

Nothing. At most 6 members own partitions; the rest are idle. You need faster
processing per partition (batching, in-process concurrency with per-key
ordering, task 13) or more partitions planned with the key-remapping cost in
mind (task 02).
</details>

<details><summary>2. What is the difference between OnPartitionsRevoked and OnPartitionsLost, and why must you not commit in Lost?</summary>

Revoked: you are still a group member and are handing partitions over; a
commit succeeds and is the right thing to do. Lost: the coordinator already
removed you (session expired, fenced); another member may own the partitions
and may have committed further. Your commit fails with `UNKNOWN_MEMBER_ID` /
`ILLEGAL_GENERATION` (or a fenced epoch with KIP-848).
</details>

<details><summary>3. Why can cooperative-sticky make the partition that moves wait longer than eager does?</summary>

It needs two rebalance rounds: one to take the partition away, one to give it
to the new owner. Each round is a barrier that waits for every member to
rejoin, and with `BlockRebalanceOnPoll` each member first finishes its
current batch. Eager does one round. Cooperative wins because the other
partitions keep flowing, not because the moved one is faster.
</details>

<details><summary>4. A pod is SIGKILLed. How long do its partitions go unprocessed, classic vs KIP-848?</summary>

Until the coordinator notices the missing heartbeats: the session timeout,
45s by default in both. Classic: `session.timeout.ms` set by the client;
KIP-848: `group.consumer.session.timeout.ms` on the broker (minimum 45s by
default). A clean `Close` (LeaveGroup) avoids the wait. Static membership
(task 07) deliberately trades a longer wait for no rebalance on restart.
</details>

<details><summary>5. You enable kgo.ServerSideBalancer() with kgo.RoundRobinBalancer(). What happens?</summary>

The client silently uses the classic protocol: franz-go speaks KIP-848 only
when the first balancer is a sticky one (server `uniform`) or range.
Verify with `watch` or `kafka-consumer-groups --describe`.
</details>

## franz-go ↔ Java client names

| franz-go | Java / docs | Note |
|---|---|---|
| `kgo.ConsumerGroup` | `group.id` | |
| `kgo.Balancers(kgo.CooperativeStickyBalancer())` | `partition.assignment.strategy` | franz-go default: cooperative-sticky. Java default `[RangeAssignor, CooperativeStickyAssignor]` runs range (eager) |
| `kgo.ServerSideBalancer()` + sticky/range balancer | `group.protocol=consumer` + `group.remote.assignor=uniform\|range` | KIP-848 |
| `kgo.GroupProtocol` | (none) | JoinGroup protocol type, *not* `group.protocol` |
| `kgo.SessionTimeout` (45s) | `session.timeout.ms` | classic only; KIP-848: broker `group.consumer.session.timeout.ms` |
| `kgo.HeartbeatInterval` (3s) | `heartbeat.interval.ms` | classic only; KIP-848: broker `group.consumer.heartbeat.interval.ms` (5s) |
| `kgo.RebalanceTimeout` (60s) | `max.poll.interval.ms` (Java sends it as the rebalance timeout) | Java also kicks a member that doesn't poll in time; franz-go doesn't |
| `kgo.OnPartitionsAssigned/Revoked/Lost` | `ConsumerRebalanceListener.onPartitionsAssigned/Revoked/Lost` | |
| `kgo.BlockRebalanceOnPoll` + `cl.AllowRebalance()` | default Java behaviour: rebalance callbacks run inside `poll()` | |
| `cl.PollRecords(ctx, n)` | `max.poll.records` (500) | |
| `kadm.DescribeGroups` / `kadm.DescribeConsumerGroups` | `Admin.describeConsumerGroups` (handles both) | |

## Further reading

- Kafka docs: [Consumer configs](https://kafka.apache.org/documentation/#consumerconfigs) and [Broker configs](https://kafka.apache.org/documentation/#brokerconfigs) (`group.consumer.*`)
- [KIP-429: Kafka Consumer Incremental Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol)
- [KIP-848: The Next Generation of the Consumer Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
- [KIP-345: Static membership](https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances) (task 07)
- franz-go: [package kgo](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo) (`BlockRebalanceOnPoll`, `ServerSideBalancer`, `OnPartitionsRevoked`) and [package kadm](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kadm)
