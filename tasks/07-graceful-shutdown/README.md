# 07 · Graceful shutdown and deploys

> **Goal:** know exactly what happens to every partition when a consumer pod
> gets SIGTERM, gets OOM-killed, or comes back with the same name, and write
> the shutdown sequence that makes a rolling deploy cost about one second
> instead of a minute of frozen partitions and a pile of duplicates.

## Why this matters in prod

**Story 1: the lag alert that fires on every deploy.** A payments consumer
paged on-call during every release: consumer lag spiked for about 90 seconds,
then drained, and the ledger team kept finding a few dozen double-processed
events. Nothing was broken in the code. The service's `main` exited on
SIGTERM before the Kafka client was closed, so no member ever told the group
it was leaving. Each replacement pod joined a group that still contained the
dead member, and the whole group waited for that member's session to time
out, once per pod. Everything the dead pods had processed since their last
auto-commit was processed again. The fix was ten lines: drain, commit, close.

**Story 2: static membership made it worse.** Another team read that static
membership avoids rebalances, set `group.instance.id` to `$HOSTNAME` on a
Deployment, and raised the session timeout to 5 minutes "to be safe".
Deployment pods get a new random name on every rollout, so every rolled pod
left behind a ghost member that still owned its partitions. Each step of the
rollout froze two partitions for five minutes. Static membership needs a
**stable** identity (a StatefulSet ordinal) and a session timeout you can
afford to lose on a real crash.

## Concepts

**The shutdown sequence.** Kubernetes (or `docker stop`) sends SIGTERM, waits
`terminationGracePeriodSeconds` (30s by default; `STOP_GRACE` here), then
sends SIGKILL. Everything below has to fit in that window:

```
SIGTERM
  │ 1. stop polling           no new work
  │ 2. finish in-flight       bounded by SHUTDOWN_TIMEOUT < grace period
  │ 3. commit                 exactly what was finished
  │ 4. LeaveGroup             cl.Close(): the coordinator rebalances NOW
  │ 5. flush producers        records still in the buffer die with Close
  ▼ exit 0                    long before SIGKILL
```

**What a crash costs.** No commit and no LeaveGroup. The coordinator only
learns the member is gone when its heartbeats stop for `session.timeout.ms`
(45s in franz-go and in Kafka 4's broker defaults):

```
t=0     worker-b SIGKILLed               partitions 1,5: nobody fetches, lag grows
t=0-45s survivors keep going             the group has no idea
t≈47s   member expired → rebalance       survivors take 1,5 from b's LAST COMMIT
        → everything b did since that commit is processed again (duplicates)
        → survivors chew through 45s of backlog, so their own partitions lag too
```

A crashed pod that restarts in 3 seconds does **not** help: its new client is
a new member, the group waits for the old one to expire before it can
finish the rebalance, and the newcomer sits in JoinGroup for those 45s.

**Rebalances per rolling deploy.** With dynamic members every replica
restart is a leave (rebalance) plus a join (rebalance), so rolling N
replicas costs about 2N rebalances. What each one costs depends on the
protocol:

| | what pauses during a rebalance |
|---|---|
| classic, eager (`range`) | every partition of every member: all are revoked, then reassigned |
| classic, cooperative-sticky (franz-go default) | only partitions that move; the rest keep flowing |
| KIP-848 (`GROUP_PROTOCOL=consumer`) | only partitions that move; the broker assigns, members learn it on their next heartbeat (5s) |

**Static membership** (`kgo.InstanceID`, KIP-345). The member has a name that
survives restarts. On Close it does **not** send LeaveGroup (franz-go source:
`leave()` returns early for an instance ID; under KIP-848 it sends a
"temporary leave", epoch -2). Its partitions stay assigned to that name and
simply pause. If a member with the same instance ID joins before the session
timeout, the coordinator hands it the same assignment with no rebalance: the
generation does not even change. The price: a real crash is detected only
after the session timeout, so a big session timeout is a long outage.

```
dynamic:  a leaves ─► rebalance ─► a' joins ─► rebalance   (partitions move twice)
static :  a stops  ─► (pause)  ─► a  joins                 (same partitions, no rebalance)
```

**Commits during shutdown, the franz-go specifics.** Default autocommit
commits every 5s, and franz-go's default `OnPartitionsRevoked` commits
before a partition moves. Both commit what was returned by the *previous*
poll: calling `PollRecords` again is what declares the last batch done. That
one-poll lag makes autocommit at-least-once, and it is also the trap: if you
abandon half a batch and then poll once more, the abandoned records are
committed as done. `AutoCommitMarks` + `MarkCommitRecords` commits only what
you mark, so a cut-short batch commits exactly the finished jobs.

## Coming from Pulsar

| Pulsar | Kafka | The difference that bites |
|---|---|---|
| `consumer.close()`: unacked messages are redelivered to the other consumers right away | Close sends LeaveGroup and triggers a rebalance; no Close means no LeaveGroup | Kafka has no per-message redelivery: whole partitions move, and only through a rebalance |
| Consumer crash: broker sees the TCP connection drop and redelivers its unacked messages | The coordinator waits `session.timeout.ms` (45s) without heartbeats | A crash costs the session timeout for every partition the member owned |
| Shared / Key_Shared: the broker dispatches messages, membership changes are cheap | Partitions are assigned to members; every membership change is a rebalance | Deploys are membership changes, which is why this task exists |
| Individual acks: only the dead consumer's in-flight messages are replayed | One committed offset per partition | Everything after the last commit is replayed: duplicates grow with the commit interval |
| No identity across restarts needed | `group.instance.id` (static membership) | It trades rebalances for slower crash detection, and needs stable pod names |

Pulsar's consumer failover is per-message and broker-driven; Kafka's is
per-partition and session-driven. That's why a Kafka consumer's shutdown
code matters so much more than a Pulsar one's.

## Run it

```bash
make up                                                # once
make dev T=07-graceful-shutdown R=producer             # terminal 1: RATE jobs/s into t07.jobs
make workers T=07-graceful-shutdown                    # worker-a/b/c from the prod image, version v1
make logs R="worker-a worker-b worker-c"               # terminal 2
go run ./tasks/07-graceful-shutdown -role audit        # from the host, any time
```

The workers are the prod image (`Dockerfile` target `prod`): a static binary
as PID 1 on distroless, so SIGTERM reaches the Go process directly, exactly
as in Kubernetes. Each one runs role `consumer` with `INSTANCE=worker-a/b/c`
and JSON logs; `KAFKA_LOG_LEVEL=info` in `task.env` adds franz-go's group
logs (joins, generations, revokes, `leaving group`). Deploys and crashes:

```bash
make rollout T=07-graceful-shutdown V=v2 PAUSE=15      # recreate a, b, c one by one with VERSION=v2
make crash W=worker-b                                  # SIGKILL, like an OOM-kill; it stays down
docker compose --profile prod start worker-b           # bring it back
SINCE=2m go run ./tasks/07-graceful-shutdown -role audit
```

`audit` reads both topics and prints, per partition of `t07.jobs`: jobs,
done, DUPLICATES (processed more than once), LOST (below the group's committed
offset but never processed), PENDING (not processed yet), MAX WAIT (job
created → first result) and the chain of owners (`worker@version`); then
every **stall** (a run of jobs that waited longer than `STALL`) with the
owner before and after it. `SINCE` limits it to recent jobs, so audit right
after each experiment.

Run the reference with `T=07-graceful-shutdown/solution` in `workers` and
`rollout`. Without Docker, build once and play the orchestrator yourself:

```bash
go build -o /tmp/t07 ./tasks/07-graceful-shutdown
KAFKA_LOG_LEVEL=info INSTANCE=worker-a VERSION=v1 /tmp/t07 -role consumer   # x3 terminals: a, b, c
kill -TERM <pid>    # a deploy step;   kill -KILL <pid>    # an OOM-kill
```

(Use a built binary, not `go run`: `kill` would hit the `go` command, not
your program.) Knobs are in [`task.env`](task.env); compose recreates the
workers when it changes, so put them there and re-run `make workers`.

## Your task

The starter **runs**: the worker processes jobs fine and shuts down the way
many services do, by simply ending the process on SIGTERM (here `os.Exit`;
in real code it's `log.Fatal`, a `main` that returns without waiting for the
consumer goroutine, or a 1-second grace period). Run experiment A first and
keep the numbers, then fix it step by step. A rollout stops the *old*
version, so after each step roll out twice (`V=v2`, then `V=v3`) and audit
the second one with `SINCE` covering only it.

1. **TODO(07.1) Leave, don't exit.** Replace `os.Exit` with leaving the loop
   so the deferred `cl.Close()` runs: a dynamic member sends LeaveGroup and
   the group rebalances immediately.
   *Done when:* `make rollout` produces stalls of one to three seconds
   instead of a minute, and every stopping worker logs franz-go's `leaving
   group`. Now roll out with `PROCESS_TIME=200ms` and look at LOST (measured:
   2 in one rollout). The job in flight at SIGTERM is abandoned (`batch cut
   short ... abandoned=1`), the next poll declares it done, and Close's
   revoke commits it. You traded duplicates for lost jobs.
2. **TODO(07.2) Drain the in-flight batch.** Process with a context that
   survives SIGTERM: `context.WithoutCancel(ctx)`, cancelled
   `SHUTDOWN_TIMEOUT` after the signal (`context.AfterFunc` + `time.AfterFunc`).
   *Done when:* a rollout from this version to itself shows LOST 0 and no
   worker logs `batch cut short`. (A duplicate or two can remain: a
   rebalance can still revoke a partition while you are mid-batch. Step 3.)
   Then set `RATE=1 PROCESS_TIME=2s MAX_POLL=20 SHUTDOWN_TIMEOUT=1s` (restart
   the producer too) and roll again: batches are cut short, and every
   abandoned job is LOST again (measured: `abandoned=20`, LOST 20).
3. **TODO(07.3) Commit exactly what you processed.** `kgo.AutoCommitMarks()`,
   `cl.MarkCommitRecords(r)` after each job's result is acked, and
   `kgo.BlockRebalanceOnPoll()` with `cl.AllowRebalance()` after every batch,
   so a rebalance can never take a partition away mid-batch. Because a custom
   `OnPartitionsRevoked` replaces franz-go's commit-on-revoke, yours must call
   `cl.CommitMarkedOffsets(ctx)`; it also runs on Close. Check `ctx` after
   the batch and break *before* polling again.
   *Done when:* with the slow settings from step 2, workers log `batch cut
   short` and the audit still shows LOST 0 and DUPLICATES 0.
4. **TODO(07.4) Static membership.** With `STATIC_MEMBERSHIP=true`, add
   `kgo.InstanceID(app.Instance)`. In compose `INSTANCE` is `worker-a/b/c`,
   which survives a container recreate, like a StatefulSet pod name.
   *Done when:* after a rollout every partition's OWNERS reads like
   `worker-a@v1 -> worker-a@v2`, the stall per step is roughly the restart
   time, and the workers that were not restarted log no `partitions
   assigned/revoked` at all.

## Experiments

Audit after each one with `SINCE` set to cover just that experiment. Between
experiments that change membership mode (static ↔ dynamic), give the group
a minute or use `-role evict` (D) so leftovers don't skew the next run.

**A. The naive deploy.** Starter as is, `make rollout T=07-graceful-shutdown
V=v2 PAUSE=15`, then `SINCE=3m ... -role audit`. Measured on this course's
cluster: **every** partition stalled 60 to 105 seconds and 157 jobs were
processed twice. Each new pod joined while the dead pod's member was still
in the group, the join waited for that member to expire, and the next step
started before the previous one had healed, so the rebalances chained. Why
are there so many duplicates when autocommit runs every 5 seconds? (franz-go
blocks commits between JoinGroup and SyncGroup, and the surviving v1
workers sat in a long join until they were killed themselves.)

**B. OOM-kill.** With the solution: `make crash W=worker-b`, wait a minute,
audit. b's two partitions stall about 47s (session timeout plus the
rebalance), then a and c pick them up from b's last commit: you see a handful
of DUPLICATES (up to `AutoCommitInterval`, 5s, of b's work) and the
survivors' *own* partitions stall several seconds while they catch up b's
backlog. `docker compose start worker-b` to restore. Now kill and restart
within 3 seconds (`make crash W=worker-b && docker compose start worker-b`):
the stall is still about 45s. Find `joining group` in worker-b's logs and the
`joined` line 40 seconds later.

**C. Static rollout and static crash.** `STATIC_MEMBERSHIP=true` in
`task.env`, `make workers T=07-graceful-shutdown/solution`, wait a minute,
`make rollout ... V=v3`. Per step: one stall of about the restart time
(measured 1.1 to 1.5s with a 1s restart), owners unchanged, no franz-go
`leaving group` line, and the `joined` line of the restarted worker shows
the same `generation` as before. Then `make crash W=worker-b` and
`docker compose start worker-b` 10 seconds later: b's partitions stall about
the 10s, nobody else notices. Crash it and **don't** restart: 45s, like a
dynamic member. Repeat with `SESSION_TIMEOUT=2m` and think about what that
setting means for a node that never comes back.

**D. Ghost members and eviction.** Static membership with unstable names,
from the host: run three replicas with `STATIC_MEMBERSHIP=true` and
`INSTANCE=worker-a`, `worker-b-7f9c`, `worker-c`, then `kill -TERM` the
middle one and start `INSTANCE=worker-b-x2k1`. The new member waits in
JoinGroup about 45s while the ghost `worker-b-7f9c` still owns its two
partitions. That is a Deployment with `group.instance.id=$HOSTNAME`. The
operator's escape hatch is the admin LeaveGroup by instance ID:
`INSTANCES=worker-b-7f9c go run ./tasks/07-graceful-shutdown -role evict`
(Java: `Admin.removeMembersFromConsumerGroup`). `make group G=t07.workers`
shows the GROUP-INSTANCE-ID column.

**E. Eager vs cooperative.** `BALANCER=range` in `task.env` and recreate
all three (a classic group only uses a protocol every member supports; you
may see `INCONSISTENT_GROUP_PROTOCOL` while old and new overlap), roll out,
and read the franz-go logs of a worker that was *not* restarted: on every
rebalance it logs `revoking all assignments because we are not cooperative`
and gets a different set back, so OWNERS churn (here `a -> b -> c` for one
partition in one rollout). With 100ms jobs the pause is short because every
member answers fast; the bill arrives with long batches (every member waits
for the slowest one's batch, with BlockRebalanceOnPoll), local caches or
state per partition, and hundreds of partitions. Cooperative members keep
the partitions that don't move.

**F. Grace period shorter than your drain.** `PROCESS_TIME=1s MAX_POLL=20
SHUTDOWN_TIMEOUT=60s` in `task.env` and `STOP_GRACE=5s make rollout ...`.
Docker SIGKILLs the worker mid-drain: no commit, no LeaveGroup, so you get
B's numbers on a "graceful" deploy. `SHUTDOWN_TIMEOUT` must be the grace
period minus the time Close needs, minus a margin.

**G. KIP-848.** `GROUP_PROTOCOL=consumer` and recreate all workers (an
empty classic group is converted). `docker compose exec kafka-1
/opt/kafka/bin/kafka-groups.sh --bootstrap-server kafka-1:29092 --list` shows
TYPE `Consumer`. A rollout costs about a second per step and only the
restarted worker's partitions pause. When a member leaves and nobody
replaces it, its partitions wait for the survivors' next heartbeat: up to
`group.consumer.heartbeat.interval.ms`, 5s (measured 5.0s). `SESSION_TIMEOUT`
is ignored now: with `SESSION_TIMEOUT=10s`, `make crash` still stalls the
dead member's partitions about 45s. The group config is bounded by the
broker:

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:29092 \
  --alter --entity-type groups --entity-name t07.workers --add-config consumer.session.timeout.ms=20000
# InvalidConfigurationException: ... must be greater than or equal to group.consumer.min.session.timeout.ms
```

In Kafka 4.3 that range is 45s to 60s (`group.consumer.min/max.session.timeout.ms`),
so the "static member with a 5-minute session timeout" trick needs broker
configuration under KIP-848. Remove the override with `--delete-config`.

**H. Someone renamed the group.** Set `GROUP=t07.workers-v2` in `task.env`
and roll out. Each v2 worker joins a brand-new group with no commits, and
franz-go's default `ConsumeStartOffset` is the start of the log: it
reprocesses all of `t07.jobs`. Until the rollout finishes, v1 workers (old
group) and v2 workers (new group) both own every partition, so every new job
is processed twice. (Java's default `auto.offset.reset=latest` would have
silently skipped everything between the old group's last commit and the
new group's start instead.) Audit with `GROUP=t07.workers-v2`.

## Edge cases and gotchas

- **Close does not flush.** `cl.Close()` fails whatever is still buffered in
  the producer. `Flush` first. And don't `Produce` with the context that
  SIGTERM cancels: franz-go may fail a buffered record whose context is done.
  The producer role uses `context.WithoutCancel` for exactly this.
- **Synchronous commits stall during joins.** franz-go blocks
  `CommitRecords`/`CommitOffsetsSync` between JoinGroup and SyncGroup. If you
  commit synchronously after every batch, one join that waits 45s for a dead
  member stalls *every* member, not just the dead one's partitions (measured
  here: all six partitions stalled about 45s). Autocommitted marks don't.
- **BlockRebalanceOnPoll has a budget.** A rebalance waits for your batch.
  `MAX_POLL × PROCESS_TIME` must stay far below the rebalance timeout (60s in
  franz-go), or the member is kicked out mid-batch.
- **No `max.poll.interval.ms` in franz-go.** Java leaves the group when you
  don't call `poll()` for 5 minutes. franz-go heartbeats from a background
  goroutine, so a stuck handler keeps its partitions forever. Watch lag.
- **PID 1 and the shell.** `ENTRYPOINT sh -c "./app"` puts a shell at PID 1
  that doesn't forward SIGTERM: every deploy becomes a SIGKILL after the grace
  period. Use exec form (as the `prod` target does) or `exec` in the script.
- **Kubernetes removes the pod from Service endpoints concurrently with
  SIGTERM.** For HTTP you add a `preStop` sleep; it counts against
  `terminationGracePeriodSeconds`, so it eats into your Kafka drain time.
- **Static + cooperative corner case.** If a static member revoked
  partitions in a cooperative rebalance and closes before rejoining, franz-go
  sends LeaveGroup anyway (and logs a warning), so the revoked partitions
  don't stay unassigned.
- **Scaling a StatefulSet down** with static membership leaves the removed
  ordinals in the group until their session expires. Evict them.
- **Mixed protocols.** Switching `BALANCER` or `GROUP_PROTOCOL` in a rolling
  deploy means old and new members coexist; classic groups pick the first
  protocol all members support. Change it in two deploys (add, then remove),
  see KIP-429.

## Check yourself

<details><summary>1. A dynamic member is SIGKILLed. Which partitions stop, for how long, and what gets processed twice?</summary>

Only its partitions stop, for about `session.timeout.ms` (45s) plus the
rebalance, because the coordinator only notices missing heartbeats. The new
owners start at the last committed offset, so everything the dead member
processed after that commit is processed again: up to the autocommit
interval of work, more if commits were blocked by a rebalance.
</details>

<details><summary>2. Kubernetes restarts the OOM-killed pod in 3 seconds. Why are its partitions still frozen for ~45s?</summary>

The restarted process is a new member (new member ID). Its join starts a
rebalance that cannot complete until the old member either rejoins (it
can't) or expires at the session timeout. With static membership and the
same instance ID, the new process would replace the old member immediately.
</details>

<details><summary>3. You enable static membership with SESSION_TIMEOUT=5m. What happens on a rolling deploy, and when a node dies?</summary>

Rolling deploy: each pod's partitions pause for its restart time, no
rebalances at all (if the pod name is stable). Node death: its partitions
are frozen for up to 5 minutes, because Close was never called and nobody
else may take them until the session expires (or an operator evicts the
instance ID).
</details>

<details><summary>4. Your shutdown abandons half a batch on SHUTDOWN_TIMEOUT, then calls PollRecords once more before Close. With default autocommit, what happens to the abandoned jobs?</summary>

They're lost. The poll promotes everything from the previous poll to
"committable", and Close's revoke commits it. Commit only what was marked
as processed, and don't poll after deciding to stop.
</details>

<details><summary>5. Why is a rolling deploy of 3 dynamic replicas up to 6 rebalances, and what do cooperative-sticky and KIP-848 change about it?</summary>

Each restart is a LeaveGroup (rebalance to move its partitions away) and a
join (rebalance to give it partitions back). They don't change the count.
They change the cost: eager revokes every partition from every member each
time; cooperative and KIP-848 only pause the partitions that actually move.
</details>

## franz-go ↔ Java client names

| franz-go | Java / docs | Note |
|---|---|---|
| `kgo.SessionTimeout` | `session.timeout.ms` | classic only; KIP-848: broker `group.consumer.session.timeout.ms`, group config `consumer.session.timeout.ms` |
| `kgo.HeartbeatInterval` (3s) | `heartbeat.interval.ms` (3s) | KIP-848: `group.consumer.heartbeat.interval.ms` (5s) |
| `kgo.RebalanceTimeout` (60s) | `max.poll.interval.ms` (5 min) | Java sends max.poll.interval.ms as the JoinGroup rebalance timeout |
| `kgo.InstanceID` | `group.instance.id` | static members don't send LeaveGroup on close (KIP-345) |
| default balancer `CooperativeStickyBalancer` | `partition.assignment.strategy=CooperativeStickyAssignor` | Java 4.x default is `[RangeAssignor, CooperativeStickyAssignor]` |
| `kgo.RangeBalancer` | `RangeAssignor` | eager |
| `kgo.ServerSideBalancer()` | `group.protocol=consumer` | + `group.remote.assignor` = `uniform` (sticky) or `range` |
| `kgo.AutoCommitInterval` (5s) | `auto.commit.interval.ms` (5s) | |
| `kgo.AutoCommitMarks` + `MarkCommitRecords` | no direct equivalent | closest: manual `commitAsync` of processed offsets |
| `kgo.OnPartitionsRevoked` | `ConsumerRebalanceListener.onPartitionsRevoked` | |
| `kgo.BlockRebalanceOnPoll` | (Java's default: rebalances run inside `poll()`) | |
| `kgo.ConsumeStartOffset` (start) | `auto.offset.reset` (latest) | different defaults, see experiment H |
| `cl.Close()` | `consumer.close()` | |
| `kadm.LeaveGroup(...).InstanceIDs(...)` | `Admin.removeMembersFromConsumerGroup` | |

## Further reading

- KIP-345: [Introduce static membership protocol to reduce consumer rebalances](https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances)
- KIP-429: [Kafka Consumer Incremental Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol)
- KIP-848: [The Next Generation of the Consumer Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
- Kafka docs: [consumer configs](https://kafka.apache.org/documentation/#consumerconfigs) and [broker group configs](https://kafka.apache.org/documentation/#brokerconfigs)
- Kubernetes: [Pod termination](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination)
- franz-go: [package kgo docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo) (read `InstanceID`, `BlockRebalanceOnPoll`, `AutoCommitMarks`)
