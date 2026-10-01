# StreamForge performance and tuning

Measured results from M6, with the reasoning behind them. Every number here
came from a run on the hardware described below — none are estimates.

## Test environment

A single laptop running everything: Redpanda, TimescaleDB, Prometheus, Grafana
and all four services. Apple Silicon, Docker Desktop, topics at **6 partitions,
replication factor 1**.

This matters when reading the numbers. A single-broker Redpanda sharing a
laptop with its own clients is not a throughput benchmark, and the absolute
figures would be very different on real hardware. What the setup *is* good for
is **relative** comparison: the same workload run against different
configurations, where the machine is the constant.

Tracing is off for every run. A span per event would measure the tracer rather
than the pipeline.

## How to reproduce

```bash
make up                                    # infra
make build                                 # services + lagprobe
./loadtest/run-experiment.sh baseline 2000 5ms none
make loadtest                              # k6 against the read path
```

`run-experiment.sh` starts the three pipeline stages with a given
configuration, samples consumer-group lag every 2s for the duration, then
scrapes the final counters. Results land in `loadtest/results/`.

## Measuring lag properly

The Grafana dashboard from M5 shows a *throughput funnel* — the ingester's
produce rate against the normalizer's. That tells you whether a stage is
falling behind, but not by how much.

`cmd/lagprobe` answers the real question. It uses `kadm.CalculateGroupLag` to
compare each partition's high watermark against the consumer group's committed
offset, which only the broker can report. That difference is the backlog in
records, and it is the number that actually grows when a stage cannot keep up.

## Result 1: the write path is bottlenecked on the normalizer

Baseline run — 2,000 events/sec offered, default tuning, 45 seconds:

| Stage | Records produced |
|---|---|
| ingester → `raw-events` | 102,704 |
| normalizer → `normalized-events` | **7,157** |
| aggregator windows closed | 11 |

Peak consumer-group lag: **95,775 records**.

The ingester kept up with offered load. The normalizer moved 7% of it, and the
backlog grew for the entire run.

## Result 2: producer linger is the cause, and it costs 6.8x

The normalizer and aggregator use `ProduceSync` — they block until Kafka
acknowledges each record before committing the consumer offset that produced
it. That was a deliberate M3 decision: it is what makes a crash redeliver the
record rather than lose it.

But `ProduceSync` sends **one record per batch**. Producer linger is the time
the client waits to accumulate more records before sending — and with one
record in flight there is nothing to accumulate, so the wait is pure added
latency on every single record.

At the default 5ms linger, that caps the stage at roughly 1/0.005 = 200
records/sec. Observed: 7,157 / 45s = **159 records/sec**. The prediction and
the measurement agree.

Setting `PRODUCER_LINGER=0s`:

| Configuration | Normalizer produced | Peak lag | Windows closed |
|---|---|---|---|
| linger 5ms (default) | 7,157 | 95,775 | 11 |
| **linger 0s** | **48,851** | 62,131 | 23 |

**6.8x throughput**, backlog growth cut by a third, and twice as many windows
completed.

### The knob is not globally good or bad

This is the part worth understanding. The ingester uses the *asynchronous*
`Produce`, where records genuinely accumulate and linger buys real batching.
The normalizer and aggregator use `ProduceSync`, where the same setting is
dead time.

**One knob, opposite effects, depending on which produce mode a stage uses.**
A single global `PRODUCER_LINGER` is therefore the wrong shape for this
pipeline. Linger is now resolved per stage: the `ProduceSync` stages
(normalizer, aggregator) default to 0s and the ingester keeps 5ms, with
`<SERVICE>_PRODUCER_LINGER` overriding one stage and `PRODUCER_LINGER` still
setting all of them for experiments. (Result 5 later removed the normalizer
from the `ProduceSync` group; it keeps the 0s default.)

## Result 3: compression made it worse

With linger already at 0, enabling lz4:

| Configuration | Normalizer produced | Peak lag |
|---|---|---|
| linger 0s, no compression | 48,851 | 62,131 |
| linger 0s, **lz4** | 33,687 | 80,827 |

A 31% regression. Same reason: compression pays off by shrinking *large*
batches, and a synchronous single-record send has no batch to shrink. What is
left is the CPU cost of compressing each small protobuf payload individually,
on a machine already running the broker.

Compression is worth revisiting for the ingester's asynchronous path, where
batches are real.

## Result 4: the read path holds up under load

`make loadtest` drives the API with a **ramping arrival rate** — a target
requests/sec rather than a fixed number of virtual users. With fixed VUs a
slowing server automatically offers less load, which hides exactly the
degradation being measured.

Ramp to 100 req/s over 45s, while the pipeline was running:

| Metric | Result |
|---|---|
| Requests | 3,858 |
| Failures | **0 (0.00%)** |
| Throughput | 86.3 req/s |
| `/v1/aggregates` | median 4.9ms, p95 **149.6ms**, max 939ms |
| `/v1/stats` | median 10.2ms, p95 **385.8ms**, max 885ms |

The two endpoints are tracked separately because their costs are unrelated:
`/v1/aggregates` is one filtered TimescaleDB scan, while `/v1/stats` issues
four PromQL instant queries to Prometheus. Averaging them would hide that
**stats is ~2.6x slower at p95** — the read path's slowest component is the
metrics backend, not the database.

33 iterations were dropped at peak, meaning the load generator itself could not
keep up on the same laptop. Another reason to read these as relative figures.

## Result 5: batching the synchronous path removes the bottleneck

Results 1–3 all trace back to one round trip per record. The consumer already
committed once per fetch; only the produce side was serial. The normalizer
now produces every record in a fetch asynchronously, waits once for all the
acknowledgements (`kafka.Batch`), and only then commits (`WithBeforeCommit`).

The delivery guarantee is unchanged. A failure anywhere in a fetch still
commits nothing and redelivers the whole fetch, exactly as a failure part way
through a fetch did before. Per-key order holds because the producer is
idempotent.

Each run below used a freshly recreated broker with 6-partition topics, linger
0s everywhere unless noted, 45 seconds. The old code was re-run the same day as
a control: it produced 32,998 at 2,000/s where Result 2 recorded 48,851. Same
code, same machine, a third lower — which is why the before/after comparison
has to be run back to back rather than against old numbers.

| Configuration | Offered | Ingester produced | Normalizer produced | Peak lag | Windows closed |
|---|---|---|---|---|---|
| per-record `ProduceSync` (control) | 2,000/s | 98,545 | 32,998 | 73,670 | 12 |
| **batched** | 2,000/s | 100,784 | **100,810** | **691** | 35 |
| batched, normalizer linger 5ms | 2,000/s | 103,477 | 103,519 | 95 | 36 |
| per-record `ProduceSync` (control) | 5,000/s | 254,167 | 43,085 | 216,796 | 11 |
| **batched** | 5,000/s | 232,900 | **233,029** | **856** | 36 |
| batched, normalizer linger 5ms | 5,000/s | 260,394 | 260,393 | 192 | 30 |

The normalizer went from moving a third of the offered load to **keeping up
with all of it, at both rates**. Lag stays in the hundreds, not the
hundreds of thousands. (The normalizer's count can slightly exceed the
ingester's because the two are scraped a moment apart.)

At 5,000/s the old code moved 43,085 records — about 960/s, its ceiling on this
machine. The batched normalizer's ceiling was not reached; finding it would
need a faster ingester or a second one.

**Linger no longer matters for the normalizer at these rates.** With a whole
fetch in flight, records do accumulate, so 5ms is no longer dead time — but
both settings kept up, so the throughput difference cannot be measured here.
Peak lag was lower with 5ms in both pairs, but on single runs with lag in the
hundreds that is within noise. The default stays at 0s.

## What would come next

- ~~**Clean shutdown mid-fetch.**~~ Done: `Consumer.Run` now treats an error
  caused by its own context's cancellation as a shutdown and returns nil, so
  the normalizer and aggregator exit 0 on a routine restart. The interrupted
  fetch is still left uncommitted and redelivered.
- **Scale the normalizer out.** It is a stateless consumer-group member and the
  topics now have 6 partitions, so replicas are the obvious lever. The
  aggregator is deliberately *not* scalable — see the README for why.
- Re-run on hardware where the broker is not competing with its own clients.
