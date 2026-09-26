import http from 'k6/http'
import { check } from 'k6'
import { Trend, Rate } from 'k6/metrics'

// Load test for the StreamForge read path.
//
// k6 drives the API, not the pipeline. The pipeline's input is a streaming
// source, so its load is set by SYNTHETIC_RATE rather than by a request
// generator — those two are varied independently, and the point of the
// exercise is what happens to read latency while the write path is saturated.
//
// The aggregates query is separated from the stats query because they have
// completely different costs: /v1/stats is four PromQL instant queries against
// Prometheus, while /v1/aggregates is a filtered TimescaleDB scan whose cost
// grows with the stored data. Averaging them would hide whichever is slower.

const aggregatesLatency = new Trend('sf_aggregates_ms')
const statsLatency = new Trend('sf_stats_ms')
const errorRate = new Rate('sf_errors')

const BASE = __ENV.BASE_URL || 'http://localhost:8080'
const API_KEY = __ENV.API_KEY || ''

export const options = {
  // Ramping arrival rate, not a fixed VU count: this holds a target request
  // RATE regardless of how slow responses get. With fixed VUs a slowing server
  // automatically reduces offered load, which hides the degradation being
  // measured.
  scenarios: {
    read_path: {
      executor: 'ramping-arrival-rate',
      startRate: 5,
      timeUnit: '1s',
      preAllocatedVUs: 20,
      maxVUs: 200,
      stages: [
        { target: 25, duration: __ENV.RAMP || '20s' },
        { target: 50, duration: __ENV.HOLD || '30s' },
        { target: 100, duration: __ENV.PEAK || '20s' },
        { target: 0, duration: '10s' },
      ],
    },
  },
  thresholds: {
    // Deliberately generous: this is a laptop running the whole stack, and a
    // threshold tuned to production hardware would fail for reasons that say
    // nothing about the code.
    'http_req_failed': ['rate<0.05'],
    'sf_aggregates_ms': ['p(95)<2000'],
    'sf_stats_ms': ['p(95)<2000'],
  },
}

function headers() {
  return API_KEY ? { 'X-Api-Key': API_KEY } : {}
}

export default function () {
  const params = { headers: headers(), tags: { endpoint: 'aggregates' } }

  const aggregates = http.get(
    `${BASE}/v1/aggregates?metric=events_total`,
    params,
  )
  aggregatesLatency.add(aggregates.timings.duration)
  const aggregatesOk = check(aggregates, {
    'aggregates 200': (r) => r.status === 200,
    'aggregates is json': (r) => (r.headers['Content-Type'] || '').includes('json'),
  })

  const stats = http.get(`${BASE}/v1/stats`, {
    headers: headers(),
    tags: { endpoint: 'stats' },
  })
  statsLatency.add(stats.timings.duration)
  const statsOk = check(stats, {
    'stats 200': (r) => r.status === 200,
  })

  errorRate.add(!(aggregatesOk && statsOk))
}
