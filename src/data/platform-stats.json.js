import { query } from './cloudflare-client.js'

const response = await query(
  `
  SELECT
    SUM(CASE WHEN cache_miss THEN 1 ELSE 0 END) AS cache_miss_requests,
    SUM(CASE WHEN NOT cache_miss THEN 1 ELSE 0 END) AS cache_hit_requests,
    SUM(egress_bytes) AS total_egress_bytes,
    COUNT(*) AS total_requests,
  FROM
    retrieval_logs;
`,
  [],
)
const result = response.result[0].results[0]

const percentileResponse = await query(
  `WITH ranked AS (
  SELECT
    worker_ttfb,
    NTILE(100) OVER (ORDER BY worker_ttfb) AS percentile_rank
  FROM your_table
)
SELECT
  MIN(worker_ttfb) FILTER (WHERE percentile_rank = 50) AS p50,
  MIN(worker_ttfb) FILTER (WHERE percentile_rank = 90) AS p90,
  MIN(worker_ttfb) FILTER (WHERE percentile_rank = 99) AS p99;`,
)
result.p50_worker_ttfb = percentileResponse.result[0].results[0].p50
result.p90_worker_ttfb = percentileResponse.result[0].results[0].p90
result.p99_worker_ttfb = percentileResponse.result[0].results[0].p99

process.stdout.write(JSON.stringify(result))
