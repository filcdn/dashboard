import { query } from './cloudflare-client.js'

const response = await query(
  `
  WITH ttfb_ranked AS (
  SELECT
    worker_ttfb,
    NTILE(100) OVER (ORDER BY worker_ttfb) AS percentile_rank
  FROM retrieval_logs
),
percentiles AS (
  SELECT
    MIN(worker_ttfb) FILTER (WHERE percentile_rank = 50) AS p50_worker_ttfb,
    MIN(worker_ttfb) FILTER (WHERE percentile_rank = 90) AS p90_worker_ttfb,
    MIN(worker_ttfb) FILTER (WHERE percentile_rank = 99) AS p99_worker_ttfb
  FROM ttfb_ranked
)
SELECT
  SUM(CASE WHEN cache_miss THEN 1 ELSE 0 END) AS cache_miss_requests,
  SUM(CASE WHEN NOT cache_miss THEN 1 ELSE 0 END) AS cache_hit_requests,
  SUM(egress_bytes) AS total_egress_bytes,
  COUNT(*) AS total_requests,
  p50_worker_ttfb,
  p90_worker_ttfb,
  p99_worker_ttfb
FROM
  retrieval_logs,
  percentiles;
`,
  [],
)

process.stdout.write(JSON.stringify(response.result[0].results[0]))
