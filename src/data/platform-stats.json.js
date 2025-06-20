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
    MIN(worker_ttfb) FILTER (WHERE percentile_rank = 10) AS client_ttfb_p10,
    MIN(worker_ttfb) FILTER (WHERE percentile_rank = 50) AS client_ttfb_p50,
    MIN(worker_ttfb) FILTER (WHERE percentile_rank = 90) AS client_ttfb_p90,
    MIN(worker_ttfb) FILTER (WHERE percentile_rank = 99) AS client_ttfb_p99
  FROM ttfb_ranked
)
SELECT
  SUM(CASE WHEN cache_miss THEN 1 ELSE 0 END) AS cache_miss_requests,
  SUM(CASE WHEN NOT cache_miss THEN 1 ELSE 0 END) AS cache_hit_requests,
  SUM(egress_bytes) AS total_egress_bytes,
  COUNT(*) AS total_requests,
  client_ttfb_p10,
  client_ttfb_p50,
  client_ttfb_p90,
  client_ttfb_p99
FROM
  retrieval_logs,
  percentiles;
`,
  [],
)

process.stdout.write(JSON.stringify(response.result[0].results[0]))
