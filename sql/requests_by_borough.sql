SELECT
  borough,
  total_requests,
  avg_response_hours
FROM gold_borough_summary
ORDER BY total_requests DESC;