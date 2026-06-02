SELECT
  problem_formerly_complaint_type AS complaint_type,
  total_requests,
  avg_response_hours
FROM gold_complaint_summary
ORDER BY total_requests DESC
LIMIT 15;