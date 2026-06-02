SELECT
  borough,
  problem_formerly_complaint_type AS complaint_type,
  total_requests,
  avg_response_hours
FROM gold_borough_complaint_summary
ORDER BY borough, total_requests DESC;