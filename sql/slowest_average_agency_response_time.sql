SELECT
  agency,
  agency_name,
  total_requests,
  avg_response_hours
FROM gold_agency_performance
WHERE total_requests >= 100
ORDER BY avg_response_hours DESC
LIMIT 15;