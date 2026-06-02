SELECT
  agency,
  agency_name,
  total_requests,
  avg_response_hours,
  median_response_hours
FROM gold_agency_performance
ORDER BY total_requests DESC
LIMIT 20;