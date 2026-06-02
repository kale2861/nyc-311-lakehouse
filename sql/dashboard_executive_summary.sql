SELECT
  'Total Requests' AS metric,
  CAST(SUM(total_requests) AS STRING) AS value
FROM gold_borough_summary

UNION ALL

SELECT
  'Average Response Hours' AS metric,
  CAST(ROUND(AVG(avg_response_hours), 2) AS STRING) AS value
FROM gold_borough_summary

UNION ALL

SELECT
  'Boroughs Analyzed' AS metric,
  CAST(COUNT(DISTINCT borough) AS STRING) AS value
FROM gold_borough_summary;