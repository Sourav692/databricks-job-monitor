
# Databricks Job Monitoring Report
Generated: 2025-08-08 02:28:04
Period: Last 7 days

## Summary Statistics
- Total Jobs Monitored: 2
- Average Job Runtime: 3.42 minutes
- Average Success Rate: 0.00%
- Total Job Runs: 6
- Total Failures: 1

## Detected Anomalies

### High Failure Rate Jobs (1 detected)
- **Job_2** (ID: 1097246715242524): 50.00% failure rate

## Job Runtime Details
   workspace_id           job_id job_name  total_runs  avg_duration_seconds  min_duration_seconds  max_duration_seconds  median_duration_seconds  p90_duration_seconds  p95_duration_seconds
348211221505628 1097246715242524    Job_2           2                 236.5                   187                   286                      187                   286                   286
348211221505628  479294266400229    Job_1           4                 174.5                     1                   394                        1                   394                   394

## Job Failure Analysis
   workspace_id           job_id job_name  total_runs  successful_runs  failed_runs success_rate_percent failure_rate_percent
348211221505628 1097246715242524    Job_2           2                0            1                 0.00                50.00
348211221505628  479294266400229    Job_1           4                0            0                 0.00                 0.00
