
# Databricks Job Monitoring Report
Generated: 2025-08-08 13:02:08
Period: Last 7 days

## Table of Contents
- [Databricks Job Monitoring Report](#databricks-job-monitoring-report)
  - [Table of Contents](#table-of-contents)
  - [Summary Statistics](#summary-statistics)
  - [Detected Anomalies](#detected-anomalies)
    - [Long Running Jobs (44 detected)](#long-running-jobs-44-detected)
    - [High Failure Rate Jobs (239 detected)](#high-failure-rate-jobs-239-detected)
  - [Job Runtime Details](#job-runtime-details)
  - [Cluster Utilization](#cluster-utilization)

## Summary Statistics

| Metric | Value |
| --- | --- |
| Total Jobs Monitored | 2987 |
| Average Job Runtime | 16.73 minutes |
| Average Success Rate | 0.00% |
| Total Job Runs | 477216 |
| Total Failures | 4377 |
| Average CPU Utilization | 88.55% |
| Average Memory Utilization | 51.98% |

## Detected Anomalies

### Long Running Jobs (44 detected)
- **[dev yuki_shiga] lakeflow_dab_job5** (ID: 357830163296216): Avg 6102.95 min
- **Flight Aware - JetBlueAirway** (ID: 485389964944575): Avg 4771.7 min
- **New Pipeline 2025-07-22 15:21** (ID: 269767756282634): Avg 4151.34 min
- **Back From Summit Data Test for ADF** (ID: 1095017166085251): Avg 3118.76 min
- **New Job Jul 29, 2025, 01:17 PM** (ID: 1040650006856672): Avg 2704.66 min

### High Failure Rate Jobs (239 detected)
- **nnu+test+pipeline job** (ID: 655507034139831): 100.00% failure rate
- **sql-server-ingestion job** (ID: 402879495656267): 100.00% failure rate
- **danielmartinez-20250530-ig job** (ID: 996903007367501): 100.00% failure rate
- **nnu9 job** (ID: 852037676170867): 100.00% failure rate
- **porto-ip-sql-server job** (ID: 383816017918972): 100.00% failure rate

## Job Runtime Details
```text
    workspace_id           job_id                                                                                                                                                                                                                 job_name  total_runs  avg_duration_seconds  min_duration_seconds  max_duration_seconds  median_duration_seconds  p90_duration_seconds  p95_duration_seconds
1444828305810485  357830163296216                                                                                                                                                                                       [dev yuki_shiga] lakeflow_dab_job5           1             366177.00                366177                366177                   366177                366177                366177
1444828305810485  485389964944575                                                                                                                                                                                             Flight Aware - JetBlueAirway           1             286302.00                286302                286302                   286302                286302                286302
1444828305810485  269767756282634                                                                                                                                                                                            New Pipeline 2025-07-22 15:21           6             249080.67                172803                630458                   172804                630458                630458
1444828305810485 1095017166085251                                                                                                                                                                                       Back From Summit Data Test for ADF           6             187125.67                 50482                301300                   172800                301300                301300
1444828305810485 1040650006856672                                                                                                                                                                                           New Job Jul 29, 2025, 01:17 PM        8051             162279.36                    14                175721                   172806                173067                173170
1444828305810485  560830474095338                                                                                                                                                                                                         test_single_node           1              86494.00                 86494                 86494                    86494                 86494                 86494
1444828305810485   77985888188966                                                                                                                                                                                                     na_ml_deployment_job           1              72189.00                 72189                 72189                    72189                 72189                 72189
1444828305810485  735601746358084                                                                                                                                                                                           New Job Jul 29, 2025, 10:36 AM           9              69978.56                  7449                 86426                    86368                 86426                 86426
1444828305810485  790723435805527                                                                                                                                             database_catalog_ingestion_pipeline_093ff5a9-4eeb-4070-a2cd-b07720162f48 job           9              69760.89                     0                627847                        0                627847                627847
2556758628403379  989098652134891                                                                                                                                                                                                       mfg_streaming_demo           9              56803.67                   763                342426                     5148                342426                342426
1444828305810485 1038398511058427                                                                                                                                                              [dev luis_cabral] Amsterdam clone of dbdemos-racing-sim-etl           1              52886.00                 52886                 52886                    52886                 52886                 52886
1660015457675682 1018868136746992                                                                                                                                                                                               field-demos_data-ingestion           1              47116.00                 47116                 47116                    47116                 47116                 47116

```

## Cluster Utilization
          cluster_id  driver  data_points  avg_cpu_utilization  peak_cpu_utilization  avg_cpu_wait  max_cpu_wait  avg_memory_utilization  max_memory_utilization  avg_network_mb_received_per_minute  avg_network_mb_sent_per_minute
0807-070013-qhfooily    True            6                98.05                 98.23          0.00          0.00                   43.80                   43.96                               16.94                            5.64
0803-070012-pbkwms5u    True            6                97.95                 98.13          0.00          0.00                   42.87                   43.28                               16.55                            5.30
0801-070014-yxxjq61e    True            7                97.80                 98.29          0.00          0.00                   43.14                   43.58                               16.89                            5.51
0802-070014-4hwg25g2    True            7                97.79                 97.91          0.00          0.00                   43.75                   43.84                               16.58                            5.40
0804-070014-56pylphu    True            6                97.62                 97.68          0.00          0.00                   44.07                   44.22                               17.19                            5.69
0805-070014-vaqt3y1e    True            6                97.25                 97.73          0.00          0.00                   43.11                   43.20                               17.05                            5.38
0806-070013-29ovht02    True            7                96.48                 97.17          0.00          0.00                   43.50                   43.93                               16.42                            5.45
0807-095750-4jddrpqq   False           12                88.69                 90.91          0.49          1.37                   55.07                   56.51                              298.60                           78.43
0807-215810-sln9bdw2   False           12                87.94                 89.18          0.36          0.67                   59.48                   60.41                              277.88                           73.16
0806-222634-2jldkcku   False           13                86.57                 90.97          0.22          0.72                   59.16                   60.38                              274.80                           72.21
0806-162813-3ag0wvma   False           14                85.51                 91.75          0.56          2.24                   59.67                   61.04                              225.07                           59.23
0806-003243-rde0zspe   False           14                85.09                 92.47          0.54          0.74                   61.28                   63.11                              288.56                           76.13
0806-114402-da1pc7wy   False            6                84.50                 88.87          0.46          0.86                   59.47                   59.61                              240.19                           63.01
0805-183505-jedfxt02   False           11                84.44                 89.59          0.55          0.83                   63.73                   68.81                              175.64                           46.02
0805-163151-g1cowcnm   False            9                84.26                 90.06          0.42          0.65                   60.90                   61.73                              225.57                           59.33
0804-110821-80uoxwr6   False          607                83.67                100.00          0.01          0.49                   38.67                   52.93                               12.15                            1.96
0801-063303-axhxui36   False           18                80.39                 92.50          0.33          0.53                   57.09                   61.15                              217.27                           56.99
0805-093613-o5brfk76   False           12                79.81                 91.81          0.86          1.32                   61.19                   66.25                              153.38                           40.19
0804-124734-mhjwa87m   False           18                78.63                 96.01          0.47          1.23                   59.02                   72.04                              193.73                           51.08
0807-235538-laopdssy   False          459                78.58                 99.51          5.86         49.35                   40.66                   60.14                             1227.31                         1892.99
