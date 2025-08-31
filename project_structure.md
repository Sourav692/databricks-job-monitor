databricks-job-monitor/
├── README.md
├── requirements.txt
├── cluster_analysis_simple.py
├── cluster_monitor.py
├── debug_tables.py
├── job_monitoring.log
├── job_monitoring_report_YYYYMMDD_HHMMSS.md
├── monitoring.log
├── quick_test.py
├── simple_test.py
├── test_connection.py
├── working_monitor.py
├── config/
│   ├── __init__.py
│   ├── database_config.json
│   └── settings.py
├── notebooks/
│   ├── cluster_performance_analysis.py
│   ├── cluster_performance_analysis copy.py
│   ├── job_monitoring_analysis.py
│   └── system_tables_exploration.py
├── scripts/
│   ├── deploy_to_databricks.py
│   ├── run_local_monitoring.py
│   ├── run_local_monitoring.py.bck
│   └── setup_monitoring.py
├── src/
│   ├── __init__.py
│   ├── dashboard/
│   │   ├── __init__.py
│   │   ├── job_dashboard.py
│   │   └── metrics_collector.py
│   ├── monitors/
│   │   ├── __init__.py
│   │   ├── base_monitor.py
│   │   ├── cluster_monitor.py
│   │   ├── job_monitor.py
│   │   ├── system_tables_client.py
│   │   └── system_tables_client copy.py
│   └── utils/
│       ├── __init__.py
│       ├── databricks_client.py
│       └── helpers.py
└── tests/
    └── __init__.py