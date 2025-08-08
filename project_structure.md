databricks-job-monitor/
├── README.md
├── requirements.txt
├── setup.py
├── config/
│   ├── __init__.py
│   ├── settings.py
│   └── database_config.json
├── src/
│   ├── __init__.py
│   ├── monitors/
│   │   ├── __init__.py
│   │   ├── base_monitor.py
│   │   ├── job_monitor.py
│   │   ├── cluster_monitor.py
│   │   └── system_tables_client.py
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── databricks_client.py
│   │   └── helpers.py
│   └── dashboard/
│       ├── __init__.py
│       ├── job_dashboard.py
│       └── metrics_collector.py
├── notebooks/
│   ├── job_monitoring_analysis.py
│   ├── cluster_performance_analysis.py
│   └── system_tables_exploration.py
├── scripts/
│   ├── setup_monitoring.py
│   ├── run_local_monitoring.py
│   └── deploy_to_databricks.py
└── tests/
    ├── __init__.py
    ├── test_monitors.py
    └── test_utils.py