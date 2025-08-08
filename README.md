# 

A comprehensive framework for monitoring Databricks jobs using system tables, providing insights into job performance, resource utilization, and operational health.

## Features

- **Job Runtime Tracking**: Monitor job execution times, duration patterns, and performance trends
- **Resource Utilization**: Track CPU, memory, and network usage across clusters
- **Failure Analysis**: Analyze job success rates and failure patterns
- **Anomaly Detection**: Automatically detect performance anomalies and resource issues
- **Interactive Dashboard**: Streamlit-based dashboard for real-time monitoring
- **Local & Databricks Support**: Works in both local development and Databricks environments

## Quick Start

### 1. Environment Setup

```bash
# Clone or create the project structure
# Set environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"
export DATABRICKS_WAREHOUSE_ID="your-sql-warehouse-id"