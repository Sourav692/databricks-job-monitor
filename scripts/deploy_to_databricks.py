import os
import sys
from pathlib import Path

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from databricks.sdk import WorkspaceClient
from config.settings import DatabricksConfig

def create_databricks_job(workspace_client: WorkspaceClient, notebook_path: str) -> str:
    """Create a monitoring job in Databricks"""
    from databricks.sdk.service.jobs import JobSettings as Job
    
    job_settings = Job.from_dict({
        "name": "Databricks Job Monitoring Framework",
        "tasks": [
            {
                "task_key": "job_monitoring_task",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "source": "WORKSPACE"
                },
                "timeout_seconds": 3600,
                "max_retries": 2
            }
        ],
        "schedule": {
            "quartz_cron_expression": "0 0 8 * * ?",  # Daily at 8 AM
            "timezone_id": "UTC",
            "pause_status": "UNPAUSED"
        },
        "email_notifications": {
            "on_failure": ["your-email@company.com"],
            "no_alert_for_skipped_runs": False
        }
    })
    
    job = workspace_client.jobs.create(**job_settings.as_shallow_dict())
    return job.job_id

def deploy_monitoring_framework():
    """Deploy the monitoring framework to Databricks"""
    config = DatabricksConfig.from_environment()
    
    if not config.host or not config.token:
        print("Error: Missing Databricks configuration")
        return
    
    workspace_client = WorkspaceClient(host=config.host, token=config.token)
    
    # Upload monitoring notebook
    notebook_content = """
# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Job Monitoring Framework
# MAGIC This notebook runs the comprehensive job monitoring analysis using system tables.

# COMMAND ----------

# MAGIC %pip install plotly streamlit pandas

# COMMAND ----------

# Import monitoring framework
import sys
import os
from datetime import datetime

# Your monitoring code here (copy from src directory)
# This would include the SystemTablesClient, JobMonitor, etc.

# COMMAND ----------

# Configure monitoring
from config.settings import DatabricksConfig, MonitoringConfig

databricks_config = DatabricksConfig.from_environment()
monitoring_config = MonitoringConfig()

# COMMAND ----------

# Run monitoring analysis
from monitors.system_tables_client import SystemTablesClient
from monitors.job_monitor import JobMonitor

system_client = SystemTablesClient(databricks_config)
job_monitor = JobMonitor(system_client, monitoring_config)

# Generate comprehensive report
report = job_monitor.generate_monitoring_report(days=7)
print(report)

# Save metrics for dashboard
metrics = job_monitor.get_comprehensive_job_metrics(days=7)

# COMMAND ----------

# Display results
displayHTML(f"<pre>{report}</pre>")
"""
    
    try:
        # Create or update the monitoring notebook
        notebook_path = "/Shared/job_monitoring_framework"
        
        workspace_client.workspace.upload(
            path=notebook_path,
            content=notebook_content.encode(),
            format="SOURCE",
            overwrite=True
        )
        
        print(f"✅ Monitoring notebook uploaded to: {notebook_path}")
        
        # Create scheduled job
        job_id = create_databricks_job(workspace_client, notebook_path)
        print(f"✅ Monitoring job created with ID: {job_id}")
        
        print("🎉 Deployment completed successfully!")
        print(f"📊 View job at: {config.host}/#job/{job_id}")
        
    except Exception as e:
        print(f"❌ Deployment failed: {e}")

if __name__ == "__main__":
    deploy_monitoring_framework()
