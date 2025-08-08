# Databricks notebook source

# %% [markdown]
# # Job Monitoring Analysis
#
# This notebook provides comprehensive analysis of Databricks job performance using system tables.
#
# ## Key Features:
# - Job runtime analysis and trends
# - Failure rate assessment
# - Resource utilization correlation
# - Performance recommendations

# %% [markdown]
# ## Setup and Configuration

# %%
# Install required packages
%pip install databricks-sdk pandas plotly

# %%
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Import monitoring framework components
import sys
sys.path.append('/Workspace/Shared/databricks-job-monitor/src')
from monitors.system_tables_client import SystemTablesClient
from monitors.job_monitor import JobMonitor
from config.settings import DatabricksConfig, MonitoringConfig

# %% [markdown]
# ## Initialize Monitoring Framework

# %%
# Configuration
databricks_config = DatabricksConfig.from_environment()
monitoring_config = MonitoringConfig()

# Initialize clients
system_client = SystemTablesClient(databricks_config)
job_monitor = JobMonitor(system_client, monitoring_config)
print("✅ Monitoring framework initialized successfully")

# %% [markdown]
# ## Data Collection

# %%
# Collect metrics for analysis
ANALYSIS_DAYS = 14
print(f"Collecting job metrics for the last {ANALYSIS_DAYS} days...")
metrics = job_monitor.get_comprehensive_job_metrics(ANALYSIS_DAYS)
print("📊 Available metrics:")
for key, df in metrics.items():
    if isinstance(df, pd.DataFrame):
        print(f" - {key}: {len(df)} records")
    else:
        print(f" - {key}: {type(df)}")

# %% [markdown]
# ## Job Runtime Analysis

# %%
if 'runtime_metrics' in metrics and not metrics['runtime_metrics'].empty:
    runtime_df = metrics['runtime_metrics']
    # Display summary statistics
    print("📈 Job Runtime Summary:")
    print(f"Total Jobs Analyzed: {len(runtime_df)}")
    print(f"Average Runtime: {runtime_df['avg_duration_seconds'].mean()/60:.1f} minutes")
    print(f"Total Runs: {runtime_df['total_runs'].sum()}")

    # Top 10 longest running jobs
    longest_jobs = runtime_df.nlargest(10, 'avg_duration_seconds')
    fig = px.bar(
        longest_jobs,
        x='job_name',
        y='avg_duration_seconds',
        title='Top 10 Longest Running Jobs (Average Duration)',
        labels={'avg_duration_seconds': 'Average Duration (seconds)'}
    )
    fig.update_layout(xaxis_tickangle=-45, height=500)
    fig.show()

    # Runtime distribution
    fig = px.histogram(
        runtime_df,
        x='avg_duration_seconds',
        bins=20,
        title='Job Runtime Distribution',
        labels={'avg_duration_seconds': 'Average Duration (seconds)'}
    )
    fig.show()

    # Display detailed table
    display(runtime_df.head(20))
else:
    print("❌ No runtime metrics available")

# %% [markdown]
# ## Job Success Rate Analysis

# %%
if 'failure_analysis' in metrics and not metrics['failure_analysis'].empty:
    failure_df = metrics['failure_analysis']
    print("🎯 Success Rate Summary:")
    print(f"Average Success Rate: {failure_df['success_rate_percent'].mean():.1f}%")
    print(f"Total Failures: {failure_df['failed_runs'].sum()}")

    # Jobs with highest failure rates
    high_failure_jobs = failure_df[failure_df['failure_rate_percent'] > 5].nlargest(10, 'failure_rate_percent')
    if not high_failure_jobs.empty:
        fig = px.bar(
            high_failure_jobs,
            x='job_name',
            y='failure_rate_percent',
            color='failure_rate_percent',
            title='Jobs with Highest Failure Rates (>5%)',
            labels={'failure_rate_percent': 'Failure Rate (%)'},
            color_continuous_scale='Reds'
        )
        fig.update_layout(xaxis_tickangle=-45, height=500)
        fig.show()

    # Success rate distribution
    fig = px.histogram(
        failure_df,
        x='success_rate_percent',
        bins=20,
        title='Job Success Rate Distribution',
        labels={'success_rate_percent': 'Success Rate (%)'}
    )
    fig.show()

    # Failure vs Success scatter
    fig = px.scatter(
        failure_df,
        x='total_runs',
        y='failure_rate_percent',
        size='failed_runs',
        hover_data=['job_name'],
        title='Job Failure Rate vs Total Runs',
        labels={
            'total_runs': 'Total Runs',
            'failure_rate_percent': 'Failure Rate (%)'
        }
    )
    fig.show()
    display(failure_df.head(20))
else:
    print("❌ No failure analysis data available")

# %% [markdown]
# ## Job Frequency Analysis

# %%
if 'job_frequency' in metrics and not metrics['job_frequency'].empty:
    freq_df = metrics['job_frequency']
    # Daily job execution trend
    daily_trend = freq_df.groupby('execution_date')['runs_per_day'].sum().reset_index()
    daily_trend = daily_trend.sort_values('execution_date')
    fig = px.line(
        daily_trend,
        x='execution_date',
        y='runs_per_day',
        title='Daily Job Execution Trend',
        labels={
            'execution_date': 'Date',
            'runs_per_day': 'Total Job Runs'
        }
    )
    fig.show()

    # Most frequently executed jobs
    job_frequency_summary = freq_df.groupby(['job_id'])['runs_per_day'].agg(['sum', 'mean', 'max']).reset_index()
    job_frequency_summary = job_frequency_summary.sort_values('sum', ascending=False).head(15)
    fig = px.bar(
        job_frequency_summary,
        x='job_id',
        y='sum',
        title='Most Frequently Executed Jobs (Total Runs)',
        labels={'sum': 'Total Runs', 'job_id': 'Job ID'}
    )
    fig.show()
    display(freq_df.head(20))
else:
    print("❌ No job frequency data available")

# %% [markdown]
# ## Anomaly Detection Results

# %%
# Detect and display anomalies
anomalies = job_monitor.detect_anomalies(metrics)
print("🚨 Detected Anomalies:")
print("=" * 50)
for anomaly_type, anomaly_list in anomalies.items():
    if anomaly_list:
        print(f"\n📊 {anomaly_type.replace('_', ' ').title()}: {len(anomaly_list)} detected")
        if anomaly_type == 'long_running_jobs':
            for anomaly in anomaly_list[:5]:  # Top 5
                print(f" - {anomaly['job_name']}: {anomaly['avg_duration_minutes']:.1f} min avg")
        elif anomaly_type == 'high_failure_rates':
            for anomaly in anomaly_list[:5]:  # Top 5
                print(f" - {anomaly['job_name']}: {anomaly['failure_rate_percent']:.1f}% failure rate")
    else:
        print(f"\n✅ {anomaly_type.replace('_', ' ').title()}: None detected")

# %% [markdown]
# ## Performance Recommendations

# %%
recommendations = []
# Runtime-based recommendations
if 'runtime_metrics' in metrics and not metrics['runtime_metrics'].empty:
    runtime_df = metrics['runtime_metrics']
    long_jobs = runtime_df[runtime_df['avg_duration_seconds'] > 3600]  # > 1 hour
    if not long_jobs.empty:
        recommendations.append({
            'category': 'Performance Optimization',
            'priority': 'High',
            'recommendation': f'Review {len(long_jobs)} jobs with average runtime > 1 hour for optimization opportunities',
            'jobs': long_jobs['job_name'].tolist()[:5]
        })
# Failure-based recommendations
if 'failure_analysis' in metrics and not metrics['failure_analysis'].empty:
    failure_df = metrics['failure_analysis']
    high_failure_jobs = failure_df[failure_df['failure_rate_percent'] > 10]
    if not high_failure_jobs.empty:
        recommendations.append({
            'category': 'Reliability Improvement',
            'priority': 'Critical',
            'recommendation': f'Investigate {len(high_failure_jobs)} jobs with failure rate > 10%',
            'jobs': high_failure_jobs['job_name'].tolist()[:5]
        })
# Display recommendations
print("💡 Performance Recommendations:")
print("=" * 50)
if recommendations:
    for i, rec in enumerate(recommendations, 1):
        print(f"\n{i}. {rec['category']} ({rec['priority']} Priority)")
        print(f" {rec['recommendation']}")
        if rec['jobs']:
            print(f" Affected jobs: {', '.join(rec['jobs'])}")
else:
    print("✅ No specific recommendations - system appears to be performing well!")

# %% [markdown]
# ## Export Results

# %%
# Export analysis results
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
# Create comprehensive report
report_content = f"""
# Job Monitoring Analysis Report
Generated: {datetime.now()}
Analysis Period: {ANALYSIS_DAYS} days
## Summary Statistics
"""
if 'runtime_metrics' in metrics and not metrics['runtime_metrics'].empty:
    runtime_df = metrics['runtime_metrics']
    report_content += f"""
### Runtime Metrics
- Total Jobs: {len(runtime_df)}
- Average Runtime: {runtime_df['avg_duration_seconds'].mean()/60:.1f} minutes
- Longest Job: {runtime_df['avg_duration_seconds'].max()/60:.1f} minutes
- Total Runs: {runtime_df['total_runs'].sum()}
"""
if 'failure_analysis' in metrics and not metrics['failure_analysis'].empty:
    failure_df = metrics['failure_analysis']
    report_content += f"""
### Success Rate Metrics
- Average Success Rate: {failure_df['success_rate_percent'].mean():.1f}%
- Total Failures: {failure_df['failed_runs'].sum()}
- Jobs with >10% Failure Rate: {len(failure_df[failure_df['failure_rate_percent'] > 10])}
"""
# Add anomalies to report
report_content += "\n## Detected Anomalies\n"
for anomaly_type, anomaly_list in anomalies.items():
    report_content += f"- {anomaly_type.replace('_', ' ').title()}: {len(anomaly_list)}\n"
# Save report
print("💾 Saving analysis report...")
dbutils.fs.put(f"/tmp/job_analysis_report_{timestamp}.md", report_content, True)
print(f"✅ Report saved to /tmp/job_analysis_report_{timestamp}.md")
print("📊 Analysis completed successfully!")