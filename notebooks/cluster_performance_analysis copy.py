# Databricks notebook source

# %% [markdown]
# # Cluster Performance Analysis
#
# This notebook analyzes cluster resource utilization and performance using system tables.
#
# ## Analysis Areas:
# - CPU and Memory utilization patterns
# - Network I/O performance
# - Cluster efficiency assessment
# - Cost optimization opportunities

# %% [markdown]
# ## Setup and Imports

# %%
%pip install databricks-sdk pandas plotly seaborn

# %%
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Import monitoring framework
import sys
sys.path.append('/Workspace/Shared/databricks-job-monitor/src')
from monitors.system_tables_client import SystemTablesClient
from monitors.cluster_monitor import ClusterMonitor
from config.settings import DatabricksConfig, MonitoringConfig

# %% [markdown]
# ## Initialize Monitoring

# %%
# Configuration
databricks_config = DatabricksConfig.from_environment()
monitoring_config = MonitoringConfig()

# Initialize monitors
system_client = SystemTablesClient(databricks_config)
cluster_monitor = ClusterMonitor(system_client, monitoring_config)
print("✅ Cluster monitoring initialized")

# %% [markdown]
# ## Data Collection

# %%
ANALYSIS_DAYS = 7
print(f"Collecting cluster metrics for the last {ANALYSIS_DAYS} days...")
cluster_metrics = cluster_monitor.get_metrics(ANALYSIS_DAYS)
print("📊 Available cluster metrics:")
for key, df in cluster_metrics.items():
    if isinstance(df, pd.DataFrame):
        print(f" - {key}: {len(df)} records")

# %% [markdown]
# ## CPU Utilization Analysis

# %%
if 'cluster_utilization' in cluster_metrics and not cluster_metrics['cluster_utilization'].empty:
    util_df = cluster_metrics['cluster_utilization']
    print("🖥️ CPU Utilization Summary:")
    print(f"Average CPU Utilization: {util_df['avg_cpu_utilization'].mean():.1f}%")
    print(f"Peak CPU Utilization: {util_df['peak_cpu_utilization'].max():.1f}%")
    print(f"Clusters Analyzed: {util_df['cluster_id'].nunique()}")
    
    # CPU utilization distribution
    fig = px.histogram(
        util_df,
        x='avg_cpu_utilization',
        bins=20,
        title='CPU Utilization Distribution Across Clusters',
        labels={'avg_cpu_utilization': 'Average CPU Utilization (%)'}
    )
    fig.show()
    
    # Top CPU consuming clusters
    top_cpu_clusters = util_df.nlargest(10, 'avg_cpu_utilization')
    fig = px.bar(
        top_cpu_clusters,
        x='cluster_name',
        y='avg_cpu_utilization',
        color='peak_cpu_utilization',
        title='Top 10 CPU Intensive Clusters',
        labels={'avg_cpu_utilization': 'Average CPU (%)', 'peak_cpu_utilization': 'Peak CPU (%)'}
    )
    fig.update_layout(xaxis_tickangle=-45, height=500)
    fig.show()
    
    # CPU vs Memory utilization scatter
    fig = px.scatter(
        util_df,
        x='avg_cpu_utilization',
        y='avg_memory_utilization',
        size='measurement_count',
        hover_data=['cluster_name'],
        title='CPU vs Memory Utilization',
        labels={
            'avg_cpu_utilization': 'Average CPU (%)',
            'avg_memory_utilization': 'Average Memory (%)'
        }
    )
    # Add efficiency zones
    fig.add_shape(
        type="rect", x0=20, y0=20, x1=80, y1=80,
        line=dict(color="green", width=2, dash="dash"),
        fillcolor="green", opacity=0.1
    )
    fig.add_annotation(
        x=50, y=50, text="Optimal Zone", showarrow=False,
        font=dict(color="green", size=14)
    )
    fig.show()
    
    display(util_df.head(15))

# %% [markdown]
# ## Memory Utilization Analysis

# %%
if 'cluster_utilization' in cluster_metrics:
    util_df = cluster_metrics['cluster_utilization']
    print("🧠 Memory Utilization Summary:")
    print(f"Average Memory Utilization: {util_df['avg_memory_utilization'].mean():.1f}%")
    print(f"Peak Memory Utilization: {util_df['peak_memory_utilization'].max():.1f}%")
    
    # Memory utilization by cluster type
    memory_by_node_type = util_df.groupby('node_type_id').agg({
        'avg_memory_utilization': ['mean', 'std', 'count']
    }).round(2)
    print("\n📊 Memory Utilization by Node Type:")
    display(memory_by_node_type)
    
    # Memory utilization over time (if data available)
    if 'monitoring_start' in util_df.columns:
        util_df['monitoring_date'] = pd.to_datetime(util_df['monitoring_start']).dt.date
        daily_memory = util_df.groupby('monitoring_date')['avg_memory_utilization'].mean().reset_index()
        fig = px.line(
            daily_memory,
            x='monitoring_date',
            y='avg_memory_utilization',
            title='Average Memory Utilization Trend',
            labels={'avg_memory_utilization': 'Average Memory (%)'}
        )
        fig.show()

# %% [markdown]
# ## Network Performance Analysis

# %%
if 'cluster_utilization' in cluster_metrics:
    util_df = cluster_metrics['cluster_utilization']
    print("🌐 Network Performance Summary:")
    avg_received = util_df['avg_network_mb_received_per_minute'].mean()
    avg_sent = util_df['avg_network_mb_sent_per_minute'].mean()
    print(f"Average Network Received: {avg_received:.1f} MB/min")
    print(f"Average Network Sent: {avg_sent:.1f} MB/min")
    
    # Network I/O comparison
    network_data = util_df[['cluster_name', 'avg_network_mb_received_per_minute', 'avg_network_mb_sent_per_minute']].head(15)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name='Received',
        x=network_data['cluster_name'],
        y=network_data['avg_network_mb_received_per_minute']
    ))
    fig.add_trace(go.Bar(
        name='Sent',
        x=network_data['cluster_name'],
        y=network_data['avg_network_mb_sent_per_minute']
    ))
    fig.update_layout(
        title='Network I/O by Cluster',
        xaxis_title='Cluster Name',
        yaxis_title='MB per Minute',
        barmode='group',
        xaxis_tickangle=-45,
        height=500
    )
    fig.show()
    
    # Network I/O vs CPU correlation
    fig = px.scatter(
        util_df,
        x='avg_cpu_utilization',
        y='avg_network_mb_received_per_minute',
        title='CPU Utilization vs Network Received',
        labels={
            'avg_cpu_utilization': 'Average CPU (%)',
            'avg_network_mb_received_per_minute': 'Network Received (MB/min)'
        }
    )
    fig.show()

# %% [markdown]
# ## Cluster Efficiency Analysis

# %%
if 'efficiency_metrics' in cluster_metrics and not cluster_metrics['efficiency_metrics'].empty:
    eff_df = cluster_metrics['efficiency_metrics']
    print("⚡ Cluster Efficiency Summary:")
    
    # Efficiency categories
    efficiency_counts = eff_df['efficiency_category'].value_counts()
    print("\nEfficiency Categories:")
    for category, count in efficiency_counts.items():
        print(f" - {category}: {count} clusters")
    
    # Efficiency category pie chart
    fig = px.pie(
        values=efficiency_counts.values,
        names=efficiency_counts.index,
        title='Cluster Efficiency Distribution'
    )
    fig.show()
    
    # Underutilized clusters analysis
    underutilized = eff_df[eff_df['efficiency_category'] == 'Underutilized']
    if not underutilized.empty:
        print(f"\n🔍 Underutilized Clusters Analysis ({len(underutilized)} clusters):")
        fig = px.scatter(
            underutilized,
            x='avg_cpu_utilization',
            y='avg_memory_utilization',
            size='total_measurements',
            title='Underutilized Clusters - Resource Usage Pattern',
            labels={
                'avg_cpu_utilization': 'Average CPU (%)',
                'avg_memory_utilization': 'Average Memory (%)'
            }
        )
        fig.show()
        display(underutilized[['cluster_id', 'avg_cpu_utilization', 'avg_memory_utilization', 'low_cpu_percent', 'low_memory_percent']].head(10))

# %% [markdown]
# ## Cost Analysis

# %%
if 'cost_analysis' in cluster_metrics and not cluster_metrics['cost_analysis'].empty:
    cost_df = cluster_metrics['cost_analysis']
    print("💰 Cost Analysis Summary:")
    print(f"Total Clusters with Cost Data: {len(cost_df)}")
    
    # Top cost-driving clusters
    top_cost_clusters = cost_df.nlargest(10, 'total_usage')
    fig = px.bar(
        top_cost_clusters,
        x='cluster_name',
        y='total_usage',
        color='sku_name',
        title='Top 10 Clusters by Usage',
        labels={'total_usage': 'Total Usage Units'}
    )
    fig.update_layout(xaxis_tickangle=-45, height=500)
    fig.show()
    
    # Usage by SKU type
    sku_usage = cost_df.groupby('sku_name')['total_usage'].sum().reset_index()
    fig = px.pie(
        sku_usage,
        values='total_usage',
        names='sku_name',
        title='Usage Distribution by SKU Type'
    )
    fig.show()
    
    display(cost_df.head(15))

# %% [markdown]
# ## Node Type Performance Comparison

# %%
if 'node_types' in cluster_metrics and not cluster_metrics['node_types'].empty:
    node_types_df = cluster_metrics['node_types']
    print("🖥️ Available Node Types:")
    print(f"Total Node Types: {len(node_types_df)}")
    
    # Node types by cores and memory
    fig = px.scatter(
        node_types_df,
        x='num_cores',
        y='memory_mb',
        color='category',
        size='num_cores',
        hover_data=['node_type_id'],
        title='Node Types: Cores vs Memory',
        labels={'memory_mb': 'Memory (MB)', 'num_cores': 'Number of Cores'}
    )
    fig.show()
    
    # Node type categories
    category_counts = node_types_df['category'].value_counts()
    fig = px.bar(
        x=category_counts.index,
        y=category_counts.values,
        title='Node Types by Category',
        labels={'x': 'Category', 'y': 'Count'}
    )
    fig.show()
    
    display(node_types_df.head(15))

# %% [markdown]
# ## Anomaly Detection

# %%
anomalies = cluster_monitor.detect_anomalies(cluster_metrics)
print("🚨 Cluster Anomalies Detected:")
print("=" * 50)
for anomaly_type, anomaly_list in anomalies.items():
    if anomaly_list:
        print(f"\n📊 {anomaly_type.replace('_', ' ').title()}: {len(anomaly_list)} detected")
        for anomaly in anomaly_list[:3]:  # Show top 3
            if 'cluster_name' in anomaly:
                cluster_info = f"{anomaly.get('cluster_name', 'Unknown')} ({anomaly.get('cluster_id', 'N/A')})"
            else:
                cluster_info = anomaly.get('cluster_id', 'Unknown')
            print(f" - {cluster_info}")
            if 'avg_cpu_utilization' in anomaly:
                print(f"   CPU: {anomaly['avg_cpu_utilization']:.1f}%")
            if 'avg_memory_utilization' in anomaly:
                print(f"   Memory: {anomaly['avg_memory_utilization']:.1f}%")
    else:
        print(f"\n✅ {anomaly_type.replace('_', ' ').title()}: None detected")

# %% [markdown]
# ## Recommendations

# %%
recommendations = []

# Resource optimization recommendations
if 'efficiency_metrics' in cluster_metrics:
    eff_df = cluster_metrics['efficiency_metrics']
    underutilized = eff_df[eff_df['efficiency_category'] == 'Underutilized']
    if not underutilized.empty:
        recommendations.append({
            'category': 'Resource Optimization',
            'priority': 'High',
            'recommendation': f'Consider downsizing {len(underutilized)} underutilized clusters to reduce costs',
            'potential_savings': 'High',
            'clusters': len(underutilized)
        })

# Performance optimization
if anomalies.get('overutilized_clusters'):
    recommendations.append({
        'category': 'Performance Optimization',
        'priority': 'Medium',
        'recommendation': f'Scale up {len(anomalies["overutilized_clusters"])} overutilized clusters to improve performance',
        'potential_impact': 'High',
        'clusters': len(anomalies['overutilized_clusters'])
    })

print("💡 Cluster Optimization Recommendations:")
print("=" * 50)
if recommendations:
    for i, rec in enumerate(recommendations, 1):
        print(f"\n{i}. {rec['category']} ({rec['priority']} Priority)")
        print(f"   {rec['recommendation']}")
        if 'clusters' in rec:
            print(f"   Affected clusters: {rec['clusters']}")
else:
    print("✅ No specific recommendations - clusters appear to be well-optimized!")

# %% [markdown]
# ## Generate Report

# %%
# Generate comprehensive cluster report
cluster_report = cluster_monitor.generate_cluster_report(ANALYSIS_DAYS)
print(cluster_report)

# Save detailed report
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
dbutils.fs.put(f"/tmp/cluster_analysis_report_{timestamp}.md", cluster_report, True)
print(f"\n💾 Detailed report saved to /tmp/cluster_analysis_report_{timestamp}.md")
print("📊 Cluster performance analysis completed!")