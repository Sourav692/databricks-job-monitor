# Databricks notebook source

# %% [markdown]
# # System Tables Exploration
#
# This notebook provides comprehensive exploration of Databricks system tables structure and capabilities.
#
# ## Tables Explored:
# - `system.lakeflow.*` - Job and pipeline data
# - `system.compute.*` - Cluster and compute data
# - `system.billing.*` - Usage and cost data
# - `system.access.*` - Audit and security data

# %% [markdown]
# ## Setup

# %%
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# %% [markdown]
# ## System Tables Discovery

# %%
print("🔍 Discovering available system tables...")
# Get all system schemas
schemas_query = "SHOW SCHEMAS IN system"
system_schemas = spark.sql(schemas_query).toPandas()
print("Available system schemas:")
for schema in system_schemas['databaseName'].tolist():
    print(f" - system.{schema}")

# %% [markdown]
# ## Lakeflow Schema Exploration

# %%
print("📊 Exploring system.lakeflow schema...")
try:
    # List tables in lakeflow schema
    lakeflow_tables = spark.sql("SHOW TABLES IN system.lakeflow").toPandas()
    print(f"Found {len(lakeflow_tables)} tables in system.lakeflow:")
    for table in lakeflow_tables['tableName'].tolist():
        print(f" - {table}")
        # Get table schema
        try:
            schema_info = spark.sql(f"DESCRIBE system.lakeflow.{table}").toPandas()
            print(f"  Columns: {len(schema_info)} ({', '.join(schema_info['col_name'].head(5).tolist())}...)")
        except Exception as e:
            print(f"  Error getting schema: {e}")
except Exception as e:
    print(f"❌ Error exploring lakeflow schema: {e}")

# %% [markdown]
# ## Jobs Table Deep Dive

# %%
print("🏗️ Deep dive into system.lakeflow.jobs table...")
try:
    # Table schema
    jobs_schema = spark.sql("DESCRIBE system.lakeflow.jobs").toPandas()
    print("Jobs table schema:")
    display(jobs_schema)
    
    # Sample data
    print("\nSample jobs data:")
    sample_jobs = spark.sql("""
        SELECT job_id, job_name, creator_user_name, change_time, trigger_type
        FROM system.lakeflow.jobs
        ORDER BY change_time DESC
        LIMIT 10
    """).toPandas()
    display(sample_jobs)
    
    # Jobs statistics
    jobs_stats = spark.sql("""
        SELECT 
            COUNT(*) as total_job_records,
            COUNT(DISTINCT job_id) as unique_jobs,
            COUNT(DISTINCT workspace_id) as workspaces,
            MIN(created_time) as oldest_job,
            MAX(created_time) as newest_job
        FROM system.lakeflow.jobs
    """).toPandas()
    print("Jobs table statistics:")
    display(jobs_stats)
except Exception as e:
    print(f"❌ Error exploring jobs table: {e}")

# %% [markdown]
# ## Job Run Timeline Analysis

# %%
print("⏱️ Analyzing job run timeline data...")
try:
    # Timeline schema
    timeline_schema = spark.sql("DESCRIBE system.lakeflow.job_run_timeline").toPandas()
    print("Job run timeline schema:")
    display(timeline_schema.head(15))
    
    # Recent timeline data
    recent_timeline = spark.sql("""
        SELECT 
            job_id, run_id, result_state, run_type,
            period_start_time, period_end_time,
            (period_end_time - period_start_time) as duration_seconds
        FROM system.lakeflow.job_run_timeline
        WHERE period_start_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
        ORDER BY period_start_time DESC
        LIMIT 20
    """).toPandas()
    print("\nRecent job run timeline (last 7 days):")
    display(recent_timeline)
    
    # Timeline statistics
    timeline_stats = spark.sql("""
        SELECT 
            COUNT(*) as total_timeline_records,
            COUNT(DISTINCT run_id) as unique_runs,
            COUNT(DISTINCT job_id) as jobs_with_runs,
            result_state,
            COUNT(*) as count_by_state
        FROM system.lakeflow.job_run_timeline
        WHERE period_start_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
        GROUP BY result_state
        ORDER BY count_by_state DESC
    """).toPandas()
    print("Job states distribution (last 7 days):")
    display(timeline_stats)
    
    # Visualize job states
    if not timeline_stats.empty:
        fig = px.pie(
            timeline_stats,
            values='count_by_state',
            names='result_state',
            title='Job Run States Distribution (Last 7 Days)'
        )
        fig.show()
except Exception as e:
    print(f"❌ Error analyzing timeline: {e}")

# %% [markdown]
# ## Compute Schema Exploration

# %%
print("🖥️ Exploring system.compute schema...")
try:
    # List compute tables
    compute_tables = spark.sql("SHOW TABLES IN system.compute").toPandas()
    print(f"Found {len(compute_tables)} tables in system.compute:")
    for table in compute_tables['tableName'].tolist():
        print(f" - {table}")
        # Get record counts
        try:
            count_result = spark.sql(f"SELECT COUNT(*) as count FROM system.compute.{table}").collect()
            count = count_result[0]['count']
            print(f"  Records: {count:,}")
        except Exception as e:
            print(f"  Error counting records: {e}")
except Exception as e:
    print(f"❌ Error exploring compute schema: {e}")

# %% [markdown]
# ## Clusters Table Analysis

# %%
print("🏛️ Analyzing system.compute.clusters table...")
try:
    # Clusters schema
    clusters_schema = spark.sql("DESCRIBE system.compute.clusters").toPandas()
    print("Clusters table schema:")
    display(clusters_schema.head(15))
    
    # Recent cluster configurations
    recent_clusters = spark.sql("""
        SELECT DISTINCT 
            cluster_id, cluster_name, cluster_source, state,
            driver_node_type_id, node_type_id, num_workers, change_time
        FROM system.compute.clusters
        WHERE change_time >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
        ORDER BY change_time DESC
        LIMIT 15
    """).toPandas()
    print("\nRecent cluster configurations:")
    display(recent_clusters)
    
    # Cluster statistics
    cluster_stats = spark.sql("""
        WITH latest_clusters AS (
            SELECT *, 
                   ROW_NUMBER() OVER(PARTITION BY cluster_id ORDER BY change_time DESC) as rn
            FROM system.compute.clusters
        )
        SELECT 
            COUNT(*) as total_clusters,
            COUNT(DISTINCT workspace_id) as workspaces,
            cluster_source, state,
            COUNT(*) as count_by_source_state
        FROM latest_clusters
        WHERE rn = 1
        GROUP BY cluster_source, state
        ORDER BY count_by_source_state DESC
    """).toPandas()
    print("Cluster statistics by source and state:")
    display(cluster_stats)
except Exception as e:
    print(f"❌ Error analyzing clusters: {e}")

# %% [markdown]
# ## Node Timeline Deep Dive

# %%
print("📈 Analyzing system.compute.node_timeline table...")
try:
    # Node timeline schema (first 20 columns)
    node_schema = spark.sql("DESCRIBE system.compute.node_timeline").toPandas()
    print("Node timeline schema (showing key columns):")
    display(node_schema.head(20))
    
    # Recent utilization data
    recent_utilization = spark.sql("""
        SELECT 
            cluster_id, instance_id, driver,
            cpu_user_percent, cpu_system_percent, cpu_wait_percent,
            mem_used_percent, network_received_bytes, network_sent_bytes,
            start_time, end_time
        FROM system.compute.node_timeline
        WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL 1 DAY
        ORDER BY start_time DESC
        LIMIT 20
    """).toPandas()
    print("\nRecent node utilization (last 24 hours):")
    display(recent_utilization)
    
    # Utilization statistics
    util_stats = spark.sql("""
        SELECT 
            COUNT(*) as total_measurements,
            COUNT(DISTINCT cluster_id) as unique_clusters,
            COUNT(DISTINCT instance_id) as unique_instances,
            AVG(cpu_user_percent + cpu_system_percent) as avg_cpu_utilization,
            AVG(mem_used_percent) as avg_memory_utilization,
            MAX(cpu_user_percent + cpu_system_percent) as max_cpu_utilization,
            MAX(mem_used_percent) as max_memory_utilization
        FROM system.compute.node_timeline
        WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
    """).toPandas()
    print("Utilization statistics (last 7 days):")
    display(util_stats)
except Exception as e:
    print(f"❌ Error analyzing node timeline: {e}")

# %% [markdown]
# ## Billing Schema Exploration

# %%
print("💰 Exploring system.billing schema...")
try:
    # List billing tables
    billing_tables = spark.sql("SHOW TABLES IN system.billing").toPandas()
    print(f"Found {len(billing_tables)} tables in system.billing:")
    for table in billing_tables['tableName'].tolist():
        print(f" - {table}")
    
    # Usage table analysis
    if 'usage' in billing_tables['tableName'].tolist():
        print("\nUsage table schema:")
        usage_schema = spark.sql("DESCRIBE system.billing.usage").toPandas()
        display(usage_schema.head(15))
        
        # Recent usage data
        recent_usage = spark.sql("""
            SELECT 
                workspace_id, usage_date, sku_name, usage_unit,
                usage_quantity, custom_tags
            FROM system.billing.usage
            WHERE usage_date >= CURRENT_DATE() - INTERVAL 7 DAYS
            ORDER BY usage_date DESC, usage_quantity DESC
            LIMIT 15
        """).toPandas()
        print("Recent billing usage (last 7 days):")
        display(recent_usage)
        
        # Usage by SKU
        sku_usage = spark.sql("""
            SELECT 
                sku_name, usage_unit,
                SUM(usage_quantity) as total_usage,
                COUNT(*) as usage_records
            FROM system.billing.usage
            WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
            GROUP BY sku_name, usage_unit
            ORDER BY total_usage DESC
            LIMIT 10
        """).toPandas()
        print("Top usage by SKU (last 30 days):")
        display(sku_usage)
except Exception as e:
    print(f"❌ Error exploring billing: {e}")

# %% [markdown]
# ## Access/Audit Schema

# %%
print("🔐 Exploring system.access schema...")
try:
    # List access tables
    access_tables = spark.sql("SHOW TABLES IN system.access").toPandas()
    print(f"Found {len(access_tables)} tables in system.access:")
    for table in access_tables['tableName'].tolist():
        print(f" - {table}")
    
    # Audit table analysis
    if 'audit' in access_tables['tableName'].tolist():
        print("\nAudit table schema:")
        audit_schema = spark.sql("DESCRIBE system.access.audit").toPandas()
        display(audit_schema.head(15))
        
        # Recent audit events
        recent_audit = spark.sql("""
            SELECT 
                timestamp, workspace_id,
                user_identity.email as user_email,
                service_name, action_name, request_id
            FROM system.access.audit
            WHERE DATE(timestamp) >= CURRENT_DATE() - INTERVAL 1 DAY
            ORDER BY timestamp DESC
            LIMIT 15
        """).toPandas()
        print("Recent audit events (last 24 hours):")
        display(recent_audit)
        
        # Audit activity by service
        service_activity = spark.sql("""
            SELECT 
                service_name, action_name,
                COUNT(*) as event_count
            FROM system.access.audit
            WHERE DATE(timestamp) >= CURRENT_DATE() - INTERVAL 7 DAYS
            GROUP BY service_name, action_name
            ORDER BY event_count DESC
            LIMIT 15
        """).toPandas()
        print("Top audit activity by service (last 7 days):")
        display(service_activity)
except Exception as e:
    print(f"❌ Error exploring access schema: {e}")

# %% [markdown]
# ## Cross-Table Analysis

# %%
print("🔗 Cross-table analysis examples...")
try:
    # Jobs with cluster utilization
    jobs_with_clusters = spark.sql("""
        WITH latest_jobs AS (
            SELECT *, 
                   ROW_NUMBER() OVER(PARTITION BY job_id ORDER BY change_time DESC) as rn
            FROM system.lakeflow.jobs
        ),
        cluster_utilization AS (
            SELECT 
                cluster_id,
                AVG(cpu_user_percent + cpu_system_percent) as avg_cpu_util,
                AVG(mem_used_percent) as avg_mem_util
            FROM system.compute.node_timeline
            WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
            GROUP BY cluster_id
        )
        SELECT 
            j.job_id, j.job_name,
            j.new_cluster.node_type_id,
            cu.avg_cpu_util, cu.avg_mem_util
        FROM latest_jobs j
        LEFT JOIN cluster_utilization cu ON j.new_cluster.cluster_id = cu.cluster_id
        WHERE j.rn = 1 AND cu.cluster_id IS NOT NULL
        LIMIT 10
    """).toPandas()
    
    if not jobs_with_clusters.empty:
        print("Jobs with associated cluster utilization:")
        display(jobs_with_clusters)
    else:
        print("No jobs with cluster utilization data found")
    
    # Job costs analysis (if billing data available)
    job_costs = spark.sql("""
        SELECT 
            usage_metadata['job_id'] as job_id,
            sku_name,
            SUM(usage_quantity) as total_usage
        FROM system.billing.usage
        WHERE usage_date >= CURRENT_DATE() - INTERVAL 7 DAYS
            AND usage_metadata['job_id'] IS NOT NULL
        GROUP BY usage_metadata['job_id'], sku_name
        ORDER BY total_usage DESC
        LIMIT 10
    """).toPandas()
    
    if not job_costs.empty:
        print("Job costs from billing data:")
        display(job_costs)
    else:
        print("No job cost data available in billing records")
except Exception as e:
    print(f"❌ Error in cross-table analysis: {e}")

# %% [markdown]
# ## Data Quality Assessment

# %%
print("✅ System tables data quality assessment...")
quality_checks = {}

# Check data freshness
try:
    freshness_check = spark.sql("""
        SELECT 'jobs' as table_name, 
               MAX(change_time) as latest_record,
               DATEDIFF(CURRENT_TIMESTAMP(), MAX(change_time)) as days_since_latest
        FROM system.lakeflow.jobs
        UNION ALL
        SELECT 'job_run_timeline' as table_name, 
               MAX(period_end_time) as latest_record,
               DATEDIFF(CURRENT_TIMESTAMP(), MAX(period_end_time)) as days_since_latest
        FROM system.lakeflow.job_run_timeline
        UNION ALL
        SELECT 'node_timeline' as table_name, 
               MAX(end_time) as latest_record,
               DATEDIFF(CURRENT_TIMESTAMP(), MAX(end_time)) as days_since_latest
        FROM system.compute.node_timeline
        UNION ALL
        SELECT 'billing_usage' as table_name, 
               MAX(usage_date) as latest_record,
               DATEDIFF(CURRENT_DATE(), MAX(usage_date)) as days_since_latest
        FROM system.billing.usage
    """).toPandas()
    print("Data freshness check:")
    display(freshness_check)
except Exception as e:
    print(f"❌ Error in freshness check: {e}")

# Check for null values in key columns
try:
    null_checks = spark.sql("""
        SELECT 'jobs.job_id' as column_check,
               COUNT(*) as total_records,
               SUM(CASE WHEN job_id IS NULL THEN 1 ELSE 0 END) as null_count
        FROM system.lakeflow.jobs
        UNION ALL
        SELECT 'timeline.cluster_id' as column_check,
               COUNT(*) as total_records,
               SUM(CASE WHEN cluster_id IS NULL THEN 1 ELSE 0 END) as null_count
        FROM system.compute.node_timeline
        WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
    """).toPandas()
    print("Null value checks in key columns:")
    display(null_checks)
except Exception as e:
    print(f"❌ Error in null checks: {e}")

# %% [markdown]
# ## Summary and Recommendations

# %%
print("📋 System Tables Exploration Summary")
print("=" * 50)
print("""
## Key Findings:

### Available System Schemas:
- system.lakeflow: Job and pipeline monitoring
- system.compute: Cluster and resource monitoring
- system.billing: Usage and cost tracking
- system.access: Audit and security events

### Data Volume and Freshness:
- Job data appears to be regularly updated
- Compute metrics provide minute-level granularity
- Billing data follows daily batch updates
- Audit logs capture real-time activity

### Key Tables for Monitoring:
1. system.lakeflow.job_run_timeline - Job execution tracking
2. system.compute.node_timeline - Resource utilization
3. system.billing.usage - Cost analysis
4. system.access.audit - Security monitoring

## Recommendations for Monitoring Framework:

### Data Collection Strategy:
- Use job_run_timeline for runtime and failure analysis
- Leverage node_timeline for resource optimization
- Correlate billing data with job/cluster usage
- Monitor audit logs for security insights

### Query Optimization:
- Use time-based partitioning in queries
- Leverage workspace_id and cluster_id for filtering
- Consider SCD2 nature of jobs and clusters tables
- Use appropriate aggregation windows for performance

### Monitoring Best Practices:
- Implement incremental data collection
- Cache frequently accessed metrics
- Set up alerting on key thresholds
- Regular data quality checks
""")

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
print(f"\n💾 Exploration completed at {timestamp}")
print("Ready to implement comprehensive monitoring framework! 🚀")