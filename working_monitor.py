import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv
from databricks import sql
import pandas as pd

# Load environment variables
load_dotenv()

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('monitoring.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def get_databricks_connection():
    """Get Databricks SQL connection"""
    host = os.getenv('DATABRICKS_HOST', '').replace('https://', '').replace('http://', '')
    token = os.getenv('DATABRICKS_TOKEN', '')
    warehouse_id = os.getenv('DATABRICKS_WAREHOUSE_ID', '')
    
    if not all([host, token, warehouse_id]):
        raise ValueError("Missing required environment variables")
    
    return sql.connect(
        server_hostname=host,
        http_path=f'/sql/1.0/warehouses/{warehouse_id}',
        access_token=token
    )

def execute_query(connection, query, description="Query"):
    """Execute a query and return results as DataFrame"""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"Executing {description}...")
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        
        df = pd.DataFrame(results, columns=columns)
        logger.info(f"{description} returned {len(df)} rows")
        return df
        
    except Exception as e:
        logger.error(f"{description} failed: {e}")
        return pd.DataFrame()

def get_job_runtime_metrics(connection, days=7):
    """Get job runtime metrics"""
    query = f"""
    WITH job_run_duration AS (
        SELECT 
            workspace_id,
            job_id,
            run_id,
            CAST(
                UNIX_TIMESTAMP(MAX(period_end_time)) - UNIX_TIMESTAMP(MIN(period_start_time))
                AS LONG
            ) as duration_seconds
        FROM system.lakeflow.job_run_timeline
        WHERE period_start_time >= date_sub(current_timestamp(), {days})
        GROUP BY workspace_id, job_id, run_id
    ),
    job_metadata AS (
        SELECT DISTINCT
            workspace_id,
            job_id,
            job_name,
            ROW_NUMBER() OVER(PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
        FROM system.lakeflow.jobs
    )
    SELECT 
        jrd.workspace_id,
        jrd.job_id,
        COALESCE(jm.job_name, CONCAT('Job_', jrd.job_id)) as job_name,
        COUNT(DISTINCT jrd.run_id) as total_runs,
        ROUND(AVG(jrd.duration_seconds), 2) as avg_duration_seconds,
        MIN(jrd.duration_seconds) as min_duration_seconds,
        MAX(jrd.duration_seconds) as max_duration_seconds,
        ROUND(AVG(jrd.duration_seconds) / 60, 2) as avg_duration_minutes,
        ROUND(MAX(jrd.duration_seconds) / 60, 2) as max_duration_minutes
    FROM job_run_duration jrd
    LEFT JOIN job_metadata jm ON jrd.workspace_id = jm.workspace_id 
        AND jrd.job_id = jm.job_id 
        AND jm.rn = 1
    GROUP BY jrd.workspace_id, jrd.job_id, jm.job_name
    HAVING COUNT(DISTINCT jrd.run_id) > 0
    ORDER BY avg_duration_seconds DESC
    """
    
    return execute_query(connection, query, "Job Runtime Metrics")

def get_job_failure_analysis(connection, days=7):
    """Get job failure analysis"""
    query = f"""
    WITH job_runs AS (
        SELECT DISTINCT
            jrt.workspace_id,
            jrt.job_id,
            jrt.run_id,
            jrt.result_state
        FROM system.lakeflow.job_run_timeline jrt
        WHERE jrt.period_start_time >= date_sub(current_timestamp(), {days})
            AND jrt.result_state IS NOT NULL
    ),
    job_metadata AS (
        SELECT DISTINCT
            workspace_id,
            job_id,
            job_name,
            ROW_NUMBER() OVER(PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
        FROM system.lakeflow.jobs
    )
    SELECT 
        jr.workspace_id,
        jr.job_id,
        COALESCE(jm.job_name, CONCAT('Job_', jr.job_id)) as job_name,
        COUNT(DISTINCT jr.run_id) as total_runs,
        COUNT(DISTINCT CASE WHEN jr.result_state = 'SUCCESS' THEN jr.run_id END) as successful_runs,
        COUNT(DISTINCT CASE WHEN jr.result_state IN ('FAILED', 'TIMEOUT', 'CANCELLED') THEN jr.run_id END) as failed_runs,
        ROUND(
            COUNT(DISTINCT CASE WHEN jr.result_state = 'SUCCESS' THEN jr.run_id END) * 100.0 / 
            COUNT(DISTINCT jr.run_id), 2
        ) as success_rate_percent,
        ROUND(
            COUNT(DISTINCT CASE WHEN jr.result_state IN ('FAILED', 'TIMEOUT', 'CANCELLED') THEN jr.run_id END) * 100.0 / 
            COUNT(DISTINCT jr.run_id), 2
        ) as failure_rate_percent
    FROM job_runs jr
    LEFT JOIN job_metadata jm ON jr.workspace_id = jm.workspace_id 
        AND jr.job_id = jm.job_id 
        AND jm.rn = 1
    GROUP BY jr.workspace_id, jr.job_id, jm.job_name
    HAVING COUNT(DISTINCT jr.run_id) > 0
    ORDER BY failure_rate_percent DESC, total_runs DESC
    """
    
    return execute_query(connection, query, "Job Failure Analysis")

def get_cluster_utilization(connection, days=1):
    """Get cluster utilization metrics"""
    query = f"""
    SELECT 
        cluster_id,
        driver,
        COUNT(*) as data_points,
        ROUND(AVG(cpu_user_percent + cpu_system_percent), 2) as avg_cpu_utilization,
        ROUND(MAX(cpu_user_percent + cpu_system_percent), 2) as peak_cpu_utilization,
        ROUND(AVG(cpu_wait_percent), 2) as avg_cpu_wait,
        ROUND(AVG(mem_used_percent), 2) as avg_memory_utilization,
        ROUND(MAX(mem_used_percent), 2) as peak_memory_utilization,
        ROUND(AVG(network_received_bytes)/(1024*1024), 2) as avg_network_mb_received_per_minute,
        ROUND(AVG(network_sent_bytes)/(1024*1024), 2) as avg_network_mb_sent_per_minute
    FROM system.compute.node_timeline
    WHERE start_time >= date_sub(current_timestamp(), {days})
    GROUP BY cluster_id, driver
    HAVING COUNT(*) > 10  -- Only clusters with significant data
    ORDER BY avg_cpu_utilization DESC
    LIMIT 20
    """
    
    return execute_query(connection, query, "Cluster Utilization")

def get_recent_job_activity(connection, days=1):
    """Get recent job activity summary"""
    query = f"""
    SELECT 
        DATE(period_start_time) as job_date,
        COUNT(DISTINCT job_id) as unique_jobs,
        COUNT(DISTINCT run_id) as total_runs,
        COUNT(DISTINCT CASE WHEN result_state = 'SUCCESS' THEN run_id END) as successful_runs,
        COUNT(DISTINCT CASE WHEN result_state IN ('FAILED', 'TIMEOUT', 'CANCELLED') THEN run_id END) as failed_runs,
        ROUND(AVG(UNIX_TIMESTAMP(period_end_time) - UNIX_TIMESTAMP(period_start_time)) / 60, 2) as avg_duration_minutes
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= date_sub(current_timestamp(), {days})
        AND result_state IS NOT NULL
    GROUP BY DATE(period_start_time)
    ORDER BY job_date DESC
    """
    
    return execute_query(connection, query, "Recent Job Activity")

def format_dataframe_for_display(df, title):
    """Format DataFrame for nice console display"""
    if df.empty:
        return f"\n{title}\n{'='*len(title)}\nNo data available\n"
    
    # Limit to first 10 rows for display
    display_df = df.head(10)
    
    output = f"\n{title}\n{'='*len(title)}\n"
    output += display_df.to_string(index=False, max_colwidth=30)
    
    if len(df) > 10:
        output += f"\n... and {len(df) - 10} more rows"
    
    output += f"\nTotal rows: {len(df)}\n"
    return output

def save_detailed_report(runtime_df, failure_df, cluster_df, activity_df):
    """Save detailed report to file"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"databricks_monitoring_report_{timestamp}.txt"
    
    with open(filename, 'w') as f:
        f.write("DATABRICKS JOB MONITORING REPORT\n")
        f.write("="*50 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Summary
        f.write("SUMMARY\n")
        f.write("-"*20 + "\n")
        f.write(f"Jobs analyzed: {len(runtime_df)}\n")
        f.write(f"Clusters monitored: {len(cluster_df)}\n")
        f.write(f"Total job runs: {failure_df['total_runs'].sum() if not failure_df.empty else 0}\n")
        
        if not failure_df.empty:
            avg_success_rate = failure_df['success_rate_percent'].mean()
            f.write(f"Average success rate: {avg_success_rate:.2f}%\n")
        f.write("\n")
        
        # Detailed data
        if not runtime_df.empty:
            f.write("JOB RUNTIME METRICS\n")
            f.write("-"*30 + "\n")
            f.write(runtime_df.to_string(index=False))
            f.write("\n\n")
        
        if not failure_df.empty:
            f.write("JOB FAILURE ANALYSIS\n")
            f.write("-"*30 + "\n")
            f.write(failure_df.to_string(index=False))
            f.write("\n\n")
        
        if not cluster_df.empty:
            f.write("CLUSTER UTILIZATION\n")
            f.write("-"*30 + "\n")
            f.write(cluster_df.to_string(index=False))
            f.write("\n\n")
        
        if not activity_df.empty:
            f.write("RECENT JOB ACTIVITY\n")
            f.write("-"*30 + "\n")
            f.write(activity_df.to_string(index=False))
            f.write("\n\n")
    
    return filename

def main():
    """Main monitoring function"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Starting Databricks Job Monitoring...")
    
    try:
        # Get connection
        logger.info("Establishing database connection...")
        connection = get_databricks_connection()
        logger.info("✅ Connected successfully!")
        
        # Get metrics
        logger.info("Fetching job runtime metrics...")
        runtime_metrics = get_job_runtime_metrics(connection, days=7)
        
        logger.info("Fetching job failure analysis...")
        failure_analysis = get_job_failure_analysis(connection, days=7)
        
        logger.info("Fetching cluster utilization...")
        cluster_utilization = get_cluster_utilization(connection, days=1)
        
        logger.info("Fetching recent job activity...")
        recent_activity = get_recent_job_activity(connection, days=3)
        
        # Display results
        print(format_dataframe_for_display(recent_activity, "RECENT JOB ACTIVITY (Last 3 Days)"))
        print(format_dataframe_for_display(runtime_metrics, "JOB RUNTIME METRICS (Last 7 Days)"))
        print(format_dataframe_for_display(failure_analysis, "JOB FAILURE ANALYSIS (Last 7 Days)"))
        print(format_dataframe_for_display(cluster_utilization, "CLUSTER UTILIZATION (Last 24 Hours)"))
        
        # Save detailed report
        report_file = save_detailed_report(runtime_metrics, failure_analysis, cluster_utilization, recent_activity)
        logger.info(f"✅ Detailed report saved to: {report_file}")
        
        # Display summary statistics
        print("\n" + "="*60)
        print("MONITORING SUMMARY")
        print("="*60)
        
        if not runtime_metrics.empty:
            avg_runtime = runtime_metrics['avg_duration_minutes'].mean()
            longest_job = runtime_metrics.loc[runtime_metrics['max_duration_minutes'].idxmax()]
            print(f"📊 Total jobs monitored: {len(runtime_metrics)}")
            print(f"⏱️  Average job runtime: {avg_runtime:.2f} minutes")
            print(f"🐌 Longest running job: {longest_job['job_name']} ({longest_job['max_duration_minutes']:.2f} min)")
        
        if not failure_analysis.empty:
            avg_success_rate = failure_analysis['success_rate_percent'].mean()
            total_runs = failure_analysis['total_runs'].sum()
            total_failures = failure_analysis['failed_runs'].sum()
            print(f"✅ Average success rate: {avg_success_rate:.2f}%")
            print(f"🏃 Total job runs: {total_runs}")
            print(f"❌ Total failures: {total_failures}")
        
        if not cluster_utilization.empty:
            avg_cpu = cluster_utilization['avg_cpu_utilization'].mean()
            avg_memory = cluster_utilization['avg_memory_utilization'].mean()
            print(f"🖥️  Average CPU utilization: {avg_cpu:.2f}%")
            print(f"💾 Average memory utilization: {avg_memory:.2f}%")
        
        print("="*60)
        print(f"📄 Full report saved to: {report_file}")
        print("✅ Monitoring completed successfully!")
        
        # Close connection
        connection.close()
        
    except Exception as e:
        logger.error(f"❌ Monitoring failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
