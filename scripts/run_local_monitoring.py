import os
import sys
import logging
from datetime import datetime
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import DatabricksConfig, MonitoringConfig
from src.monitors.system_tables_client import SystemTablesClient
from src.monitors.job_monitor import JobMonitor

def setup_logging():
    """Setup logging configuration with Windows-compatible encoding"""
    # Create a custom formatter that avoids unicode characters
    class WindowsSafeFormatter(logging.Formatter):
        def format(self, record):
            # Replace unicode characters with ASCII equivalents
            msg = super().format(record)
            return msg.replace('✅', '[SUCCESS]').replace('❌', '[ERROR]').replace('⚠️', '[WARNING]')
    
    formatter = WindowsSafeFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # File handler
    file_handler = logging.FileHandler('job_monitoring.log', encoding='utf-8')
    file_handler.setFormatter(formatter)
    
    # Console handler with encoding handling
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, console_handler]
    )

def print_safe(text):
    """Print text with safe encoding for Windows"""
    safe_text = text.replace('✅', '[SUCCESS]').replace('❌', '[ERROR]').replace('⚠️', '[WARNING]')
    safe_text = safe_text.replace('📊', '[CHART]').replace('🖥️', '[COMPUTE]').replace('📄', '[FILE]')
    print(safe_text)

def main():
    """Main monitoring execution for local development"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Starting Databricks job monitoring...")
    
    try:
        # Load configuration
        databricks_config = DatabricksConfig.from_environment()
        monitoring_config = MonitoringConfig()
        
        if not databricks_config.host or not databricks_config.token:
            logger.error("Missing required Databricks configuration. Please set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.")
            return False
        
        if not databricks_config.warehouse_id:
            logger.warning("No DATABRICKS_WAREHOUSE_ID provided. Some queries may fail.")
        
        logger.info(f"Running in {'local' if databricks_config.is_local_environment else 'Databricks'} environment")
        logger.info(f"Host: {databricks_config.host}")
        logger.info(f"Warehouse ID: {databricks_config.warehouse_id}")
        
        # Initialize components
        system_client = SystemTablesClient(databricks_config)
        
        # Test connection
        if system_client.test_connection():
            logger.info("[SUCCESS] Connection test successful!")
        else:
            logger.error("[ERROR] Connection test failed!")
            return False
        
        # Debug: Check available tables and schemas
        print_safe("\n" + "="*80)
        print_safe("DEBUGGING: CHECKING SYSTEM TABLES SCHEMA")
        print_safe("="*80)
        
        # Check if we have job data at all
        logger.info("Checking for available job data...")
        simple_job_data = system_client.get_simple_job_data(days=30)  # Check last 30 days
        
        if not simple_job_data.empty:
            print_safe(f"Found {len(simple_job_data)} jobs with activity in last 30 days:")
            print(simple_job_data.to_string(index=False))
        else:
            print_safe("No job activity found in last 30 days")
            
            # Let's check what's in the job timeline table
            logger.info("Checking job timeline table structure...")
            timeline_schema = system_client.get_table_schema('system.lakeflow.job_run_timeline')
            if not timeline_schema.empty:
                print_safe("Job timeline table schema:")
                print(timeline_schema.to_string(index=False))
        
        # Check recent activity
        recent_activity = system_client.get_recent_job_activity(days=7)
        if not recent_activity.empty:
            print_safe(f"\nRecent job activity (last 7 days):")
            print(recent_activity.to_string(index=False))
        
        job_monitor = JobMonitor(system_client, monitoring_config)
        
        # Get comprehensive metrics
        logger.info("Fetching comprehensive job metrics...")
        metrics = job_monitor.get_comprehensive_job_metrics(days=7)
        
        # Display results
        print_safe("\n" + "="*80)
        print_safe("DATABRICKS JOB MONITORING RESULTS")
        print_safe("="*80)
        
        if 'runtime_metrics' in metrics and not metrics['runtime_metrics'].empty:
            print_safe(f"\n[CHART] JOB RUNTIME METRICS ({len(metrics['runtime_metrics'])} jobs)")
            print_safe("-" * 50)
            print(metrics['runtime_metrics'].to_string(index=False))
        else:
            print_safe("\n[WARNING] No job runtime data found")
        
        if 'failure_analysis' in metrics and not metrics['failure_analysis'].empty:
            print_safe(f"\n[ERROR] JOB FAILURE ANALYSIS ({len(metrics['failure_analysis'])} jobs)")
            print_safe("-" * 50)
            print(metrics['failure_analysis'].to_string(index=False))
        else:
            print_safe("\n[WARNING] No job failure data found")
        
        if 'cluster_utilization' in metrics and not metrics['cluster_utilization'].empty:
            print_safe(f"\n[COMPUTE] CLUSTER UTILIZATION ({len(metrics['cluster_utilization'])} clusters)")
            print_safe("-" * 50)
            print(metrics['cluster_utilization'].to_string(index=False))
        else:
            print_safe("\n[WARNING] No cluster utilization data found")
        
        # Generate and save comprehensive report
        logger.info("Generating monitoring report...")
        report = job_monitor.generate_monitoring_report(days=7)
        
        # Save report
        report_filename = f"job_monitoring_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f"[SUCCESS] Monitoring report saved to: {report_filename}")
        
        # Display summary
        print_safe("\n" + "="*80)
        print_safe("SUMMARY")
        print_safe("="*80)
        
        if 'runtime_metrics' in metrics and not metrics['runtime_metrics'].empty:
            avg_runtime = metrics['runtime_metrics']['avg_duration_seconds'].mean() / 60
            longest_job = metrics['runtime_metrics'].loc[metrics['runtime_metrics']['max_duration_seconds'].idxmax()]
            print_safe(f"[CHART] Total jobs monitored: {len(metrics['runtime_metrics'])}")
            print_safe(f"[TIME] Average runtime: {avg_runtime:.2f} minutes")
            print_safe(f"[SLOW] Longest job: {longest_job['job_name']} ({longest_job['max_duration_seconds']/60:.2f} min)")
        
        if 'failure_analysis' in metrics and not metrics['failure_analysis'].empty:
            avg_success_rate = metrics['failure_analysis']['success_rate_percent'].mean()
            total_runs = metrics['failure_analysis']['total_runs'].sum()
            total_failures = metrics['failure_analysis']['failed_runs'].sum()
            print_safe(f"[SUCCESS] Average success rate: {avg_success_rate:.2f}%")
            print_safe(f"[RUN] Total runs: {total_runs}")
            print_safe(f"[ERROR] Total failures: {total_failures}")
        
        if 'cluster_utilization' in metrics and not metrics['cluster_utilization'].empty:
            avg_cpu = metrics['cluster_utilization']['avg_cpu_utilization'].mean()
            avg_memory = metrics['cluster_utilization']['avg_memory_utilization'].mean()
            print_safe(f"[COMPUTE] Average CPU: {avg_cpu:.2f}%")
            print_safe(f"[MEMORY] Average Memory: {avg_memory:.2f}%")
        
        print_safe(f"[FILE] Full report: {report_filename}")
        print_safe("="*80)
        
        # Close connections
        system_client.close_connections()
        logger.info("[SUCCESS] Monitoring completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"[ERROR] Error in monitoring execution: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
