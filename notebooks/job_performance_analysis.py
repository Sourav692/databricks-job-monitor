import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import DatabricksConfig, MonitoringConfig
from src.monitors.system_tables_client import SystemTablesClient
from src.monitors.job_monitor import JobMonitor

def df_to_markdown(df):
    if df is None or (hasattr(df, 'empty') and df.empty):
        return "_No data available._"
    try:
        return df.to_markdown(index=False)
    except Exception:
        return "```\n" + df.to_string(index=False) + "\n```"

def small_table(rows):
    import pandas as _pd
    return df_to_markdown(_pd.DataFrame(rows))

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
        
        # Optional prechecks (silenced verbose console output)
        logger.info("Checking for available job data...")
        _ = system_client.get_simple_job_data(days=30)
        _ = system_client.get_table_schema('system.lakeflow.job_run_timeline')
        _ = system_client.get_recent_job_activity(days=7)
        
        job_monitor = JobMonitor(system_client, monitoring_config)
        
        # Get comprehensive metrics
        logger.info("Fetching comprehensive job metrics...")
        metrics = job_monitor.get_comprehensive_job_metrics(days=7)
        
        # Suppress verbose console display of tables
        
        # Generate and save comprehensive report
        logger.info("Generating monitoring report...")
        report = job_monitor.generate_monitoring_report(days=7)
        
        # Save report under project_root/job_report similar to cluster_report
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_dir = os.path.join(project_root, 'job_report')
        os.makedirs(report_dir, exist_ok=True)
        report_filename = f"job_monitoring_report_{timestamp}.md"
        report_path = os.path.join(report_dir, report_filename)
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f"[SUCCESS] Monitoring report saved to: {report_path}")
        
        # Final minimal console output
        print_safe(f"Report saved to: {report_path}")
        
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


