# Utility functions for the monitoring framework
import os
import sys
import logging
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import DatabricksConfig, MonitoringConfig
from src.monitors.system_tables_client import SystemTablesClient
from src.monitors.job_monitor import JobMonitor

def main():
    """Main monitoring execution"""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        config = DatabricksConfig.from_environment()
        
        if not config.host or not config.token:
            logger.error("Missing Databricks configuration. Check your environment variables.")
            return
        
        # Initialize monitoring
        system_client = SystemTablesClient(config)
        monitor = JobMonitor(system_client, MonitoringConfig())
        
        # Generate report
        logger.info("Generating monitoring report...")
        report = monitor.generate_monitoring_report(days=7)
        
        # Save and display
        with open("monitoring_report.md", "w") as f:
            f.write(report)
        
        print(report)
        logger.info("Report saved to monitoring_report.md")
        
    except Exception as e:
        logger.error(f"Monitoring failed: {e}")
        raise

if __name__ == "__main__":
    main()