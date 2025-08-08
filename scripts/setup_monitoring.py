#!/usr/bin/env python3
"""
Databricks Job Monitoring Framework Setup Script

This script initializes the environment for job monitoring, validates the configuration,
installs dependencies, tests Databricks connections, and generates a setup report.
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# ───────────────────────────────────────────────────────────────────────────────
# Path Setup
# ───────────────────────────────────────────────────────────────────────────────
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# ───────────────────────────────────────────────────────────────────────────────
# Internal Imports
# ───────────────────────────────────────────────────────────────────────────────
from config.settings import DatabricksConfig, MonitoringConfig
from utils.databricks_client import DatabricksClientManager
from monitors.system_tables_client import SystemTablesClient

# ───────────────────────────────────────────────────────────────────────────────
# Logging Configuration
# ───────────────────────────────────────────────────────────────────────────────
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('setup.log')
        ]
    )

# ───────────────────────────────────────────────────────────────────────────────
# Directory Setup
# ───────────────────────────────────────────────────────────────────────────────
def create_directory_structure():
    directories = ['logs', 'exports', 'config', 'data/cache']
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        logging.info(f"✅ Created directory: {directory}")

# ───────────────────────────────────────────────────────────────────────────────
# Environment Validation
# ───────────────────────────────────────────────────────────────────────────────
def validate_environment() -> Dict[str, Any]:
    validation_results = {
        'environment_vars': {},
        'databricks_connection': {},
        'system_tables_access': {},
        'errors': []
    }

    required_vars = ['DATABRICKS_HOST', 'DATABRICKS_TOKEN']
    optional_vars = ['DATABRICKS_WAREHOUSE_ID', 'DATABRICKS_CLUSTER_ID']

    for var in required_vars:
        value = os.getenv(var)
        is_present = bool(value)
        validation_results['environment_vars'][var] = {
            'present': is_present,
            'value_length': len(value) if value else 0
        }
        if not is_present:
            validation_results['errors'].append(f"Required environment variable {var} is missing")

    for var in optional_vars:
        value = os.getenv(var)
        validation_results['environment_vars'][var] = {
            'present': bool(value),
            'value_length': len(value) if value else 0
        }

    return validation_results

# ───────────────────────────────────────────────────────────────────────────────
# Databricks Connection Test
# ───────────────────────────────────────────────────────────────────────────────
def test_databricks_connection() -> Dict[str, Any]:
    try:
        config = DatabricksConfig.from_environment()
        client_manager = DatabricksClientManager(config.host, config.token)
        connection_results = client_manager.test_connection()

        if connection_results.get('workspace'):
            logging.info(f"✅ Workspace connection successful (User: {connection_results.get('user', 'Unknown')})")
        else:
            logging.error("❌ Workspace connection failed")

        if connection_results.get('sql'):
            logging.info("✅ SQL Warehouse connection successful")
        else:
            logging.warning("⚠️ SQL Warehouse connection not available")

        return connection_results
    except Exception as e:
        logging.error(f"❌ Connection test failed: {e}")
        return {'workspace': False, 'sql': False, 'errors': [str(e)]}

# ───────────────────────────────────────────────────────────────────────────────
# System Table Access Test
# ───────────────────────────────────────────────────────────────────────────────
def test_system_tables_access() -> bool:
    try:
        config = DatabricksConfig.from_environment()
        system_client = SystemTablesClient(config)
        result = system_client.query_system_table("SELECT COUNT(*) as count FROM system.lakeflow.jobs LIMIT 1")
        if not result.empty:
            logging.info("✅ System tables access successful")
            return True
        else:
            logging.error("❌ System tables query returned empty result")
            return False
    except Exception as e:
        logging.error(f"❌ System tables access failed: {e}")
        return False

# ───────────────────────────────────────────────────────────────────────────────
# Create Sample Config Files
# ───────────────────────────────────────────────────────────────────────────────
def create_sample_config():
    env_content = """# Databricks Configuration
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
DATABRICKS_WAREHOUSE_ID=your-sql-warehouse-id
DATABRICKS_CLUSTER_ID=your-cluster-id

# Monitoring Configuration
MONITORING_REFRESH_INTERVAL=300
MONITORING_RETENTION_DAYS=30
"""
    Path('.env.sample').write_text(env_content)
    logging.info("✅ Sample .env file created")

    monitoring_config = {
        "refresh_interval_minutes": 5,
        "retention_days": 30,
        "alert_thresholds": {
            "cpu_utilization_threshold": 80.0,
            "memory_utilization_threshold": 85.0,
            "job_duration_threshold_minutes": 60,
            "failure_rate_threshold": 0.1
        },
        "dashboard_settings": {
            "auto_refresh": True,
            "default_time_range_days": 7,
            "max_items_in_charts": 50
        }
    }
    config_path = Path('config/monitoring_config.json')
    config_path.write_text(json.dumps(monitoring_config, indent=2))
    logging.info(f"✅ Sample monitoring config created: {config_path}")

# ───────────────────────────────────────────────────────────────────────────────
# Install Dependencies
# ───────────────────────────────────────────────────────────────────────────────
def install_dependencies() -> bool:
    import subprocess
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        logging.info("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ Failed to install dependencies: {e}")
        return False

# ───────────────────────────────────────────────────────────────────────────────
# Generate Markdown Setup Report
# ───────────────────────────────────────────────────────────────────────────────
def generate_setup_report(validation_results: Dict[str, Any]) -> str:
    report = f"""# Databricks Job Monitoring Framework Setup Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Environment Validation
"""

    report += "\n### Environment Variables\n"
    for var, info in validation_results['environment_vars'].items():
        status = "✅" if info['present'] else "❌"
        report += f"- {var}: {status} {'Present' if info['present'] else 'Missing'}\n"

    if 'databricks_connection' in validation_results:
        conn = validation_results['databricks_connection']
        report += "\n### Databricks Connection\n"
        report += f"- Workspace: {'✅ Connected' if conn.get('workspace') else '❌ Failed'}\n"
        report += f"- SQL Warehouse: {'✅ Connected' if conn.get('sql') else '❌ Failed'}\n"

    if 'system_tables_access' in validation_results:
        access = validation_results['system_tables_access']
        report += f"\n### System Tables Access\n- Access: {'✅ Successful' if access else '❌ Failed'}\n"

    if validation_results['errors']:
        report += "\n### Issues Found\n"
        for error in validation_results['errors']:
            report += f"- ❌ {error}\n"

    report += "\n## Next Steps\n"
    if not validation_results['errors']:
        report += "- ✅ Setup completed successfully\n"
        report += "- Run `python scripts/run_local_monitoring.py` to start monitoring\n"
        report += "- Use `streamlit run src/dashboard/job_dashboard.py` for interactive dashboard\n"
    else:
        report += "- ❌ Please resolve the issues above before proceeding\n"
        report += "- Check your environment variables and Databricks configuration\n"
        report += "- Ensure you have proper permissions to access system tables\n"

    return report

# ───────────────────────────────────────────────────────────────────────────────
# Main Entry Point
# ───────────────────────────────────────────────────────────────────────────────
def main():
    setup_logging()
    logging.info("🚀 Starting Databricks Job Monitoring Framework Setup")

    create_directory_structure()
    install_dependencies()
    create_sample_config()

    logging.info("🔍 Validating environment...")
    validation_results = validate_environment()

    logging.info("🔗 Testing Databricks connection...")
    connection_results = test_databricks_connection()
    validation_results['databricks_connection'] = connection_results

    if connection_results.get('workspace'):
        logging.info("📊 Testing system tables access...")
        validation_results['system_tables_access'] = test_system_tables_access()

    report = generate_setup_report(validation_results)
    report_path = f"setup_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    Path(report_path).write_text(report)

    logging.info(f"📋 Setup report saved: {report_path}")
    print(report)

    if not validation_results['errors']:
        logging.info("🎉 Setup completed successfully!")
        print("\n🎉 Setup completed successfully! You can now run the monitoring framework.")
    else:
        logging.error("❌ Setup completed with errors. Please review the report.")
        print("\n❌ Setup completed with errors. Please review the report.")

# ───────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
