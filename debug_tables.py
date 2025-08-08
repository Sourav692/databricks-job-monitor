import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config.settings import DatabricksConfig
from src.monitors.system_tables_client import SystemTablesClient

def debug_system_tables():
    config = DatabricksConfig.from_environment()
    client = SystemTablesClient(config)
    
    print("=== DEBUGGING SYSTEM TABLES ===")
    
    # 1. Check available schemas
    print("\n1. Available system schemas:")
    schemas = client.query_system_table("SHOW SCHEMAS IN system")
    print(schemas.to_string(index=False))
    
    # 2. Check lakeflow tables
    print("\n2. Available lakeflow tables:")
    lakeflow_tables = client.query_system_table("SHOW TABLES IN system.lakeflow")
    print(lakeflow_tables.to_string(index=False))
    
    # 3. Check job timeline table schema
    print("\n3. Job timeline table schema:")
    timeline_schema = client.query_system_table("DESCRIBE system.lakeflow.job_run_timeline")
    print(timeline_schema.to_string(index=False))
    
    # 4. Check jobs table schema
    print("\n4. Jobs table schema:")
    jobs_schema = client.query_system_table("DESCRIBE system.lakeflow.jobs")
    print(jobs_schema.to_string(index=False))
    
    # 5. Check if there's any data in job_run_timeline (last 30 days)
    print("\n5. Sample data from job_run_timeline (last 30 days):")
    sample_data = client.query_system_table("""
        SELECT COUNT(*) as total_records, 
               MIN(period_start_time) as earliest_record,
               MAX(period_start_time) as latest_record
        FROM system.lakeflow.job_run_timeline
        WHERE period_start_time >= date_sub(current_timestamp(), 30)
    """)
    print(sample_data.to_string(index=False))
    
    # 6. Check if there's any data at all
    print("\n6. Total records in job_run_timeline:")
    total_records = client.query_system_table("SELECT COUNT(*) as total FROM system.lakeflow.job_run_timeline")
    print(total_records.to_string(index=False))
    
    client.close_connections()

if __name__ == "__main__":
    debug_system_tables()