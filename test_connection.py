import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config.settings import DatabricksConfig
from src.monitors.system_tables_client import SystemTablesClient

def test_connection():
    print("Testing Databricks connection...")
    
    # Load config
    config = DatabricksConfig.from_environment()
    
    print(f"Host: {config.host}")
    print(f"Warehouse ID: {config.warehouse_id}")
    print(f"Token: {'*' * 20}...{config.token[-4:] if config.token else 'None'}")
    
    if not config.host or not config.token:
        print("❌ Missing credentials!")
        return False
    
    try:
        # Test connection
        client = SystemTablesClient(config)
        
        if client.test_connection():
            print("✅ Connection successful!")
            
            # Try a simple query
            result = client.query_system_table("SELECT current_timestamp() as test_time")
            if not result.empty:
                print(f"✅ Query test successful! Current time: {result.iloc[0]['test_time']}")
            else:
                print("⚠️ Connection works but query returned no data")
            
            client.close_connections()
            return True
        else:
            print("❌ Connection failed!")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    test_connection()
