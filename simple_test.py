import os
import sys
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

print("Starting connection test...")
print(f"Current working directory: {os.getcwd()}")
print(f"Python path: {sys.path[:3]}...")  # Show first 3 entries

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
print(f"Added to path: {project_root}")

try:
    print("Importing config...")
    from config.settings import DatabricksConfig
    print("✅ Config import successful")
    
    print("Loading configuration from environment...")
    config = DatabricksConfig.from_environment()
    print(f"✅ Config loaded - Host: {config.host[:30]}...")
    
    if not config.host or not config.token:
        print("❌ Missing configuration! Check your .env file")
        print("Expected .env contents:")
        print("DATABRICKS_HOST=https://your-workspace.cloud.databricks.com")
        print("DATABRICKS_TOKEN=dapi...")
        print("DATABRICKS_WAREHOUSE_ID=your-warehouse-id")
        sys.exit(1)
    
    print("Importing system tables client...")
    from src.monitors.system_tables_client import SystemTablesClient
    print("✅ SystemTablesClient import successful")
    
    print("Creating client...")
    client = SystemTablesClient(config)
    print("✅ Client created successfully")
    
    print("Testing connection...")
    if client.test_connection():
        print("✅ Connection test successful!")
        
        # Try to get available schemas
        print("Getting available schemas...")
        schemas = client.get_available_schemas()
        if not schemas.empty:
            print("✅ Available system schemas:")
            print(schemas.to_string(index=False))
        else:
            print("⚠️ No schemas found or access denied")
            
        # Try a simple query
        print("Testing simple query...")
        result = client.query_system_table("SELECT current_timestamp() as test_time")
        if not result.empty:
            print(f"✅ Query successful! Current time: {result.iloc[0, 0]}")
        else:
            print("⚠️ Query returned no results")
            
    else:
        print("❌ Connection test failed!")
    
    client.close_connections()
    print("✅ Test completed successfully!")
    
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("\nDebugging information:")
    print(f"Current directory: {os.getcwd()}")
    print(f"Directory contents: {os.listdir('.')}")
    if os.path.exists('config'):
        print(f"Config directory contents: {os.listdir('config')}")
    if os.path.exists('src'):
        print(f"Src directory contents: {os.listdir('src')}")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
