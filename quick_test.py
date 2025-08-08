import os
from dotenv import load_dotenv
from databricks import sql
import pandas as pd

# Load environment variables
load_dotenv()

def quick_test():
    host = os.getenv('DATABRICKS_HOST', '').replace('https://', '').replace('http://', '')
    token = os.getenv('DATABRICKS_TOKEN', '')
    warehouse_id = os.getenv('DATABRICKS_WAREHOUSE_ID', '')
    
    print(f"Host: {host}")
    print(f"Token: {'*' * 20}...{token[-4:] if token else 'None'}")
    print(f"Warehouse: {warehouse_id}")
    
    if not all([host, token, warehouse_id]):
        print("❌ Missing required environment variables")
        return False
    
    try:
        print("Connecting to Databricks...")
        connection = sql.connect(
            server_hostname=host,
            http_path=f'/sql/1.0/warehouses/{warehouse_id}',
            access_token=token
        )
        
        print("Testing query...")
        cursor = connection.cursor()
        cursor.execute("SELECT current_timestamp() as test_time")
        result = cursor.fetchone()
        
        print(f"✅ Success! Current time: {result[0]}")
        
        # Test system tables access
        cursor.execute("SHOW SCHEMAS IN system")
        schemas = cursor.fetchall()
        print(f"✅ Found {len(schemas)} system schemas")
        for schema in schemas:
            print(f"  - {schema[0]}")
        
        cursor.close()
        connection.close()
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    quick_test()