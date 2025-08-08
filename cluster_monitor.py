import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config.settings import DatabricksConfig
from src.monitors.system_tables_client import SystemTablesClient

def monitor_clusters():
    """Monitor cluster utilization only"""
    
    # Load config
    config = DatabricksConfig.from_environment()
    
    if not config.host or not config.token or not config.warehouse_id:
        print("Missing required configuration. Check your .env file.")
        return
    
    # Initialize client
    client = SystemTablesClient(config)
    
    print("=== CLUSTER UTILIZATION MONITORING ===")
    
    # Get cluster utilization for different time periods
    time_periods = [
        (1, "Last 24 Hours"),
        (3, "Last 3 Days"), 
        (7, "Last 7 Days")
    ]
    
    for days, label in time_periods:
        print(f"\n{label}:")
        print("-" * len(label))
        
        cluster_data = client.get_cluster_cpu_utilization(days=days)
        
        if not cluster_data.empty:
            print(f"Found {len(cluster_data)} clusters with utilization data")
            print(cluster_data.to_string(index=False))
            
            # Show summary statistics
            avg_cpu = cluster_data['avg_cpu_utilization'].mean()
            max_cpu = cluster_data['peak_cpu_utilization'].max()
            avg_memory = cluster_data['avg_memory_utilization'].mean()
            max_memory = cluster_data['peak_memory_utilization'].max()
            
            print(f"\nSummary for {label}:")
            print(f"  Average CPU Utilization: {avg_cpu:.2f}%")
            print(f"  Peak CPU Utilization: {max_cpu:.2f}%")
            print(f"  Average Memory Utilization: {avg_memory:.2f}%") 
            print(f"  Peak Memory Utilization: {max_memory:.2f}%")
        else:
            print("No cluster utilization data found")
    
    client.close_connections()
    print("\n=== CLUSTER MONITORING COMPLETED ===")

if __name__ == "__main__":
    monitor_clusters()
