import os
from dataclasses import dataclass
from typing import Optional

# Load environment variables from .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv not installed, continue without it
    pass

@dataclass
class DatabricksConfig:
    """Configuration for Databricks connection and monitoring"""
    host: str
    token: str
    warehouse_id: Optional[str] = None
    cluster_id: Optional[str] = None
    
    @classmethod
    def from_environment(cls):
        """Load configuration from environment variables"""
        return cls(
            host=os.getenv('DATABRICKS_HOST', ''),
            token=os.getenv('DATABRICKS_TOKEN', ''),
            warehouse_id=os.getenv('DATABRICKS_WAREHOUSE_ID'),
            cluster_id=os.getenv('DATABRICKS_CLUSTER_ID')
        )
    
    @property
    def is_local_environment(self) -> bool:
        """Check if running in local environment"""
        return 'DATABRICKS_RUNTIME_VERSION' not in os.environ

@dataclass 
class MonitoringConfig:
    """Configuration for monitoring parameters"""
    refresh_interval_minutes: int = 5
    retention_days: int = 30
    alert_thresholds: dict = None
    
    def __post_init__(self):
        if self.alert_thresholds is None:
            self.alert_thresholds = {
                'cpu_utilization_threshold': 80.0,
                'memory_utilization_threshold': 85.0,
                'job_duration_threshold_minutes': 60,
                'failure_rate_threshold': 0.1
            }
