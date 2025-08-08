from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
import pandas as pd
import logging
from datetime import datetime, timedelta


class BaseMonitor(ABC):
    """Base class for all monitoring components"""
    
    def __init__(self, system_client, config):
        self.client = system_client
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def get_metrics(self, days: int = 7) -> Dict[str, pd.DataFrame]:
        """Get monitoring metrics for specified time period"""
        pass
    
    @abstractmethod
    def detect_anomalies(self, metrics: Dict[str, pd.DataFrame]) -> Dict[str, List[Dict]]:
        """Detect anomalies in the metrics"""
        pass
    
    def _validate_time_range(self, days: int) -> int:
        """Validate and sanitize time range input"""
        if days < 1:
            self.logger.warning(f"Invalid days value {days}, using default of 1")
            return 1
        elif days > 90:
            self.logger.warning(f"Days value {days} is quite large, using 90")
            return 90
        return days
    
    def _calculate_percentiles(self, data: pd.Series) -> Dict[str, float]:
        """Calculate standard percentiles for a data series"""
        return {
            'p50': data.quantile(0.5),
            'p90': data.quantile(0.9),
            'p95': data.quantile(0.95),
            'p99': data.quantile(0.99)
        }
    
    def _format_duration(self, seconds: float) -> str:
        """Format duration in seconds to human readable string"""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            return f"{seconds/60:.1f}m"
        else:
            return f"{seconds/3600:.1f}h"
    
    def _check_threshold(self, value: float, warning: float, critical: float) -> str:
        """Check if value exceeds thresholds"""
        if value >= critical:
            return "critical"
        elif value >= warning:
            return "warning"
        else:
            return "normal"
    
    def generate_summary(self, metrics: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Generate summary statistics from metrics"""
        summary = {
            'timestamp': datetime.now(),
            'total_records': sum(len(df) for df in metrics.values()),
            'table_counts': {name: len(df) for name, df in metrics.items()}
        }
        return summary