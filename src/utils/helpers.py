"""
Helper utilities for the monitoring framework
"""
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json

def format_duration(seconds: float) -> str:
    """Format duration in seconds to human readable format"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"

def calculate_percentiles(df: pd.DataFrame, column: str, percentiles: List[float] = [0.5, 0.9, 0.95]) -> Dict[str, float]:
    """Calculate percentiles for a given column"""
    result = {}
    for p in percentiles:
        result[f"p{int(p*100)}"] = df[column].quantile(p)
    return result

def detect_outliers(df: pd.DataFrame, column: str, method: str = 'iqr') -> pd.DataFrame:
    """Detect outliers in a dataset using IQR or Z-score method"""
    if method == 'iqr':
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        return df[(df[column] < lower_bound) | (df[column] > upper_bound)]
    elif method == 'zscore':
        from scipy import stats
        z_scores = np.abs(stats.zscore(df[column]))
        return df[z_scores > 3]
    return pd.DataFrame()

def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide two numbers, returning default if denominator is zero"""
    return numerator / denominator if denominator != 0 else default

def format_bytes(bytes_value: float) -> str:
    """Format bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} PB"

def export_metrics_to_json(metrics: Dict[str, pd.DataFrame], filepath: str):
    """Export metrics to JSON file"""
    json_data = {}
    for key, df in metrics.items():
        if isinstance(df, pd.DataFrame):
            json_data[key] = df.to_dict('records')
        else:
            json_data[key] = df
    
    with open(filepath, 'w') as f:
        json.dump(json_data, f, indent=2, default=str)

def load_metrics_from_json(filepath: str) -> Dict[str, pd.DataFrame]:
    """Load metrics from JSON file"""
    with open(filepath, 'r') as f:
        json_data = json.load(f)
    
    metrics = {}
    for key, data in json_data.items():
        if isinstance(data, list) and len(data) > 0:
            metrics[key] = pd.DataFrame(data)
        else:
            metrics[key] = data
    
    return metrics

def create_date_range_filter(days: int) -> str:
    """Create SQL date range filter for the last N days"""
    return f"period_start_time > CURRENT_TIMESTAMP() - INTERVAL {days} DAYS"

class MetricsCache:
    """Simple in-memory cache for metrics"""
    
    def __init__(self, ttl_minutes: int = 30):
        self.cache = {}
        self.ttl_minutes = ttl_minutes
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached value if not expired"""
        if key in self.cache:
            data, timestamp = self.cache[key]
            if datetime.now() - timestamp < timedelta(minutes=self.ttl_minutes):
                return data
            else:
                del self.cache[key]
        return None
    
    def set(self, key: str, value: Any):
        """Set cached value with timestamp"""
        self.cache[key] = (value, datetime.now())
    
    def clear(self):
        """Clear all cached values"""
        self.cache.clear()