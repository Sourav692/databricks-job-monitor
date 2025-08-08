import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


class MetricsCollector:
    """Collects and caches metrics from multiple monitors"""
    
    def __init__(self, job_monitor, cluster_monitor, refresh_interval: int = 300):
        self.job_monitor = job_monitor
        self.cluster_monitor = cluster_monitor
        self.refresh_interval = refresh_interval
        self.logger = logging.getLogger(__name__)
        
        # Cache for metrics
        self._cache = {}
        self._cache_timestamps = {}
        self._cache_lock = threading.Lock()
    
    def get_all_metrics(self, days: int = 7, use_cache: bool = True) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Get all metrics from job and cluster monitors"""
        cache_key = f"all_metrics_{days}"
        
        if use_cache and self._is_cache_valid(cache_key):
            self.logger.info("Returning cached metrics")
            return self._cache[cache_key]
        
        self.logger.info(f"Collecting fresh metrics for {days} days")
        
        metrics = {
            'job_metrics': {},
            'cluster_metrics': {},
            'collection_time': datetime.now(),
            'days_analyzed': days
        }
        
        # Collect metrics in parallel for better performance
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit both monitoring tasks
            job_future = executor.submit(self.job_monitor.get_comprehensive_job_metrics, days)
            cluster_future = executor.submit(self.cluster_monitor.get_metrics, days)
            
            # Collect results
            try:
                metrics['job_metrics'] = job_future.result(timeout=300)
                self.logger.info("Job metrics collected successfully")
            except Exception as e:
                self.logger.error(f"Error collecting job metrics: {e}")
                metrics['job_metrics'] = {}
            
            try:
                metrics['cluster_metrics'] = cluster_future.result(timeout=300)
                self.logger.info("Cluster metrics collected successfully")
            except Exception as e:
                self.logger.error(f"Error collecting cluster metrics: {e}")
                metrics['cluster_metrics'] = {}
        
        # Cache the results
        if use_cache:
            with self._cache_lock:
                self._cache[cache_key] = metrics
                self._cache_timestamps[cache_key] = datetime.now()
        
        return metrics
    
    def get_summary_statistics(self, days: int = 7) -> Dict[str, Any]:
        """Get high-level summary statistics"""
        metrics = self.get_all_metrics(days)
        
        summary = {
            'collection_time': metrics['collection_time'],
            'days_analyzed': days,
            'job_stats': {},
            'cluster_stats': {},
            'overall_health': 'unknown'
        }
        
        # Job statistics
        job_metrics = metrics.get('job_metrics', {})
        if 'runtime_metrics' in job_metrics and not job_metrics['runtime_metrics'].empty:
            runtime_df = job_metrics['runtime_metrics']
            summary['job_stats'] = {
                'total_jobs': len(runtime_df),
                'avg_runtime_minutes': runtime_df['avg_duration_seconds'].mean() / 60,
                'total_runs': runtime_df['total_runs'].sum()
            }
        
        if 'failure_analysis' in job_metrics and not job_metrics['failure_analysis'].empty:
            failure_df = job_metrics['failure_analysis']
            summary['job_stats'].update({
                'avg_success_rate': failure_df['success_rate_percent'].mean(),
                'total_failures': failure_df['failed_runs'].sum()
            })
        
        # Cluster statistics
        cluster_metrics = metrics.get('cluster_metrics', {})
        if 'cluster_utilization' in cluster_metrics and not cluster_metrics['cluster_utilization'].empty:
            cluster_df = cluster_metrics['cluster_utilization']
            summary['cluster_stats'] = {
                'total_clusters': cluster_df['cluster_id'].nunique(),
                'avg_cpu_utilization': cluster_df['avg_cpu_utilization'].mean(),
                'avg_memory_utilization': cluster_df['avg_memory_utilization'].mean()
            }
        
        # Overall health assessment
        summary['overall_health'] = self._assess_overall_health(summary)
        
        return summary
    
    def get_alerts(self, days: int = 7) -> Dict[str, List[Dict]]:
        """Get alerts and anomalies from all monitors"""
        metrics = self.get_all_metrics(days)
        all_alerts = {
            'critical': [],
            'warning': [],
            'info': []
        }
        
        # Get job-related alerts
        if metrics.get('job_metrics'):
            job_anomalies = self.job_monitor.detect_anomalies(metrics['job_metrics'])
            
            # Convert job anomalies to alerts
            for anomaly_type, anomalies in job_anomalies.items():
                for anomaly in anomalies:
                    alert = {
                        'type': 'job',
                        'category': anomaly_type,
                        'timestamp': datetime.now(),
                        'details': anomaly
                    }
                    
                    # Classify alert severity
                    if anomaly_type in ['high_failure_rates']:
                        all_alerts['critical'].append(alert)
                    elif anomaly_type in ['long_running_jobs', 'resource_intensive_jobs']:
                        all_alerts['warning'].append(alert)
                    else:
                        all_alerts['info'].append(alert)
        
        # Get cluster-related alerts
        if metrics.get('cluster_metrics'):
            cluster_anomalies = self.cluster_monitor.detect_anomalies(metrics['cluster_metrics'])
            
            for anomaly_type, anomalies in cluster_anomalies.items():
                for anomaly in anomalies:
                    alert = {
                        'type': 'cluster',
                        'category': anomaly_type,
                        'timestamp': datetime.now(),
                        'details': anomaly
                    }
                    
                    if anomaly_type in ['overutilized_clusters']:
                        all_alerts['critical'].append(alert)
                    elif anomaly_type in ['underutilized_clusters', 'expensive_clusters']:
                        all_alerts['warning'].append(alert)
                    else:
                        all_alerts['info'].append(alert)
        
        return all_alerts
    
    def get_trending_data(self, days: int = 7) -> Dict[str, pd.DataFrame]:
        """Get trending data for dashboard charts"""
        metrics = self.get_all_metrics(days)
        trending_data = {}
        
        # Job runtime trends
        if 'job_metrics' in metrics and 'job_frequency' in metrics['job_metrics']:
            freq_df = metrics['job_metrics']['job_frequency']
            if not freq_df.empty:
                trending_data['job_frequency_trend'] = (
                    freq_df.groupby('execution_date')['runs_per_day']
                    .sum().reset_index()
                    .sort_values('execution_date')
                )
        
        # Resource utilization trends
        if 'cluster_metrics' in metrics and 'cluster_utilization' in metrics['cluster_metrics']:
            cluster_df = metrics['cluster_metrics']['cluster_utilization']
            if not cluster_df.empty:
                trending_data['resource_utilization_trend'] = (
                    cluster_df.groupby('cluster_id')[['avg_cpu_utilization', 'avg_memory_utilization']]
                    .mean().reset_index()
                )
        
        return trending_data
    
    def export_metrics_to_csv(self, days: int = 7, output_dir: str = "./exports") -> Dict[str, str]:
        """Export all metrics to CSV files"""
        import os
        from pathlib import Path
        
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        metrics = self.get_all_metrics(days)
        exported_files = {}
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Export job metrics
        job_metrics = metrics.get('job_metrics', {})
        for metric_name, df in job_metrics.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                filename = f"job_{metric_name}_{timestamp}.csv"
                filepath = os.path.join(output_dir, filename)
                df.to_csv(filepath, index=False)
                exported_files[f"job_{metric_name}"] = filepath
        
        # Export cluster metrics
        cluster_metrics = metrics.get('cluster_metrics', {})
        for metric_name, df in cluster_metrics.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                filename = f"cluster_{metric_name}_{timestamp}.csv"
                filepath = os.path.join(output_dir, filename)
                df.to_csv(filepath, index=False)
                exported_files[f"cluster_{metric_name}"] = filepath
        
        # Export summary
        summary = self.get_summary_statistics(days)
        summary_filename = f"monitoring_summary_{timestamp}.json"
        summary_filepath = os.path.join(output_dir, summary_filename)
        
        import json
        with open(summary_filepath, 'w') as f:
            # Convert datetime objects to strings for JSON serialization
            json_summary = {k: str(v) if isinstance(v, datetime) else v for k, v in summary.items()}
            json.dump(json_summary, f, indent=2, default=str)
        
        exported_files['summary'] = summary_filepath
        
        self.logger.info(f"Exported {len(exported_files)} files to {output_dir}")
        return exported_files
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached data is still valid"""
        if cache_key not in self._cache_timestamps:
            return False
        
        cache_age = datetime.now() - self._cache_timestamps[cache_key]
        return cache_age.total_seconds() < self.refresh_interval
    
    def _assess_overall_health(self, summary: Dict[str, Any]) -> str:
        """Assess overall health based on summary statistics"""
        health_score = 0
        max_score = 0
        
        # Job health assessment
        job_stats = summary.get('job_stats', {})
        if 'avg_success_rate' in job_stats:
            max_score += 1
            if job_stats['avg_success_rate'] > 95:
                health_score += 1
            elif job_stats['avg_success_rate'] > 85:
                health_score += 0.5
        
        # Cluster health assessment
        cluster_stats = summary.get('cluster_stats', {})
        if 'avg_cpu_utilization' in cluster_stats:
            max_score += 1
            cpu_util = cluster_stats['avg_cpu_utilization']
            if 20 <= cpu_util <= 80:  # Optimal range
                health_score += 1
            elif 10 <= cpu_util <= 90:  # Acceptable range
                health_score += 0.5
        
        if max_score == 0:
            return 'unknown'
        
        health_ratio = health_score / max_score
        
        if health_ratio >= 0.8:
            return 'excellent'
        elif health_ratio >= 0.6:
            return 'good'
        elif health_ratio >= 0.4:
            return 'fair'
        else:
            return 'poor'
    
    def clear_cache(self):
        """Clear all cached metrics"""
        with self._cache_lock:
            self._cache.clear()
            self._cache_timestamps.clear()
        self.logger.info("Metrics cache cleared")