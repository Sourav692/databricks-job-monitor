from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime, timedelta
import logging
import os
import sys

# Add project root to Python path
project_root = os.path.join(os.path.dirname(__file__), '..', '..')
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.settings import MonitoringConfig
from src.monitors.system_tables_client import SystemTablesClient

class JobMonitor:
    """Monitor for tracking Databricks job performance and metrics"""
    
    def __init__(self, system_client: SystemTablesClient, config: MonitoringConfig):
        self.client = system_client
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def get_comprehensive_job_metrics(self, days: int = 7) -> Dict[str, pd.DataFrame]:
        """Get comprehensive job monitoring metrics"""
        metrics = {}
        
        try:
            # Runtime metrics
            self.logger.info(f"Fetching job runtime metrics for last {days} days...")
            metrics['runtime_metrics'] = self.client.get_job_runtime_metrics(days)
            
            # Failure analysis
            self.logger.info("Analyzing job failures...")
            metrics['failure_analysis'] = self.client.get_job_failure_analysis(days)
            
            # Cluster utilization
            self.logger.info("Fetching cluster utilization metrics...")
            metrics['cluster_utilization'] = self.client.get_cluster_cpu_utilization(days)
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error fetching job metrics: {e}")
            return {}
    
    def detect_anomalies(self, metrics: Dict[str, pd.DataFrame]) -> Dict[str, List[Dict]]:
        """Detect performance anomalies in job metrics"""
        anomalies = {
            'long_running_jobs': [],
            'high_failure_rates': [],
            'resource_intensive_jobs': [],
            'irregular_schedules': []
        }
        
        if 'runtime_metrics' in metrics and not metrics['runtime_metrics'].empty:
            # Detect long-running jobs
            threshold_minutes = self.config.alert_thresholds['job_duration_threshold_minutes']
            long_jobs = metrics['runtime_metrics'][
                metrics['runtime_metrics']['avg_duration_seconds'] > threshold_minutes * 60
            ]
            
            for _, job in long_jobs.iterrows():
                anomalies['long_running_jobs'].append({
                    'job_id': job['job_id'],
                    'job_name': job.get('job_name', 'Unknown'),
                    'avg_duration_minutes': round(job['avg_duration_seconds'] / 60, 2),
                    'max_duration_minutes': round(job['max_duration_seconds'] / 60, 2)
                })
        
        if 'failure_analysis' in metrics and not metrics['failure_analysis'].empty:
            # Detect high failure rates
            failure_threshold = self.config.alert_thresholds['failure_rate_threshold']
            high_failure_jobs = metrics['failure_analysis'][
                metrics['failure_analysis']['failure_rate_percent'] > failure_threshold * 100
            ]
            
            for _, job in high_failure_jobs.iterrows():
                anomalies['high_failure_rates'].append({
                    'job_id': job['job_id'],
                    'job_name': job.get('job_name', 'Unknown'),
                    'failure_rate_percent': job['failure_rate_percent'],
                    'failed_runs': job['failed_runs'],
                    'total_runs': job['total_runs']
                })
        
        return anomalies
    
    def generate_monitoring_report(self, days: int = 7) -> str:
        """Generate a comprehensive monitoring report"""
        metrics = self.get_comprehensive_job_metrics(days)
        anomalies = self.detect_anomalies(metrics)
        
        report = f"""
# Databricks Job Monitoring Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Period: Last {days} days

## Summary Statistics
"""
        
        if 'runtime_metrics' in metrics and not metrics['runtime_metrics'].empty:
            total_jobs = len(metrics['runtime_metrics'])
            avg_runtime = metrics['runtime_metrics']['avg_duration_seconds'].mean() / 60
            report += f"- Total Jobs Monitored: {total_jobs}\n"
            report += f"- Average Job Runtime: {avg_runtime:.2f} minutes\n"
        
        if 'failure_analysis' in metrics and not metrics['failure_analysis'].empty:
            avg_success_rate = metrics['failure_analysis']['success_rate_percent'].mean()
            total_runs = metrics['failure_analysis']['total_runs'].sum()
            total_failures = metrics['failure_analysis']['failed_runs'].sum()
            report += f"- Average Success Rate: {avg_success_rate:.2f}%\n"
            report += f"- Total Job Runs: {total_runs}\n"
            report += f"- Total Failures: {total_failures}\n"
        
        if 'cluster_utilization' in metrics and not metrics['cluster_utilization'].empty:
            avg_cpu = metrics['cluster_utilization']['avg_cpu_utilization'].mean()
            avg_memory = metrics['cluster_utilization']['avg_memory_utilization'].mean()
            report += f"- Average CPU Utilization: {avg_cpu:.2f}%\n"
            report += f"- Average Memory Utilization: {avg_memory:.2f}%\n"
        
        # Add anomaly details
        report += "\n## Detected Anomalies\n"
        
        if anomalies['long_running_jobs']:
            report += f"\n### Long Running Jobs ({len(anomalies['long_running_jobs'])} detected)\n"
            for job in anomalies['long_running_jobs'][:5]:  # Top 5
                report += f"- **{job['job_name']}** (ID: {job['job_id']}): Avg {job['avg_duration_minutes']} min\n"
        
        if anomalies['high_failure_rates']:
            report += f"\n### High Failure Rate Jobs ({len(anomalies['high_failure_rates'])} detected)\n"
            for job in anomalies['high_failure_rates'][:5]:  # Top 5
                report += f"- **{job['job_name']}** (ID: {job['job_id']}): {job['failure_rate_percent']}% failure rate\n"
        
        # Add detailed metrics tables
        if 'runtime_metrics' in metrics and not metrics['runtime_metrics'].empty:
            report += "\n## Job Runtime Details\n"
            report += metrics['runtime_metrics'].to_string(index=False)
            report += "\n"
        
        if 'failure_analysis' in metrics and not metrics['failure_analysis'].empty:
            report += "\n## Job Failure Analysis\n"
            report += metrics['failure_analysis'].to_string(index=False)
            report += "\n"
        
        if 'cluster_utilization' in metrics and not metrics['cluster_utilization'].empty:
            report += "\n## Cluster Utilization\n"
            report += metrics['cluster_utilization'].to_string(index=False)
            report += "\n"
        
        return report
