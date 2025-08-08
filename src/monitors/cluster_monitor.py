from typing import Dict, List, Any
import pandas as pd
from datetime import datetime
from .base_monitor import BaseMonitor


class ClusterMonitor(BaseMonitor):
    """Monitor for tracking Databricks cluster performance and utilization"""
    
    def get_metrics(self, days: int = 7) -> Dict[str, pd.DataFrame]:
        """Get comprehensive cluster monitoring metrics"""
        days = self._validate_time_range(days)
        metrics = {}
        
        try:
            # Cluster utilization metrics
            self.logger.info(f"Fetching cluster utilization for last {days} days...")
            metrics['cluster_utilization'] = self._get_cluster_utilization(days)
            
            # Node type analysis
            self.logger.info("Fetching node type information...")
            metrics['node_types'] = self._get_node_types()
            
            # Cluster efficiency metrics
            self.logger.info("Calculating cluster efficiency metrics...")
            metrics['efficiency_metrics'] = self._calculate_efficiency_metrics(days)
            
            # Cost analysis
            self.logger.info("Analyzing cluster costs...")
            metrics['cost_analysis'] = self._get_cluster_costs(days)
            
            return metrics
        except Exception as e:
            self.logger.error(f"Error fetching cluster metrics: {e}")
            return {}
    
    def _get_cluster_utilization(self, days: int) -> pd.DataFrame:
        """Get detailed cluster utilization metrics"""
        query = f"""
        SELECT 
            nt.cluster_id,
            c.cluster_name,
            c.driver_node_type_id,
            c.node_type_id,
            nt.driver,
            nt.instance_id,
            AVG(nt.cpu_user_percent + nt.cpu_system_percent) as avg_cpu_utilization,
            MAX(nt.cpu_user_percent + nt.cpu_system_percent) as peak_cpu_utilization,
            MIN(nt.cpu_user_percent + nt.cpu_system_percent) as min_cpu_utilization,
            AVG(nt.cpu_wait_percent) as avg_cpu_wait,
            MAX(nt.cpu_wait_percent) as max_cpu_wait,
            AVG(nt.mem_used_percent) as avg_memory_utilization,
            MAX(nt.mem_used_percent) as peak_memory_utilization,
            MIN(nt.mem_used_percent) as min_memory_utilization,
            AVG(nt.network_received_bytes)/(1024*1024) as avg_network_mb_received_per_minute,
            AVG(nt.network_sent_bytes)/(1024*1024) as avg_network_mb_sent_per_minute,
            COUNT(*) as measurement_count,
            MIN(nt.start_time) as monitoring_start,
            MAX(nt.end_time) as monitoring_end
        FROM system.compute.node_timeline nt
        LEFT JOIN (
            SELECT *, 
                   ROW_NUMBER() OVER(PARTITION BY cluster_id ORDER BY change_time DESC) as rn
            FROM system.compute.clusters
        ) c ON nt.cluster_id = c.cluster_id AND c.rn = 1
        WHERE nt.start_time >= date_add(now(), -{days})
        GROUP BY nt.cluster_id, c.cluster_name, c.driver_node_type_id, c.node_type_id, nt.driver, nt.instance_id
        ORDER BY avg_cpu_utilization DESC
        """
        return self.client.query_system_table(query)
    
    def _get_node_types(self) -> pd.DataFrame:
        """Get available node types and their specifications"""
        query = """
        SELECT 
            node_type_id,
            memory_mb,
            num_cores,
            num_gpus,
            instance_type_id,
            is_io_cache_enabled,
            category
        FROM system.compute.node_types
        ORDER BY num_cores, memory_mb
        """
        return self.client.query_system_table(query)
    
    def _calculate_efficiency_metrics(self, days: int) -> pd.DataFrame:
        """Calculate cluster efficiency metrics"""
        query = f"""
        WITH cluster_stats AS (
            SELECT 
                cluster_id,
                AVG(cpu_user_percent + cpu_system_percent) as avg_cpu_utilization,
                AVG(mem_used_percent) as avg_memory_utilization,
                COUNT(*) as total_measurements,
                COUNT(CASE WHEN (cpu_user_percent + cpu_system_percent) < 10 THEN 1 END) as low_cpu_count,
                COUNT(CASE WHEN mem_used_percent < 20 THEN 1 END) as low_memory_count
            FROM system.compute.node_timeline
            WHERE start_time >= date_add(now(), -{days})
            GROUP BY cluster_id
        )
        SELECT 
            cs.*,
            ROUND((low_cpu_count * 100.0 / total_measurements), 2) as low_cpu_percent,
            ROUND((low_memory_count * 100.0 / total_measurements), 2) as low_memory_percent,
            CASE 
                WHEN avg_cpu_utilization < 20 AND avg_memory_utilization < 30 THEN 'Underutilized'
                WHEN avg_cpu_utilization > 80 OR avg_memory_utilization > 85 THEN 'High Utilization'
                ELSE 'Normal'
            END as efficiency_category
        FROM cluster_stats cs
        ORDER BY avg_cpu_utilization DESC
        """
        return self.client.query_system_table(query)
    
    def _get_cluster_costs(self, days: int) -> pd.DataFrame:
        """Analyze cluster costs from billing data"""
        query = f"""
        WITH cluster_usage AS (
            SELECT 
                usage_metadata['cluster_id'] as cluster_id,
                sku_name,
                usage_unit,
                SUM(usage_quantity) as total_usage,
                COUNT(*) as usage_records
            FROM system.billing.usage
            WHERE usage_date >= date_add(current_date(), -{days})
                AND usage_metadata['cluster_id'] IS NOT NULL
            GROUP BY usage_metadata['cluster_id'], sku_name, usage_unit
        )
        SELECT 
            cu.cluster_id,
            c.cluster_name,
            cu.sku_name,
            cu.usage_unit,
            cu.total_usage,
            cu.usage_records,
            c.driver_node_type_id,
            c.node_type_id,
            c.num_workers
        FROM cluster_usage cu
        LEFT JOIN (
            SELECT *, 
                   ROW_NUMBER() OVER(PARTITION BY cluster_id ORDER BY change_time DESC) as rn
            FROM system.compute.clusters
        ) c ON cu.cluster_id = c.cluster_id AND c.rn = 1
        ORDER BY cu.total_usage DESC
        """
        return self.client.query_system_table(query)
    
    def detect_anomalies(self, metrics: Dict[str, pd.DataFrame]) -> Dict[str, List[Dict]]:
        """Detect cluster performance anomalies"""
        anomalies = {
            'underutilized_clusters': [],
            'overutilized_clusters': [],
            'expensive_clusters': [],
            'inefficient_clusters': []
        }
        
        if 'cluster_utilization' in metrics and not metrics['cluster_utilization'].empty:
            utilization_df = metrics['cluster_utilization']
            
            # Detect underutilized clusters
            underutilized = utilization_df[
                (utilization_df['avg_cpu_utilization'] < 20) & 
                (utilization_df['avg_memory_utilization'] < 30)
            ]
            
            for _, cluster in underutilized.iterrows():
                anomalies['underutilized_clusters'].append({
                    'cluster_id': cluster['cluster_id'],
                    'cluster_name': cluster.get('cluster_name', 'Unknown'),
                    'avg_cpu_utilization': round(cluster['avg_cpu_utilization'], 2),
                    'avg_memory_utilization': round(cluster['avg_memory_utilization'], 2)
                })
            
            # Detect overutilized clusters
            overutilized = utilization_df[
                (utilization_df['avg_cpu_utilization'] > 85) | 
                (utilization_df['avg_memory_utilization'] > 90)
            ]
            
            for _, cluster in overutilized.iterrows():
                anomalies['overutilized_clusters'].append({
                    'cluster_id': cluster['cluster_id'],
                    'cluster_name': cluster.get('cluster_name', 'Unknown'),
                    'avg_cpu_utilization': round(cluster['avg_cpu_utilization'], 2),
                    'avg_memory_utilization': round(cluster['avg_memory_utilization'], 2)
                })
        
        if 'efficiency_metrics' in metrics and not metrics['efficiency_metrics'].empty:
            efficiency_df = metrics['efficiency_metrics']
            
            # Detect inefficient clusters
            inefficient = efficiency_df[efficiency_df['efficiency_category'] == 'Underutilized']
            
            for _, cluster in inefficient.iterrows():
                anomalies['inefficient_clusters'].append({
                    'cluster_id': cluster['cluster_id'],
                    'avg_cpu_utilization': round(cluster['avg_cpu_utilization'], 2),
                    'low_cpu_percent': cluster['low_cpu_percent'],
                    'low_memory_percent': cluster['low_memory_percent']
                })
        
        return anomalies
    
    def generate_cluster_report(self, days: int = 7) -> str:
        """Generate comprehensive cluster monitoring report"""
        metrics = self.get_metrics(days)
        anomalies = self.detect_anomalies(metrics)
        
        report = f"""
# Cluster Monitoring Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Period: Last {days} days

## Cluster Summary
"""
        
        if 'cluster_utilization' in metrics and not metrics['cluster_utilization'].empty:
            cluster_count = metrics['cluster_utilization']['cluster_id'].nunique()
            avg_cpu = metrics['cluster_utilization']['avg_cpu_utilization'].mean()
            avg_memory = metrics['cluster_utilization']['avg_memory_utilization'].mean()
            
            report += f"- Total Clusters Monitored: {cluster_count}\n"
            report += f"- Average CPU Utilization: {avg_cpu:.1f}%\n"
            report += f"- Average Memory Utilization: {avg_memory:.1f}%\n"
        
        # Add anomaly details
        report += "\n## Detected Issues\n"
        
        if anomalies['underutilized_clusters']:
            report += f"\n### Underutilized Clusters ({len(anomalies['underutilized_clusters'])} found)\n"
            for cluster in anomalies['underutilized_clusters'][:5]:
                report += f"- **{cluster['cluster_name']}**: CPU {cluster['avg_cpu_utilization']}%, Memory {cluster['avg_memory_utilization']}%\n"
        
        if anomalies['overutilized_clusters']:
            report += f"\n### Overutilized Clusters ({len(anomalies['overutilized_clusters'])} found)\n"
            for cluster in anomalies['overutilized_clusters'][:5]:
                report += f"- **{cluster['cluster_name']}**: CPU {cluster['avg_cpu_utilization']}%, Memory {cluster['avg_memory_utilization']}%\n"
        
        return report