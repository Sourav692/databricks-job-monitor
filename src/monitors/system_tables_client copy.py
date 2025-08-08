from databricks import sql
from databricks.sdk import WorkspaceClient
from typing import Dict, List, Any, Optional
import pandas as pd
from datetime import datetime, timedelta

from config.settings import DatabricksConfig

class SystemTablesClient:
    """Client for accessing Databricks system tables"""
    
    def __init__(self, config: DatabricksConfig):
        self.config = config
        self.workspace_client = None
        self.sql_connection = None
        
        if config.is_local_environment:
            self._setup_local_connection()
        else:
            self._setup_databricks_connection()
    
    def _setup_local_connection(self):
        """Setup connection for local development"""
        self.workspace_client = WorkspaceClient(
            host=self.config.host,
            token=self.config.token
        )
        
        # For system tables, we need SQL connection
        if self.config.warehouse_id:
            self.sql_connection = sql.connect(
                server_hostname=self.config.host.replace('https://', ''),
                http_path=f'/sql/1.0/warehouses/{self.config.warehouse_id}',
                access_token=self.config.token
            )
    
    def _setup_databricks_connection(self):
        """Setup connection for Databricks environment"""
        self.workspace_client = WorkspaceClient()
    
    def query_system_table(self, query: str) -> pd.DataFrame:
        """Execute query against system tables"""
        try:
            if self.config.is_local_environment and self.sql_connection:
                cursor = self.sql_connection.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return pd.DataFrame(results, columns=columns)
            else:
                # Use Spark SQL in Databricks environment
                return spark.sql(query).toPandas()
                
        except Exception as e:
            print(f"Error executing query: {e}")
            return pd.DataFrame()
    
    def get_job_runtime_metrics(self, days: int = 7) -> pd.DataFrame:
        """Get job runtime metrics for specified days"""
        query = f"""
        WITH job_run_duration AS (
            SELECT 
                workspace_id,
                job_id,
                run_id,
                CAST(SUM(period_end_time - period_start_time) AS LONG) as duration_seconds
            FROM system.lakeflow.job_run_timeline
            WHERE period_start_time > CURRENT_TIMESTAMP() - INTERVAL {days} DAYS
            GROUP BY workspace_id, job_id, run_id
        )
        SELECT 
            jrd.workspace_id,
            jrd.job_id,
            j.job_name,
            COUNT(DISTINCT jrd.run_id) as total_runs,
            AVG(jrd.duration_seconds) as avg_duration_seconds,
            MIN(jrd.duration_seconds) as min_duration_seconds,
            MAX(jrd.duration_seconds) as max_duration_seconds,
            PERCENTILE(jrd.duration_seconds, 0.5) as median_duration_seconds,
            PERCENTILE(jrd.duration_seconds, 0.9) as p90_duration_seconds,
            PERCENTILE(jrd.duration_seconds, 0.95) as p95_duration_seconds
        FROM job_run_duration jrd
        LEFT JOIN (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
            FROM system.lakeflow.jobs
        ) j ON jrd.workspace_id = j.workspace_id 
            AND jrd.job_id = j.job_id 
            AND j.rn = 1
        GROUP BY jrd.workspace_id, jrd.job_id, j.job_name
        ORDER BY avg_duration_seconds DESC
        """
        
        return self.query_system_table(query)
    
    def get_cluster_cpu_utilization(self, days: int = 1) -> pd.DataFrame:
        """Get cluster CPU utilization metrics"""
        query = f"""
        SELECT 
            cluster_id,
            driver,
            AVG(cpu_user_percent + cpu_system_percent) as avg_cpu_utilization,
            MAX(cpu_user_percent + cpu_system_percent) as peak_cpu_utilization,
            AVG(cpu_wait_percent) as avg_cpu_wait,
            MAX(cpu_wait_percent) as max_cpu_wait,
            AVG(mem_used_percent) as avg_memory_utilization,
            MAX(mem_used_percent) as max_memory_utilization,
            AVG(network_received_bytes)/(1024*1024) as avg_network_mb_received_per_minute,
            AVG(network_sent_bytes)/(1024*1024) as avg_network_mb_sent_per_minute
        FROM system.compute.node_timeline
        WHERE start_time >= date_add(now(), -{days})
        GROUP BY cluster_id, driver
        ORDER BY avg_cpu_utilization DESC
        """
        
        return self.query_system_table(query)
    
    def get_job_failure_analysis(self, days: int = 7) -> pd.DataFrame:
        """Analyze job failures and success rates"""
        query = f"""
        WITH job_outcomes AS (
            SELECT 
                jrt.workspace_id,
                jrt.job_id,
                jrt.run_id,
                jrt.result_state,
                j.job_name
            FROM system.lakeflow.job_run_timeline jrt
            LEFT JOIN (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
                FROM system.lakeflow.jobs
            ) j ON jrt.workspace_id = j.workspace_id 
                AND jrt.job_id = j.job_id 
                AND j.rn = 1
            WHERE jrt.period_start_time > CURRENT_TIMESTAMP() - INTERVAL {days} DAYS
        )
        SELECT 
            workspace_id,
            job_id,
            job_name,
            COUNT(*) as total_runs,
            SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
            SUM(CASE WHEN result_state IN ('FAILED', 'TIMEOUT', 'CANCELLED') THEN 1 ELSE 0 END) as failed_runs,
            ROUND(SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate_percent,
            ROUND(SUM(CASE WHEN result_state IN ('FAILED', 'TIMEOUT', 'CANCELLED') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as failure_rate_percent
        FROM job_outcomes
        GROUP BY workspace_id, job_id, job_name
        HAVING COUNT(*) > 0
        ORDER BY failure_rate_percent DESC, total_runs DESC
        """
        
        return self.query_system_table(query)
