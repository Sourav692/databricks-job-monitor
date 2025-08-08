from databricks import sql
from databricks.sdk import WorkspaceClient
from typing import Dict, List, Any, Optional
import pandas as pd
import logging
import os
import sys

# Add project root to Python path for imports
project_root = os.path.join(os.path.dirname(__file__), '..', '..')
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.settings import DatabricksConfig

class SystemTablesClient:
    """Client for accessing Databricks system tables"""
    
    def __init__(self, config: DatabricksConfig):
        self.config = config
        self.workspace_client = None
        self.sql_connection = None
        self.logger = logging.getLogger(__name__)
        
        self._setup_connections()
    
    def _setup_connections(self):
        """Setup connections for local development"""
        try:
            # Setup workspace client
            self.workspace_client = WorkspaceClient(
                host=self.config.host,
                token=self.config.token
            )
            
            # Setup SQL connection for system tables
            if self.config.warehouse_id:
                hostname = self.config.host.replace('https://', '').replace('http://', '')
                self.sql_connection = sql.connect(
                    server_hostname=hostname,
                    http_path=f'/sql/1.0/warehouses/{self.config.warehouse_id}',
                    access_token=self.config.token,
                    _tls_no_verify=True,
                    _tls_verify_hostname=False
                )
                self.logger.info("SQL connection established successfully")
            else:
                self.logger.warning("No warehouse ID provided. System table queries will not work.")
                
        except Exception as e:
            self.logger.error(f"Failed to setup connections: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            if not self.sql_connection:
                return False
                
            cursor = self.sql_connection.cursor()
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchone()
            cursor.close()
            return result[0] == 1
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def query_system_table(self, query: str) -> pd.DataFrame:
        """Execute query against system tables"""
        if not self.sql_connection:
            self.logger.error("No SQL connection available")
            return pd.DataFrame()
        
        try:
            self.logger.info(f"Executing query: {query[:100]}...")
            cursor = self.sql_connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            cursor.close()
            
            df = pd.DataFrame(results, columns=columns)
            self.logger.info(f"Query returned {len(df)} rows")
            return df
            
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            return pd.DataFrame()
    
    def get_available_tables(self):
        """Get available system tables to debug schema"""
        queries = [
            "SHOW TABLES IN system.lakeflow",
            "SHOW TABLES IN system.compute"
        ]
        
        results = {}
        for query in queries:
            try:
                df = self.query_system_table(query)
                schema = query.split("IN ")[1]
                results[schema] = df
            except Exception as e:
                self.logger.error(f"Failed to get tables for {query}: {e}")
        
        return results
    
    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """Get schema of a specific system table"""
        query = f"DESCRIBE {table_name}"
        return self.query_system_table(query)
    
    def get_job_runtime_metrics(self, days: int = 7) -> pd.DataFrame:
        """Get job runtime metrics for specified days - Fixed column names"""
        query = """
        WITH job_run_duration AS (
            SELECT 
                workspace_id,
                job_id,
                run_id,
                CAST(
                    UNIX_TIMESTAMP(MAX(period_end_time)) - UNIX_TIMESTAMP(MIN(period_start_time))
                    AS LONG
                ) as duration_seconds
            FROM system.lakeflow.job_run_timeline
            WHERE period_start_time >= date_sub(current_timestamp(), {})
            GROUP BY workspace_id, job_id, run_id
        ),
        job_metadata AS (
            SELECT DISTINCT
                workspace_id,
                job_id,
                name as job_name,
                ROW_NUMBER() OVER(PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
            FROM system.lakeflow.jobs
        )
        SELECT 
            jrd.workspace_id,
            jrd.job_id,
            COALESCE(jm.job_name, CONCAT('Job_', jrd.job_id)) as job_name,
            COUNT(DISTINCT jrd.run_id) as total_runs,
            ROUND(AVG(jrd.duration_seconds), 2) as avg_duration_seconds,
            MIN(jrd.duration_seconds) as min_duration_seconds,
            MAX(jrd.duration_seconds) as max_duration_seconds,
            ROUND(PERCENTILE_APPROX(jrd.duration_seconds, 0.5), 2) as median_duration_seconds,
            ROUND(PERCENTILE_APPROX(jrd.duration_seconds, 0.9), 2) as p90_duration_seconds,
            ROUND(PERCENTILE_APPROX(jrd.duration_seconds, 0.95), 2) as p95_duration_seconds
        FROM job_run_duration jrd
        LEFT JOIN job_metadata jm ON jrd.workspace_id = jm.workspace_id 
            AND jrd.job_id = jm.job_id 
            AND jm.rn = 1
        GROUP BY jrd.workspace_id, jrd.job_id, jm.job_name
        HAVING COUNT(DISTINCT jrd.run_id) > 0
        ORDER BY avg_duration_seconds DESC
        """.format(days)
        
        return self.query_system_table(query)
    
    def get_job_failure_analysis(self, days: int = 7) -> pd.DataFrame:
        """Analyze job failures and success rates - Fixed column names"""
        query = """
        WITH job_runs AS (
            SELECT DISTINCT
                jrt.workspace_id,
                jrt.job_id,
                jrt.run_id,
                jrt.result_state
            FROM system.lakeflow.job_run_timeline jrt
            WHERE jrt.period_start_time >= date_sub(current_timestamp(), {})
                AND jrt.result_state IS NOT NULL
        ),
        job_metadata AS (
            SELECT DISTINCT
                workspace_id,
                job_id,
                name as job_name,
                ROW_NUMBER() OVER(PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
            FROM system.lakeflow.jobs
        )
        SELECT 
            jr.workspace_id,
            jr.job_id,
            COALESCE(jm.job_name, CONCAT('Job_', jr.job_id)) as job_name,
            COUNT(DISTINCT jr.run_id) as total_runs,
            COUNT(DISTINCT CASE WHEN jr.result_state = 'SUCCESS' THEN jr.run_id END) as successful_runs,
            COUNT(DISTINCT CASE WHEN jr.result_state IN ('FAILED', 'TIMEOUT', 'CANCELLED') THEN jr.run_id END) as failed_runs,
            ROUND(
                COUNT(DISTINCT CASE WHEN jr.result_state = 'SUCCESS' THEN jr.run_id END) * 100.0 / 
                COUNT(DISTINCT jr.run_id), 2
            ) as success_rate_percent,
            ROUND(
                COUNT(DISTINCT CASE WHEN jr.result_state IN ('FAILED', 'TIMEOUT', 'CANCELLED') THEN jr.run_id END) * 100.0 / 
                COUNT(DISTINCT jr.run_id), 2
            ) as failure_rate_percent
        FROM job_runs jr
        LEFT JOIN job_metadata jm ON jr.workspace_id = jm.workspace_id 
            AND jr.job_id = jm.job_id 
            AND jm.rn = 1
        GROUP BY jr.workspace_id, jr.job_id, jm.job_name
        HAVING COUNT(DISTINCT jr.run_id) > 0
        ORDER BY failure_rate_percent DESC, total_runs DESC
        """.format(days)
        
        return self.query_system_table(query)
    
    def get_cluster_cpu_utilization(self, days: int = 1) -> pd.DataFrame:
        """Get cluster CPU utilization metrics"""
        query = """
        SELECT 
            cluster_id,
            driver,
            COUNT(*) as data_points,
            ROUND(AVG(cpu_user_percent + cpu_system_percent), 2) as avg_cpu_utilization,
            ROUND(MAX(cpu_user_percent + cpu_system_percent), 2) as peak_cpu_utilization,
            ROUND(AVG(cpu_wait_percent), 2) as avg_cpu_wait,
            ROUND(MAX(cpu_wait_percent), 2) as max_cpu_wait,
            ROUND(AVG(mem_used_percent), 2) as avg_memory_utilization,
            ROUND(MAX(mem_used_percent), 2) as max_memory_utilization,
            ROUND(AVG(network_received_bytes)/(1024*1024), 2) as avg_network_mb_received_per_minute,
            ROUND(AVG(network_sent_bytes)/(1024*1024), 2) as avg_network_mb_sent_per_minute
        FROM system.compute.node_timeline
        WHERE start_time >= date_sub(current_timestamp(), {})
        GROUP BY cluster_id, driver
        HAVING COUNT(*) > 5
        ORDER BY avg_cpu_utilization DESC
        LIMIT 20
        """.format(days)
        
        return self.query_system_table(query)
    
    def get_simple_job_data(self, days: int = 7) -> pd.DataFrame:
        """Get simple job data without complex joins"""
        query = """
        SELECT 
            workspace_id,
            job_id,
            COUNT(DISTINCT run_id) as total_runs,
            COUNT(DISTINCT DATE(period_start_time)) as active_days,
            MIN(period_start_time) as first_run,
            MAX(period_end_time) as last_run
        FROM system.lakeflow.job_run_timeline
        WHERE period_start_time >= date_sub(current_timestamp(), {})
        GROUP BY workspace_id, job_id
        ORDER BY total_runs DESC
        LIMIT 20
        """.format(days)
        
        return self.query_system_table(query)
    
    def get_recent_job_activity(self, days: int = 3) -> pd.DataFrame:
        """Get recent job activity summary"""
        query = """
        SELECT 
            DATE(period_start_time) as job_date,
            COUNT(DISTINCT job_id) as unique_jobs,
            COUNT(DISTINCT run_id) as total_runs,
            COUNT(DISTINCT CASE WHEN result_state = 'SUCCESS' THEN run_id END) as successful_runs,
            COUNT(DISTINCT CASE WHEN result_state IN ('FAILED', 'TIMEOUT', 'CANCELLED') THEN run_id END) as failed_runs
        FROM system.lakeflow.job_run_timeline
        WHERE period_start_time >= date_sub(current_timestamp(), {})
            AND result_state IS NOT NULL
        GROUP BY DATE(period_start_time)
        ORDER BY job_date DESC
        """.format(days)
        
        return self.query_system_table(query)
    
    def close_connections(self):
        """Close all connections"""
        if self.sql_connection:
            try:
                self.sql_connection.close()
                self.logger.info("SQL connection closed")
            except:
                pass
