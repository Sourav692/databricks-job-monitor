"""
Databricks client utilities for connection management
"""
import os
from databricks.sdk import WorkspaceClient
from databricks import sql
from typing import Optional

class DatabricksConnectionManager:
    """Manages Databricks connections for different environments"""
    
    def __init__(self, host: str = None, token: str = None, warehouse_id: str = None):
        self.host = host or os.getenv('DATABRICKS_HOST')
        self.token = token or os.getenv('DATABRICKS_TOKEN')
        self.warehouse_id = warehouse_id or os.getenv('DATABRICKS_WAREHOUSE_ID')
        
        self._workspace_client = None
        self._sql_connection = None
    
    @property
    def workspace_client(self) -> WorkspaceClient:
        """Get or create workspace client"""
        if self._workspace_client is None:
            if self.is_local_environment:
                self._workspace_client = WorkspaceClient(
                    host=self.host,
                    token=self.token
                )
            else:
                self._workspace_client = WorkspaceClient()
        return self._workspace_client
    
    @property
    def sql_connection(self):
        """Get or create SQL connection for system tables"""
        if self._sql_connection is None and self.warehouse_id:
            if self.is_local_environment:
                self._sql_connection = sql.connect(
                    server_hostname=self.host.replace('https://', '').replace('http://', ''),
                    http_path=f'/sql/1.0/warehouses/{self.warehouse_id}',
                    access_token=self.token
                )
            else:
                # In Databricks environment, use current session
                pass
        return self._sql_connection
    
    @property
    def is_local_environment(self) -> bool:
        """Check if running in local environment"""
        return 'DATABRICKS_RUNTIME_VERSION' not in os.environ
    
    def test_connection(self) -> bool:
        """Test if connections are working"""
        try:
            if self.is_local_environment:
                # Test workspace client
                workspaces = list(self.workspace_client.clusters.list())
                return True
            else:
                # In Databricks, assume connection is valid
                return True
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False
    
    def close_connections(self):
        """Close all open connections"""
        if self._sql_connection:
            try:
                self._sql_connection.close()
            except:
                pass
            self._sql_connection = None