"""
Cassandra Writer for Data Ingestion
"""

from typing import Any, Dict, List

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
except ImportError:
    Cluster = None
    PlainTextAuthProvider = None

from .base_writer import BaseWriter
from ..exceptions import ConnectionError as ClientConnectionError


class CassandraWriter(BaseWriter):
    """Writer for Apache Cassandra."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.hosts = config.get('hosts', ['localhost'])
        self.port = config.get('port', 9042)
        self.keyspace = config.get('keyspace')
        self.table = config.get('table')
        self.username = config.get('username')
        self.password = config.get('password')
        self.cluster = None
        self.session = None
        
        self.logger.info(f"CassandraWriter initialized for {self.keyspace}.{self.table}")
    
    def connect(self) -> None:
        """Connect to Cassandra."""
        try:
            if Cluster is None:
                raise ImportError("cassandra-driver is not installed")
            
            self.logger.debug(f"Connecting to Cassandra at {self.hosts}")
            
            auth_provider = None
            if self.username and self.password:
                auth_provider = PlainTextAuthProvider(
                    username=self.username,
                    password=self.password
                )
            
            self.cluster = Cluster(
                self.hosts,
                port=self.port,
                auth_provider=auth_provider
            )
            
            self.session = self.cluster.connect(self.keyspace)
            
            self.logger.info(f"Successfully connected to Cassandra keyspace: {self.keyspace}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Cassandra: {e}", exc_info=True)
            raise ClientConnectionError(f"Failed to connect to Cassandra: {e}")
    
    def disconnect(self) -> None:
        """Disconnect from Cassandra."""
        if self.cluster:
            self.logger.debug("Disconnecting from Cassandra")
            self.cluster.shutdown()
            self.cluster = None
            self.session = None
            self.logger.info("Disconnected from Cassandra")
    
    def write(self, data: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """Write data to Cassandra."""
        if not self.session:
            raise ClientConnectionError("Not connected. Call connect() first.")
        
        try:
            table = kwargs.get('table', self.table)
            
            if not table:
                raise ValueError("Table name is required")
            
            self.logger.debug(f"Writing {len(data)} records to {self.keyspace}.{table}")
            
            # Build INSERT statement
            if data:
                columns = list(data[0].keys())
                placeholders = ', '.join(['?' for _ in columns])
                column_names = ', '.join(columns)
                
                query = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"
                prepared = self.session.prepare(query)
                
                # Execute batch inserts
                success_count = 0
                for record in data:
                    try:
                        values = [record.get(col) for col in columns]
                        self.session.execute(prepared, values)
                        success_count += 1
                    except Exception as e:
                        self.logger.warning(f"Failed to insert record: {e}")
                
                self.logger.info(f"Successfully wrote {success_count}/{len(data)} records to Cassandra")
                
                return {
                    'success': True,
                    'records_written': success_count,
                    'records_failed': len(data) - success_count,
                    'table': f"{self.keyspace}.{table}"
                }
            
            return {'success': True, 'records_written': 0}
            
        except Exception as e:
            self.logger.error(f"Error writing to Cassandra: {e}", exc_info=True)
            return {
                'success': False,
                'records_written': 0,
                'error': str(e)
            }
    
    def is_connected(self) -> bool:
        """Check if connected."""
        return self.session is not None
