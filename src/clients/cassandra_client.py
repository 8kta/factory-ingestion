from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from typing import Any, Dict, List, Optional
from .base_client import BaseClient


class CassandraClient(BaseClient):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.hosts = config.get('hosts', ['localhost'])
        self.port = config.get('port', 9042)
        self.keyspace = config.get('keyspace')
        self.username = config.get('username')
        self.password = config.get('password')
        self.cluster = None
        self.session = None
    
    def connect(self) -> None:
        auth_provider = None
        if self.username and self.password:
            auth_provider = PlainTextAuthProvider(
                username=self.username,
                password=self.password
            )
        
        self.cluster = Cluster(
            contact_points=self.hosts,
            port=self.port,
            auth_provider=auth_provider
        )
        
        self.connection = self.cluster.connect()
        
        if self.keyspace:
            self.connection.set_keyspace(self.keyspace)
    
    def disconnect(self) -> None:
        if self.connection:
            self.connection.shutdown()
            self.connection = None
        if self.cluster:
            self.cluster.shutdown()
            self.cluster = None
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        if not self.connection:
            raise ConnectionError("Client not connected. Call connect() first.")
        
        if params:
            rows = self.connection.execute(query, params)
        else:
            rows = self.connection.execute(query)
        
        results = []
        for row in rows:
            results.append(dict(row._asdict()))
        
        return results
    
    def is_connected(self) -> bool:
        return self.connection is not None and not self.connection.is_shutdown
