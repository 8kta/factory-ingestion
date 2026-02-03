import mysql.connector
from typing import Any, Dict, List, Optional
from .base_client import BaseClient


class MySQLClient(BaseClient):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.host = config.get('host')
        self.port = config.get('port', 3306)
        self.database = config.get('database')
        self.username = config.get('username')
        self.password = config.get('password')
    
    def connect(self) -> None:
        self.connection = mysql.connector.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.username,
            password=self.password
        )
    
    def disconnect(self) -> None:
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        if not self.connection:
            raise ConnectionError("Client not connected. Call connect() first.")
        
        cursor = self.connection.cursor(dictionary=True)
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        results = cursor.fetchall()
        cursor.close()
        
        return results
    
    def is_connected(self) -> bool:
        return self.connection is not None and self.connection.is_connected()
