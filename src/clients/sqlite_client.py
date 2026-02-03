import sqlite3
from typing import Any, Dict, List, Optional
from .base_client import BaseClient


class SQLiteClient(BaseClient):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.database_path = config.get('database_path')
    
    def connect(self) -> None:
        self.connection = sqlite3.connect(self.database_path)
        self.connection.row_factory = sqlite3.Row
    
    def disconnect(self) -> None:
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        if not self.connection:
            raise ConnectionError("Client not connected. Call connect() first.")
        
        cursor = self.connection.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        rows = cursor.fetchall()
        results = [dict(row) for row in rows]
        cursor.close()
        
        return results
    
    def is_connected(self) -> bool:
        return self.connection is not None
