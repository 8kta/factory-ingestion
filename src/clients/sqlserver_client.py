from typing import Any, Dict, List, Optional
from .base_client import BaseClient

try:
    import pyodbc
except ImportError:
    pyodbc = None


class SQLServerClient(BaseClient):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.host = config.get('host')
        self.port = config.get('port', 1433)
        self.database = config.get('database')
        self.username = config.get('username')
        self.password = config.get('password')
        self.driver = config.get('driver', '{ODBC Driver 17 for SQL Server}')
    
    def connect(self) -> None:
        if pyodbc is None:
            raise ImportError(
                "pyodbc is not installed. Install it with: pip install pyodbc\n"
                "Note: pyodbc requires unixODBC to be installed on your system.\n"
                "On macOS: brew install unixodbc\n"
                "On Ubuntu: apt-get install unixodbc unixodbc-dev"
            )
        connection_string = (
            f"DRIVER={self.driver};"
            f"SERVER={self.host},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password}"
        )
        self.connection = pyodbc.connect(connection_string)
    
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
        
        columns = [column[0] for column in cursor.description]
        results = []
        
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        
        cursor.close()
        return results
    
    def is_connected(self) -> bool:
        return self.connection is not None
