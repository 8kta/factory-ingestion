import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Any, Dict, List, Optional
from .base_client import BaseClient
from ..exceptions import ConnectionError as ClientConnectionError, QueryExecutionError, AuthenticationError


class PostgresClient(BaseClient):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.host = config.get('host')
        self.port = config.get('port', 5432)
        self.database = config.get('database')
        self.username = config.get('username')
        self.password = config.get('password')
        
        self.logger.info(f"PostgreSQL client initialized for {self.host}:{self.port}/{self.database}")
    
    def connect(self) -> None:
        try:
            self.logger.debug(f"Connecting to PostgreSQL at {self.host}:{self.port}")
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password,
                connect_timeout=10
            )
            self.logger.info(f"Successfully connected to PostgreSQL database '{self.database}'")
        except psycopg2.OperationalError as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}", exc_info=True)
            if "authentication failed" in str(e).lower():
                raise AuthenticationError(f"Authentication failed for user '{self.username}': {e}")
            raise ClientConnectionError(f"Failed to connect to PostgreSQL at {self.host}:{self.port}: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error during PostgreSQL connection: {e}", exc_info=True)
            raise ClientConnectionError(f"Unexpected connection error: {e}")
    
    def disconnect(self) -> None:
        try:
            if self.connection:
                self.logger.debug("Disconnecting from PostgreSQL")
                self.connection.close()
                self.connection = None
                self.logger.info("Successfully disconnected from PostgreSQL")
        except Exception as e:
            self.logger.error(f"Error during PostgreSQL disconnect: {e}", exc_info=True)
            self.connection = None
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        if not self.connection:
            self.logger.error("Attempted to execute query without connection")
            raise ClientConnectionError("Client not connected. Call connect() first.")
        
        try:
            self.logger.debug(f"Executing query: {query[:100]}..." if len(query) > 100 else f"Executing query: {query}")
            if params:
                self.logger.debug(f"Query parameters: {params}")
            
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            results = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            
            self.logger.info(f"Query executed successfully, returned {len(results)} rows")
            return results
            
        except psycopg2.Error as e:
            self.logger.error(f"PostgreSQL query execution error: {e}", exc_info=True)
            raise QueryExecutionError(f"Query execution failed: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error during query execution: {e}", exc_info=True)
            raise QueryExecutionError(f"Unexpected query error: {e}")
    
    def is_connected(self) -> bool:
        connected = self.connection is not None and not self.connection.closed
        self.logger.debug(f"Connection status: {connected}")
        return connected
