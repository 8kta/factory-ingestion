"""
MongoDB Writer for Data Ingestion
"""

from typing import Any, Dict, List

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure
except ImportError:
    MongoClient = None
    ConnectionFailure = Exception

from .base_writer import BaseWriter
from ..exceptions import ConnectionError as ClientConnectionError


class MongoDBWriter(BaseWriter):
    """Writer for MongoDB."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 27017)
        self.database = config.get('database')
        self.collection = config.get('collection')
        self.username = config.get('username')
        self.password = config.get('password')
        self.client = None
        self.db = None
        
        self.logger.info(f"MongoDBWriter initialized for {self.database}.{self.collection}")
    
    def connect(self) -> None:
        """Connect to MongoDB."""
        try:
            if MongoClient is None:
                raise ImportError("pymongo is not installed. Install it with: pip install pymongo")
            
            self.logger.debug(f"Connecting to MongoDB at {self.host}:{self.port}")
            
            # Build connection string
            if self.username and self.password:
                connection_string = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/"
            else:
                connection_string = f"mongodb://{self.host}:{self.port}/"
            
            self.client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
            
            # Test connection
            self.client.admin.command('ping')
            
            self.db = self.client[self.database]
            
            self.logger.info(f"Successfully connected to MongoDB database: {self.database}")
            
        except ConnectionFailure as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}", exc_info=True)
            raise ClientConnectionError(f"Failed to connect to MongoDB: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to MongoDB: {e}", exc_info=True)
            raise ClientConnectionError(f"Unexpected MongoDB connection error: {e}")
    
    def disconnect(self) -> None:
        """Disconnect from MongoDB."""
        if self.client:
            self.logger.debug("Disconnecting from MongoDB")
            self.client.close()
            self.client = None
            self.db = None
            self.logger.info("Disconnected from MongoDB")
    
    def write(self, data: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """Write data to MongoDB."""
        if not self.db:
            raise ClientConnectionError("Not connected. Call connect() first.")
        
        try:
            collection = kwargs.get('collection', self.collection)
            
            if not collection:
                raise ValueError("Collection name is required")
            
            self.logger.debug(f"Writing {len(data)} documents to {self.database}.{collection}")
            
            # Insert documents
            coll = self.db[collection]
            result = coll.insert_many(data)
            
            inserted_count = len(result.inserted_ids)
            
            self.logger.info(f"Successfully wrote {inserted_count} documents to MongoDB")
            
            return {
                'success': True,
                'records_written': inserted_count,
                'collection': f"{self.database}.{collection}",
                'inserted_ids': [str(id) for id in result.inserted_ids]
            }
            
        except Exception as e:
            self.logger.error(f"Error writing to MongoDB: {e}", exc_info=True)
            return {
                'success': False,
                'records_written': 0,
                'error': str(e)
            }
    
    def is_connected(self) -> bool:
        """Check if connected."""
        return self.client is not None
