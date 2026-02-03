"""
OpenSearch Writer for Data Ingestion
"""

from typing import Any, Dict, List

try:
    from opensearchpy import OpenSearch, helpers
except ImportError:
    OpenSearch = None
    helpers = None

from .base_writer import BaseWriter
from ..exceptions import ConnectionError as ClientConnectionError


class OpenSearchWriter(BaseWriter):
    """Writer for OpenSearch."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.hosts = config.get('hosts', ['localhost:9200'])
        self.username = config.get('username')
        self.password = config.get('password')
        self.use_ssl = config.get('use_ssl', False)
        self.verify_certs = config.get('verify_certs', True)
        self.index = config.get('index')
        self.client = None
        
        self.logger.info(f"OpenSearchWriter initialized for index: {self.index}")
    
    def connect(self) -> None:
        """Connect to OpenSearch."""
        try:
            if OpenSearch is None:
                raise ImportError("opensearch-py is not installed")
            
            self.logger.debug(f"Connecting to OpenSearch at {self.hosts}")
            
            auth = None
            if self.username and self.password:
                auth = (self.username, self.password)
            
            self.client = OpenSearch(
                hosts=self.hosts,
                http_auth=auth,
                use_ssl=self.use_ssl,
                verify_certs=self.verify_certs
            )
            
            # Test connection
            self.client.info()
            
            self.logger.info("Successfully connected to OpenSearch")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to OpenSearch: {e}", exc_info=True)
            raise ClientConnectionError(f"Failed to connect to OpenSearch: {e}")
    
    def disconnect(self) -> None:
        """Disconnect from OpenSearch."""
        if self.client:
            self.logger.debug("Disconnecting from OpenSearch")
            self.client = None
            self.logger.info("Disconnected from OpenSearch")
    
    def write(self, data: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """Write data to OpenSearch using bulk API."""
        if not self.client:
            raise ClientConnectionError("Not connected. Call connect() first.")
        
        try:
            index = kwargs.get('index', self.index)
            
            if not index:
                raise ValueError("Index name is required")
            
            self.logger.debug(f"Writing {len(data)} documents to index: {index}")
            
            # Prepare bulk actions
            actions = [
                {
                    '_index': index,
                    '_source': doc
                }
                for doc in data
            ]
            
            # Bulk index
            success, failed = helpers.bulk(
                self.client,
                actions,
                raise_on_error=False
            )
            
            self.logger.info(f"Wrote {success} documents to OpenSearch, {len(failed)} failed")
            
            return {
                'success': True,
                'records_written': success,
                'records_failed': len(failed),
                'index': index
            }
            
        except Exception as e:
            self.logger.error(f"Error writing to OpenSearch: {e}", exc_info=True)
            return {
                'success': False,
                'records_written': 0,
                'error': str(e)
            }
    
    def is_connected(self) -> bool:
        """Check if connected."""
        return self.client is not None
