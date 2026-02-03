"""
Apache Solr Writer for Data Ingestion
"""

from typing import Any, Dict, List

try:
    import pysolr
except ImportError:
    pysolr = None

from .base_writer import BaseWriter
from ..exceptions import ConnectionError as ClientConnectionError


class SolrWriter(BaseWriter):
    """Writer for Apache Solr."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.url = config.get('url')
        self.timeout = config.get('timeout', 10)
        self.commit = config.get('commit', True)
        self.client = None
        
        self.logger.info(f"SolrWriter initialized for URL: {self.url}")
    
    def connect(self) -> None:
        """Connect to Solr."""
        try:
            if pysolr is None:
                raise ImportError("pysolr is not installed")
            
            self.logger.debug(f"Connecting to Solr at {self.url}")
            
            self.client = pysolr.Solr(self.url, timeout=self.timeout)
            
            # Test connection
            self.client.ping()
            
            self.logger.info("Successfully connected to Solr")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Solr: {e}", exc_info=True)
            raise ClientConnectionError(f"Failed to connect to Solr: {e}")
    
    def disconnect(self) -> None:
        """Disconnect from Solr."""
        if self.client:
            self.logger.debug("Disconnecting from Solr")
            self.client = None
            self.logger.info("Disconnected from Solr")
    
    def write(self, data: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """Write data to Solr."""
        if not self.client:
            raise ClientConnectionError("Not connected. Call connect() first.")
        
        try:
            commit = kwargs.get('commit', self.commit)
            
            self.logger.debug(f"Writing {len(data)} documents to Solr")
            
            # Add documents
            self.client.add(data, commit=commit)
            
            self.logger.info(f"Successfully wrote {len(data)} documents to Solr")
            
            return {
                'success': True,
                'records_written': len(data),
                'committed': commit
            }
            
        except Exception as e:
            self.logger.error(f"Error writing to Solr: {e}", exc_info=True)
            return {
                'success': False,
                'records_written': 0,
                'error': str(e)
            }
    
    def is_connected(self) -> bool:
        """Check if connected."""
        return self.client is not None
