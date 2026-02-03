import pysolr
from typing import Any, Dict, List, Optional
from .base_client import BaseClient


class SolrClient(BaseClient):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.url = config.get('url')
        self.timeout = config.get('timeout', 10)
    
    def connect(self) -> None:
        self.connection = pysolr.Solr(self.url, timeout=self.timeout)
    
    def disconnect(self) -> None:
        self.connection = None
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        if not self.connection:
            raise ConnectionError("Client not connected. Call connect() first.")
        
        search_params = params if params else {}
        results = self.connection.search(query, **search_params)
        
        return [dict(doc) for doc in results]
    
    def is_connected(self) -> bool:
        if not self.connection:
            return False
        try:
            self.connection.ping()
            return True
        except:
            return False
