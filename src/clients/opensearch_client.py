from opensearchpy import OpenSearch
from typing import Any, Dict, List, Optional
from .base_client import BaseClient


class OpenSearchClient(BaseClient):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.hosts = config.get('hosts', ['localhost:9200'])
        self.username = config.get('username')
        self.password = config.get('password')
        self.use_ssl = config.get('use_ssl', True)
        self.verify_certs = config.get('verify_certs', True)
    
    def connect(self) -> None:
        auth = (self.username, self.password) if self.username and self.password else None
        
        self.connection = OpenSearch(
            hosts=self.hosts,
            http_auth=auth,
            use_ssl=self.use_ssl,
            verify_certs=self.verify_certs
        )
    
    def disconnect(self) -> None:
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        if not self.connection:
            raise ConnectionError("Client not connected. Call connect() first.")
        
        index = params.get('index', '_all') if params else '_all'
        body = params.get('body', {}) if params else {}
        
        if query:
            body['query'] = {'query_string': {'query': query}}
        
        response = self.connection.search(index=index, body=body)
        
        results = []
        for hit in response['hits']['hits']:
            result = hit['_source']
            result['_id'] = hit['_id']
            result['_index'] = hit['_index']
            results.append(result)
        
        return results
    
    def is_connected(self) -> bool:
        if not self.connection:
            return False
        try:
            self.connection.info()
            return True
        except:
            return False
