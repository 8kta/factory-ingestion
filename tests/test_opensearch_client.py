import pytest
from unittest.mock import Mock, patch
from src.clients.opensearch_client import OpenSearchClient


class TestOpenSearchClient:
    
    @patch('src.clients.opensearch_client.OpenSearch')
    def test_connect_success(self, mock_opensearch_class, sample_opensearch_config):
        mock_client = Mock()
        mock_opensearch_class.return_value = mock_client
        
        client = OpenSearchClient(sample_opensearch_config)
        client.connect()
        
        mock_opensearch_class.assert_called_once_with(
            hosts=['localhost:9200'],
            http_auth=('admin', 'admin'),
            use_ssl=True,
            verify_certs=False
        )
        assert client.connection == mock_client
    
    @patch('src.clients.opensearch_client.OpenSearch')
    def test_connect_without_auth(self, mock_opensearch_class):
        config = {
            'hosts': ['localhost:9200'],
            'use_ssl': False,
            'verify_certs': False
        }
        mock_client = Mock()
        mock_opensearch_class.return_value = mock_client
        
        client = OpenSearchClient(config)
        client.connect()
        
        mock_opensearch_class.assert_called_once_with(
            hosts=['localhost:9200'],
            http_auth=None,
            use_ssl=False,
            verify_certs=False
        )
    
    @patch('src.clients.opensearch_client.OpenSearch')
    def test_disconnect(self, mock_opensearch_class, sample_opensearch_config):
        mock_client = Mock()
        mock_opensearch_class.return_value = mock_client
        
        client = OpenSearchClient(sample_opensearch_config)
        client.connect()
        client.disconnect()
        
        mock_client.close.assert_called_once()
        assert client.connection is None
    
    @patch('src.clients.opensearch_client.OpenSearch')
    def test_execute_query_success(self, mock_opensearch_class, sample_opensearch_config):
        mock_client = Mock()
        mock_client.search.return_value = {
            'hits': {
                'hits': [
                    {
                        '_id': '1',
                        '_index': 'users',
                        '_source': {'name': 'Alice', 'age': 30}
                    },
                    {
                        '_id': '2',
                        '_index': 'users',
                        '_source': {'name': 'Bob', 'age': 25}
                    }
                ]
            }
        }
        mock_opensearch_class.return_value = mock_client
        
        client = OpenSearchClient(sample_opensearch_config)
        client.connect()
        
        results = client.execute_query('name:Alice', {'index': 'users'})
        
        assert len(results) == 2
        assert results[0]['name'] == 'Alice'
        assert results[0]['_id'] == '1'
        assert results[0]['_index'] == 'users'
    
    @patch('src.clients.opensearch_client.OpenSearch')
    def test_execute_query_with_body(self, mock_opensearch_class, sample_opensearch_config):
        mock_client = Mock()
        mock_client.search.return_value = {'hits': {'hits': []}}
        mock_opensearch_class.return_value = mock_client
        
        client = OpenSearchClient(sample_opensearch_config)
        client.connect()
        
        body = {'size': 10, 'from': 0}
        results = client.execute_query('*:*', {'index': 'users', 'body': body})
        
        expected_body = {'size': 10, 'from': 0, 'query': {'query_string': {'query': '*:*'}}}
        mock_client.search.assert_called_once_with(index='users', body=expected_body)
    
    def test_execute_query_not_connected(self, sample_opensearch_config):
        client = OpenSearchClient(sample_opensearch_config)
        
        with pytest.raises(ConnectionError, match="Client not connected"):
            client.execute_query('*:*')
    
    @patch('src.clients.opensearch_client.OpenSearch')
    def test_is_connected(self, mock_opensearch_class, sample_opensearch_config):
        mock_client = Mock()
        mock_client.info.return_value = {'version': '1.0'}
        mock_opensearch_class.return_value = mock_client
        
        client = OpenSearchClient(sample_opensearch_config)
        assert not client.is_connected()
        
        client.connect()
        assert client.is_connected()
        
        mock_client.info.side_effect = Exception("Connection error")
        assert not client.is_connected()
