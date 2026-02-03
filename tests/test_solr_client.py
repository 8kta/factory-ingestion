import pytest
from unittest.mock import Mock, patch
from src.clients.solr_client import SolrClient


class TestSolrClient:
    
    @patch('src.clients.solr_client.pysolr.Solr')
    def test_connect_success(self, mock_solr_class, sample_solr_config):
        mock_client = Mock()
        mock_solr_class.return_value = mock_client
        
        client = SolrClient(sample_solr_config)
        client.connect()
        
        mock_solr_class.assert_called_once_with('http://localhost:8983/solr/test_core', timeout=10)
        assert client.connection == mock_client
    
    @patch('src.clients.solr_client.pysolr.Solr')
    def test_disconnect(self, mock_solr_class, sample_solr_config):
        mock_client = Mock()
        mock_solr_class.return_value = mock_client
        
        client = SolrClient(sample_solr_config)
        client.connect()
        client.disconnect()
        
        assert client.connection is None
    
    @patch('src.clients.solr_client.pysolr.Solr')
    def test_execute_query_success(self, mock_solr_class, sample_solr_config):
        mock_doc1 = {'id': '1', 'name': 'Alice', 'age': 30}
        mock_doc2 = {'id': '2', 'name': 'Bob', 'age': 25}
        
        mock_results = Mock()
        mock_results.__iter__ = Mock(return_value=iter([mock_doc1, mock_doc2]))
        
        mock_client = Mock()
        mock_client.search.return_value = mock_results
        mock_solr_class.return_value = mock_client
        
        client = SolrClient(sample_solr_config)
        client.connect()
        
        results = client.execute_query('name:Alice')
        
        mock_client.search.assert_called_once_with('name:Alice')
        assert len(results) == 2
        assert results[0] == mock_doc1
        assert results[1] == mock_doc2
    
    @patch('src.clients.solr_client.pysolr.Solr')
    def test_execute_query_with_params(self, mock_solr_class, sample_solr_config):
        mock_results = Mock()
        mock_results.__iter__ = Mock(return_value=iter([]))
        
        mock_client = Mock()
        mock_client.search.return_value = mock_results
        mock_solr_class.return_value = mock_client
        
        client = SolrClient(sample_solr_config)
        client.connect()
        
        params = {'rows': 20, 'sort': 'score desc'}
        results = client.execute_query('*:*', params)
        
        mock_client.search.assert_called_once_with('*:*', rows=20, sort='score desc')
    
    def test_execute_query_not_connected(self, sample_solr_config):
        client = SolrClient(sample_solr_config)
        
        with pytest.raises(ConnectionError, match="Client not connected"):
            client.execute_query('*:*')
    
    @patch('src.clients.solr_client.pysolr.Solr')
    def test_is_connected(self, mock_solr_class, sample_solr_config):
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_solr_class.return_value = mock_client
        
        client = SolrClient(sample_solr_config)
        assert not client.is_connected()
        
        client.connect()
        assert client.is_connected()
        
        mock_client.ping.side_effect = Exception("Connection error")
        assert not client.is_connected()
