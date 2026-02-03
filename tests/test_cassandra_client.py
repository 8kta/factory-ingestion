import pytest
from unittest.mock import Mock, patch, MagicMock
from src.clients.cassandra_client import CassandraClient


class TestCassandraClient:
    
    @patch('src.clients.cassandra_client.Cluster')
    @patch('src.clients.cassandra_client.PlainTextAuthProvider')
    def test_connect_success(self, mock_auth_provider, mock_cluster_class, sample_cassandra_config):
        mock_auth = Mock()
        mock_auth_provider.return_value = mock_auth
        
        mock_session = Mock()
        mock_cluster = Mock()
        mock_cluster.connect.return_value = mock_session
        mock_cluster_class.return_value = mock_cluster
        
        client = CassandraClient(sample_cassandra_config)
        client.connect()
        
        mock_auth_provider.assert_called_once_with(username='cassandra', password='cassandra')
        mock_cluster_class.assert_called_once_with(
            contact_points=['localhost'],
            port=9042,
            auth_provider=mock_auth
        )
        mock_session.set_keyspace.assert_called_once_with('test_keyspace')
        assert client.connection == mock_session
    
    @patch('src.clients.cassandra_client.Cluster')
    def test_disconnect(self, mock_cluster_class, sample_cassandra_config):
        mock_session = Mock()
        mock_cluster = Mock()
        mock_cluster.connect.return_value = mock_session
        mock_cluster_class.return_value = mock_cluster
        
        client = CassandraClient(sample_cassandra_config)
        client.connect()
        client.disconnect()
        
        mock_session.shutdown.assert_called_once()
        mock_cluster.shutdown.assert_called_once()
        assert client.connection is None
    
    @patch('src.clients.cassandra_client.Cluster')
    def test_execute_query_success(self, mock_cluster_class, sample_cassandra_config):
        mock_row1 = Mock()
        mock_row1._asdict.return_value = {'id': 1, 'name': 'Alice'}
        
        mock_row2 = Mock()
        mock_row2._asdict.return_value = {'id': 2, 'name': 'Bob'}
        
        mock_session = Mock()
        mock_session.execute.return_value = [mock_row1, mock_row2]
        
        mock_cluster = Mock()
        mock_cluster.connect.return_value = mock_session
        mock_cluster_class.return_value = mock_cluster
        
        client = CassandraClient(sample_cassandra_config)
        client.connect()
        
        results = client.execute_query("SELECT * FROM users")
        
        mock_session.execute.assert_called_once_with("SELECT * FROM users")
        assert len(results) == 2
        assert results[0] == {'id': 1, 'name': 'Alice'}
    
    @patch('src.clients.cassandra_client.Cluster')
    def test_execute_query_with_params(self, mock_cluster_class, sample_cassandra_config):
        mock_row = Mock()
        mock_row._asdict.return_value = {'id': 1, 'name': 'Alice'}
        
        mock_session = Mock()
        mock_session.execute.return_value = [mock_row]
        
        mock_cluster = Mock()
        mock_cluster.connect.return_value = mock_session
        mock_cluster_class.return_value = mock_cluster
        
        client = CassandraClient(sample_cassandra_config)
        client.connect()
        
        params = {'user_id': 1}
        results = client.execute_query("SELECT * FROM users WHERE id = ?", params)
        
        mock_session.execute.assert_called_once_with("SELECT * FROM users WHERE id = ?", params)
        assert len(results) == 1
    
    def test_execute_query_not_connected(self, sample_cassandra_config):
        client = CassandraClient(sample_cassandra_config)
        
        with pytest.raises(ConnectionError, match="Client not connected"):
            client.execute_query("SELECT * FROM users")
    
    @patch('src.clients.cassandra_client.Cluster')
    def test_is_connected(self, mock_cluster_class, sample_cassandra_config):
        mock_session = Mock()
        mock_session.is_shutdown = False
        
        mock_cluster = Mock()
        mock_cluster.connect.return_value = mock_session
        mock_cluster_class.return_value = mock_cluster
        
        client = CassandraClient(sample_cassandra_config)
        assert not client.is_connected()
        
        client.connect()
        assert client.is_connected()
        
        mock_session.is_shutdown = True
        assert not client.is_connected()
