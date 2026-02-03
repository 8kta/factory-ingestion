import pytest
from unittest.mock import Mock, patch, MagicMock
from src.clients.postgres_client import PostgresClient


class TestPostgresClient:
    
    @patch('src.clients.postgres_client.psycopg2.connect')
    def test_connect_success(self, mock_connect, sample_postgres_config):
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        client = PostgresClient(sample_postgres_config)
        client.connect()
        
        mock_connect.assert_called_once_with(
            host='localhost',
            port=5432,
            database='test_db',
            user='test_user',
            password='test_pass'
        )
        assert client.connection == mock_connection
    
    @patch('src.clients.postgres_client.psycopg2.connect')
    def test_disconnect(self, mock_connect, sample_postgres_config):
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        client = PostgresClient(sample_postgres_config)
        client.connect()
        client.disconnect()
        
        mock_connection.close.assert_called_once()
        assert client.connection is None
    
    @patch('src.clients.postgres_client.psycopg2.connect')
    def test_execute_query_success(self, mock_connect, sample_postgres_config):
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'}
        ]
        
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        client = PostgresClient(sample_postgres_config)
        client.connect()
        
        results = client.execute_query("SELECT * FROM users")
        
        mock_cursor.execute.assert_called_once_with("SELECT * FROM users")
        assert len(results) == 2
        assert results[0] == {'id': 1, 'name': 'Alice'}
    
    @patch('src.clients.postgres_client.psycopg2.connect')
    def test_execute_query_with_params(self, mock_connect, sample_postgres_config):
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [{'id': 1, 'name': 'Alice'}]
        
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        client = PostgresClient(sample_postgres_config)
        client.connect()
        
        params = {'user_id': 1}
        results = client.execute_query("SELECT * FROM users WHERE id = %(user_id)s", params)
        
        mock_cursor.execute.assert_called_once_with(
            "SELECT * FROM users WHERE id = %(user_id)s",
            params
        )
        assert len(results) == 1
    
    def test_execute_query_not_connected(self, sample_postgres_config):
        client = PostgresClient(sample_postgres_config)
        
        with pytest.raises(ConnectionError, match="Client not connected"):
            client.execute_query("SELECT * FROM users")
    
    @patch('src.clients.postgres_client.psycopg2.connect')
    def test_is_connected(self, mock_connect, sample_postgres_config):
        mock_connection = Mock()
        mock_connection.closed = 0
        mock_connect.return_value = mock_connection
        
        client = PostgresClient(sample_postgres_config)
        assert not client.is_connected()
        
        client.connect()
        assert client.is_connected()
        
        client.disconnect()
        assert not client.is_connected()
    
    @patch('src.clients.postgres_client.psycopg2.connect')
    def test_context_manager(self, mock_connect, sample_postgres_config):
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        client = PostgresClient(sample_postgres_config)
        
        with client:
            assert client.connection is not None
        
        mock_connection.close.assert_called_once()
