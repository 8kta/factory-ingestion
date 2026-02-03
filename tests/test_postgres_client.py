import pytest
from unittest.mock import Mock, patch
import psycopg2
from src.clients.postgres_client import PostgresClient
from src.exceptions import (
    ConnectionError as ClientConnectionError,
    QueryExecutionError,
    AuthenticationError
)


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
            password='test_pass',
            connect_timeout=10
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
        
        with pytest.raises(ClientConnectionError, match="Client not connected"):
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
    
    # Error Handling Tests
    
    @patch('src.clients.postgres_client.psycopg2.connect')
    def test_connect_operational_error(self, mock_connect, sample_postgres_config):
        """Test connection failure raises ClientConnectionError."""
        mock_connect.side_effect = psycopg2.OperationalError("Connection refused")
        
        client = PostgresClient(sample_postgres_config)
        
        with pytest.raises(ClientConnectionError) as exc_info:
            client.connect()
        
        assert "Connection refused" in str(exc_info.value)
    
    @patch('src.clients.postgres_client.psycopg2.connect')
    def test_connect_authentication_error(self, mock_connect, sample_postgres_config):
        """Test authentication failure raises AuthenticationError."""
        mock_connect.side_effect = psycopg2.OperationalError("authentication failed for user")
        
        client = PostgresClient(sample_postgres_config)
        
        with pytest.raises(AuthenticationError) as exc_info:
            client.connect()
        
        assert "authentication failed" in str(exc_info.value).lower()
    
    @patch('src.clients.postgres_client.psycopg2.connect')
    def test_connect_unexpected_error(self, mock_connect, sample_postgres_config):
        """Test unexpected connection error raises ClientConnectionError."""
        mock_connect.side_effect = Exception("Unexpected error")
        
        client = PostgresClient(sample_postgres_config)
        
        with pytest.raises(ClientConnectionError) as exc_info:
            client.connect()
        
        assert "Unexpected error" in str(exc_info.value)
    
    @patch('src.clients.postgres_client.psycopg2.connect')
    def test_execute_query_psycopg2_error(self, mock_connect, sample_postgres_config):
        """Test query execution error raises QueryExecutionError."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = psycopg2.Error("Syntax error")
        
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        client = PostgresClient(sample_postgres_config)
        client.connect()
        
        with pytest.raises(QueryExecutionError) as exc_info:
            client.execute_query("INVALID SQL")
        
        assert "Syntax error" in str(exc_info.value)
    
    @patch('src.clients.postgres_client.psycopg2.connect')
    def test_execute_query_unexpected_error(self, mock_connect, sample_postgres_config):
        """Test unexpected query error raises QueryExecutionError."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = RuntimeError("Unexpected runtime error")
        
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        client = PostgresClient(sample_postgres_config)
        client.connect()
        
        with pytest.raises(QueryExecutionError) as exc_info:
            client.execute_query("SELECT * FROM users")
        
        assert "Unexpected runtime error" in str(exc_info.value)
    
    @patch('src.clients.postgres_client.psycopg2.connect')
    def test_disconnect_with_error(self, mock_connect, sample_postgres_config):
        """Test disconnect handles errors gracefully."""
        mock_connection = Mock()
        mock_connection.close.side_effect = Exception("Close error")
        mock_connect.return_value = mock_connection
        
        client = PostgresClient(sample_postgres_config)
        client.connect()
        
        # Should not raise exception, just log it
        client.disconnect()
        
        # Connection should be set to None even on error
        assert client.connection is None
    
    @patch('src.clients.postgres_client.psycopg2.connect')
    def test_context_manager_with_exception(self, mock_connect, sample_postgres_config):
        """Test context manager handles exceptions properly."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        client = PostgresClient(sample_postgres_config)
        
        with pytest.raises(RuntimeError):
            with client:
                raise RuntimeError("Test error")
        
        # Connection should be closed even after exception
        mock_connection.close.assert_called_once()
