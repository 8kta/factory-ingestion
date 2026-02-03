import pytest
from unittest.mock import Mock, patch, MagicMock
from src.clients.sqlite_client import SQLiteClient


class TestSQLiteClient:
    
    @patch('src.clients.sqlite_client.sqlite3.connect')
    def test_connect_success(self, mock_connect, sample_sqlite_config):
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        client = SQLiteClient(sample_sqlite_config)
        client.connect()
        
        mock_connect.assert_called_once_with(':memory:')
        assert client.connection == mock_connection
    
    @patch('src.clients.sqlite_client.sqlite3.connect')
    def test_disconnect(self, mock_connect, sample_sqlite_config):
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        client = SQLiteClient(sample_sqlite_config)
        client.connect()
        client.disconnect()
        
        mock_connection.close.assert_called_once()
        assert client.connection is None
    
    @patch('src.clients.sqlite_client.sqlite3.connect')
    def test_execute_query_success(self, mock_connect, sample_sqlite_config):
        mock_row1 = MagicMock()
        mock_row1.keys.return_value = ['id', 'name']
        mock_row1.__getitem__.side_effect = lambda key: {0: 1, 1: 'Alice', 'id': 1, 'name': 'Alice'}[key]
        
        mock_row2 = MagicMock()
        mock_row2.keys.return_value = ['id', 'name']
        mock_row2.__getitem__.side_effect = lambda key: {0: 2, 1: 'Bob', 'id': 2, 'name': 'Bob'}[key]
        
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [mock_row1, mock_row2]
        
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        client = SQLiteClient(sample_sqlite_config)
        client.connect()
        
        results = client.execute_query("SELECT * FROM users")
        
        mock_cursor.execute.assert_called_once_with("SELECT * FROM users")
        assert len(results) == 2
    
    @patch('src.clients.sqlite_client.sqlite3.connect')
    def test_execute_query_with_params(self, mock_connect, sample_sqlite_config):
        mock_row = MagicMock()
        mock_row.keys.return_value = ['id', 'name']
        mock_row.__getitem__.side_effect = lambda key: {0: 1, 1: 'Alice', 'id': 1, 'name': 'Alice'}[key]
        
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [mock_row]
        
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        client = SQLiteClient(sample_sqlite_config)
        client.connect()
        
        params = {'user_id': 1}
        results = client.execute_query("SELECT * FROM users WHERE id = :user_id", params)
        
        mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = :user_id", params)
        assert len(results) == 1
    
    def test_execute_query_not_connected(self, sample_sqlite_config):
        client = SQLiteClient(sample_sqlite_config)
        
        with pytest.raises(ConnectionError, match="Client not connected"):
            client.execute_query("SELECT * FROM users")
    
    @patch('src.clients.sqlite_client.sqlite3.connect')
    def test_is_connected(self, mock_connect, sample_sqlite_config):
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        client = SQLiteClient(sample_sqlite_config)
        assert not client.is_connected()
        
        client.connect()
        assert client.is_connected()
        
        client.disconnect()
        assert not client.is_connected()
