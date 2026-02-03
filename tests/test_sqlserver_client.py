import pytest
from unittest.mock import Mock, patch, MagicMock
import sys

# Mock pyodbc module before importing the client
mock_pyodbc = MagicMock()
sys.modules['pyodbc'] = mock_pyodbc

from src.clients.sqlserver_client import SQLServerClient
import src.clients.sqlserver_client as sqlserver_module


class TestSQLServerClient:
    
    @patch.object(sqlserver_module, 'pyodbc', mock_pyodbc)
    @patch('pyodbc.connect')
    def test_connect_success(self, mock_connect, sample_sqlserver_config):
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        client = SQLServerClient(sample_sqlserver_config)
        client.connect()
        
        expected_conn_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost,1433;"
            "DATABASE=test_db;"
            "UID=sa;"
            "PWD=Password123"
        )
        mock_connect.assert_called_once_with(expected_conn_string)
        assert client.connection == mock_connection
    
    @patch.object(sqlserver_module, 'pyodbc', mock_pyodbc)
    @patch('pyodbc.connect')
    def test_disconnect(self, mock_connect, sample_sqlserver_config):
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        client = SQLServerClient(sample_sqlserver_config)
        client.connect()
        client.disconnect()
        
        mock_connection.close.assert_called_once()
        assert client.connection is None
    
    @patch.object(sqlserver_module, 'pyodbc', mock_pyodbc)
    @patch('pyodbc.connect')
    def test_execute_query_success(self, mock_connect, sample_sqlserver_config):
        mock_cursor = Mock()
        mock_cursor.description = [('id',), ('name',)]
        mock_cursor.fetchall.return_value = [
            (1, 'Alice'),
            (2, 'Bob')
        ]
        
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        client = SQLServerClient(sample_sqlserver_config)
        client.connect()
        
        results = client.execute_query("SELECT * FROM users")
        
        mock_cursor.execute.assert_called_once_with("SELECT * FROM users")
        assert len(results) == 2
        assert results[0] == {'id': 1, 'name': 'Alice'}
        assert results[1] == {'id': 2, 'name': 'Bob'}
    
    @patch.object(sqlserver_module, 'pyodbc', mock_pyodbc)
    @patch('pyodbc.connect')
    def test_execute_query_with_params(self, mock_connect, sample_sqlserver_config):
        mock_cursor = Mock()
        mock_cursor.description = [('id',), ('name',)]
        mock_cursor.fetchall.return_value = [(1, 'Alice')]
        
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        client = SQLServerClient(sample_sqlserver_config)
        client.connect()
        
        params = {'user_id': 1}
        results = client.execute_query("SELECT * FROM users WHERE id = ?", params)
        
        mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = ?", params)
        assert len(results) == 1
    
    def test_execute_query_not_connected(self, sample_sqlserver_config):
        client = SQLServerClient(sample_sqlserver_config)
        
        with pytest.raises(ConnectionError, match="Client not connected"):
            client.execute_query("SELECT * FROM users")
    
    @patch.object(sqlserver_module, 'pyodbc', mock_pyodbc)
    @patch('pyodbc.connect')
    def test_is_connected(self, mock_connect, sample_sqlserver_config):
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        client = SQLServerClient(sample_sqlserver_config)
        assert not client.is_connected()
        
        client.connect()
        assert client.is_connected()
        
        client.disconnect()
        assert not client.is_connected()
