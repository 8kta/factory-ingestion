import pytest
from unittest.mock import Mock, patch
from src.clients.athena_client import AthenaClient


class TestAthenaClient:
    
    @patch('src.clients.athena_client.boto3.client')
    def test_connect_success(self, mock_boto_client, sample_athena_config):
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        client = AthenaClient(sample_athena_config)
        client.connect()
        
        mock_boto_client.assert_called_once_with(
            'athena',
            region_name='us-east-1',
            aws_access_key_id='test_key',
            aws_secret_access_key='test_secret'
        )
        assert client.client == mock_client
    
    @patch('src.clients.athena_client.boto3.client')
    def test_disconnect(self, mock_boto_client, sample_athena_config):
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        client = AthenaClient(sample_athena_config)
        client.connect()
        client.disconnect()
        
        assert client.client is None
    
    @patch('src.clients.athena_client.boto3.client')
    def test_execute_query_success(self, mock_boto_client, sample_athena_config):
        mock_client = Mock()
        mock_client.start_query_execution.return_value = {'QueryExecutionId': 'query-123'}
        mock_client.get_query_execution.return_value = {
            'QueryExecution': {
                'Status': {'State': 'SUCCEEDED'}
            }
        }
        mock_client.get_query_results.return_value = {
            'ResultSet': {
                'ResultSetMetadata': {
                    'ColumnInfo': [
                        {'Label': 'id'},
                        {'Label': 'name'}
                    ]
                },
                'Rows': [
                    {'Data': [{'VarCharValue': 'id'}, {'VarCharValue': 'name'}]},
                    {'Data': [{'VarCharValue': '1'}, {'VarCharValue': 'Alice'}]},
                    {'Data': [{'VarCharValue': '2'}, {'VarCharValue': 'Bob'}]}
                ]
            }
        }
        mock_boto_client.return_value = mock_client
        
        client = AthenaClient(sample_athena_config)
        client.connect()
        
        results = client.execute_query("SELECT * FROM users")
        
        assert len(results) == 2
        assert results[0] == {'id': '1', 'name': 'Alice'}
        assert results[1] == {'id': '2', 'name': 'Bob'}
    
    @patch('src.clients.athena_client.boto3.client')
    def test_execute_query_failed(self, mock_boto_client, sample_athena_config):
        mock_client = Mock()
        mock_client.start_query_execution.return_value = {'QueryExecutionId': 'query-123'}
        mock_client.get_query_execution.return_value = {
            'QueryExecution': {
                'Status': {'State': 'FAILED'}
            }
        }
        mock_boto_client.return_value = mock_client
        
        client = AthenaClient(sample_athena_config)
        client.connect()
        
        with pytest.raises(Exception, match="Query failed with status: FAILED"):
            client.execute_query("SELECT * FROM users")
    
    def test_execute_query_not_connected(self, sample_athena_config):
        client = AthenaClient(sample_athena_config)
        
        with pytest.raises(ConnectionError, match="Client not connected"):
            client.execute_query("SELECT * FROM users")
    
    @patch('src.clients.athena_client.boto3.client')
    def test_is_connected(self, mock_boto_client, sample_athena_config):
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        client = AthenaClient(sample_athena_config)
        assert not client.is_connected()
        
        client.connect()
        assert client.is_connected()
        
        client.disconnect()
        assert not client.is_connected()
