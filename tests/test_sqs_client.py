import pytest
from unittest.mock import Mock, patch
from src.clients.sqs_client import SQSClient


class TestSQSClient:
    
    @patch('src.clients.sqs_client.boto3.client')
    def test_connect_success(self, mock_boto_client, sample_sqs_config):
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        client = SQSClient(sample_sqs_config)
        client.connect()
        
        mock_boto_client.assert_called_once_with(
            'sqs',
            region_name='us-east-1',
            aws_access_key_id='test_key',
            aws_secret_access_key='test_secret'
        )
        assert client.connection == mock_client
    
    @patch('src.clients.sqs_client.boto3.client')
    def test_disconnect(self, mock_boto_client, sample_sqs_config):
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        client = SQSClient(sample_sqs_config)
        client.connect()
        client.disconnect()
        
        assert client.connection is None
    
    @patch('src.clients.sqs_client.boto3.client')
    def test_execute_query_success(self, mock_boto_client, sample_sqs_config):
        mock_client = Mock()
        mock_client.receive_message.return_value = {
            'Messages': [
                {
                    'MessageId': 'msg-1',
                    'ReceiptHandle': 'handle-1',
                    'Body': '{"event": "test1"}',
                    'Attributes': {'SentTimestamp': '1234567890'},
                    'MessageAttributes': {'type': {'StringValue': 'event'}}
                },
                {
                    'MessageId': 'msg-2',
                    'ReceiptHandle': 'handle-2',
                    'Body': '{"event": "test2"}',
                    'Attributes': {},
                    'MessageAttributes': {}
                }
            ]
        }
        mock_boto_client.return_value = mock_client
        
        client = SQSClient(sample_sqs_config)
        client.connect()
        
        results = client.execute_query('', {'max_messages': 10, 'wait_time': 5})
        
        mock_client.receive_message.assert_called_once_with(
            QueueUrl='https://sqs.us-east-1.amazonaws.com/123456789012/test-queue',
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5
        )
        assert len(results) == 2
        assert results[0]['MessageId'] == 'msg-1'
        assert results[0]['Body'] == '{"event": "test1"}'
    
    @patch('src.clients.sqs_client.boto3.client')
    def test_execute_query_no_messages(self, mock_boto_client, sample_sqs_config):
        mock_client = Mock()
        mock_client.receive_message.return_value = {}
        mock_boto_client.return_value = mock_client
        
        client = SQSClient(sample_sqs_config)
        client.connect()
        
        results = client.execute_query('')
        
        assert len(results) == 0
    
    @patch('src.clients.sqs_client.boto3.client')
    def test_send_message(self, mock_boto_client, sample_sqs_config):
        mock_client = Mock()
        mock_client.send_message.return_value = {'MessageId': 'msg-123'}
        mock_boto_client.return_value = mock_client
        
        client = SQSClient(sample_sqs_config)
        client.connect()
        
        response = client.send_message('Test message body')
        
        mock_client.send_message.assert_called_once_with(
            QueueUrl='https://sqs.us-east-1.amazonaws.com/123456789012/test-queue',
            MessageBody='Test message body'
        )
        assert response['MessageId'] == 'msg-123'
    
    @patch('src.clients.sqs_client.boto3.client')
    def test_send_message_with_attributes(self, mock_boto_client, sample_sqs_config):
        mock_client = Mock()
        mock_client.send_message.return_value = {'MessageId': 'msg-123'}
        mock_boto_client.return_value = mock_client
        
        client = SQSClient(sample_sqs_config)
        client.connect()
        
        attributes = {'type': {'StringValue': 'event', 'DataType': 'String'}}
        response = client.send_message('Test message', message_attributes=attributes)
        
        mock_client.send_message.assert_called_once_with(
            QueueUrl='https://sqs.us-east-1.amazonaws.com/123456789012/test-queue',
            MessageBody='Test message',
            MessageAttributes=attributes
        )
    
    @patch('src.clients.sqs_client.boto3.client')
    def test_delete_message(self, mock_boto_client, sample_sqs_config):
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        client = SQSClient(sample_sqs_config)
        client.connect()
        
        client.delete_message('receipt-handle-123')
        
        mock_client.delete_message.assert_called_once_with(
            QueueUrl='https://sqs.us-east-1.amazonaws.com/123456789012/test-queue',
            ReceiptHandle='receipt-handle-123'
        )
    
    def test_execute_query_not_connected(self, sample_sqs_config):
        client = SQSClient(sample_sqs_config)
        
        with pytest.raises(ConnectionError, match="Client not connected"):
            client.execute_query('')
    
    @patch('src.clients.sqs_client.boto3.client')
    def test_is_connected(self, mock_boto_client, sample_sqs_config):
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        client = SQSClient(sample_sqs_config)
        assert not client.is_connected()
        
        client.connect()
        assert client.is_connected()
        
        client.disconnect()
        assert not client.is_connected()
