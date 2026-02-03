import pytest
from unittest.mock import Mock, patch
from src.clients.sns_client import SNSClient


class TestSNSClient:
    
    @patch('src.clients.sns_client.boto3.client')
    def test_connect_success(self, mock_boto_client, sample_sns_config):
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        client = SNSClient(sample_sns_config)
        client.connect()
        
        mock_boto_client.assert_called_once_with(
            'sns',
            region_name='us-east-1',
            aws_access_key_id='test_key',
            aws_secret_access_key='test_secret'
        )
        assert client.connection == mock_client
    
    @patch('src.clients.sns_client.boto3.client')
    def test_disconnect(self, mock_boto_client, sample_sns_config):
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        client = SNSClient(sample_sns_config)
        client.connect()
        client.disconnect()
        
        assert client.connection is None
    
    @patch('src.clients.sns_client.boto3.client')
    def test_execute_query_success(self, mock_boto_client, sample_sns_config):
        mock_client = Mock()
        mock_client.list_subscriptions_by_topic.return_value = {
            'Subscriptions': [
                {
                    'SubscriptionArn': 'arn:aws:sns:us-east-1:123456789012:test-topic:sub-1',
                    'Endpoint': 'email@example.com',
                    'Protocol': 'email'
                },
                {
                    'SubscriptionArn': 'arn:aws:sns:us-east-1:123456789012:test-topic:sub-2',
                    'Endpoint': 'https://example.com/webhook',
                    'Protocol': 'https'
                }
            ]
        }
        mock_boto_client.return_value = mock_client
        
        client = SNSClient(sample_sns_config)
        client.connect()
        
        results = client.execute_query('')
        
        mock_client.list_subscriptions_by_topic.assert_called_once_with(
            TopicArn='arn:aws:sns:us-east-1:123456789012:test-topic'
        )
        assert len(results) == 2
        assert results[0]['Protocol'] == 'email'
        assert results[1]['Protocol'] == 'https'
    
    @patch('src.clients.sns_client.boto3.client')
    def test_publish_message(self, mock_boto_client, sample_sns_config):
        mock_client = Mock()
        mock_client.publish.return_value = {'MessageId': 'msg-123'}
        mock_boto_client.return_value = mock_client
        
        client = SNSClient(sample_sns_config)
        client.connect()
        
        response = client.publish_message('Test notification')
        
        mock_client.publish.assert_called_once_with(
            TopicArn='arn:aws:sns:us-east-1:123456789012:test-topic',
            Message='Test notification'
        )
        assert response['MessageId'] == 'msg-123'
    
    @patch('src.clients.sns_client.boto3.client')
    def test_publish_message_with_subject(self, mock_boto_client, sample_sns_config):
        mock_client = Mock()
        mock_client.publish.return_value = {'MessageId': 'msg-123'}
        mock_boto_client.return_value = mock_client
        
        client = SNSClient(sample_sns_config)
        client.connect()
        
        response = client.publish_message('Test notification', subject='Test Subject')
        
        mock_client.publish.assert_called_once_with(
            TopicArn='arn:aws:sns:us-east-1:123456789012:test-topic',
            Message='Test notification',
            Subject='Test Subject'
        )
    
    @patch('src.clients.sns_client.boto3.client')
    def test_publish_message_with_attributes(self, mock_boto_client, sample_sns_config):
        mock_client = Mock()
        mock_client.publish.return_value = {'MessageId': 'msg-123'}
        mock_boto_client.return_value = mock_client
        
        client = SNSClient(sample_sns_config)
        client.connect()
        
        attributes = {'severity': {'DataType': 'String', 'StringValue': 'high'}}
        response = client.publish_message('Alert', message_attributes=attributes)
        
        mock_client.publish.assert_called_once_with(
            TopicArn='arn:aws:sns:us-east-1:123456789012:test-topic',
            Message='Alert',
            MessageAttributes=attributes
        )
    
    def test_execute_query_not_connected(self, sample_sns_config):
        client = SNSClient(sample_sns_config)
        
        with pytest.raises(ConnectionError, match="Client not connected"):
            client.execute_query('')
    
    def test_publish_message_not_connected(self, sample_sns_config):
        client = SNSClient(sample_sns_config)
        
        with pytest.raises(ConnectionError, match="Client not connected"):
            client.publish_message('Test')
    
    @patch('src.clients.sns_client.boto3.client')
    def test_is_connected(self, mock_boto_client, sample_sns_config):
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        client = SNSClient(sample_sns_config)
        assert not client.is_connected()
        
        client.connect()
        assert client.is_connected()
        
        client.disconnect()
        assert not client.is_connected()
