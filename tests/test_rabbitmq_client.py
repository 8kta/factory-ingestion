import pytest
from unittest.mock import Mock, patch, MagicMock
from src.clients.rabbitmq_client import RabbitMQClient


class TestRabbitMQClient:
    
    @patch('src.clients.rabbitmq_client.pika.BlockingConnection')
    @patch('src.clients.rabbitmq_client.pika.ConnectionParameters')
    @patch('src.clients.rabbitmq_client.pika.PlainCredentials')
    def test_connect_success(self, mock_credentials, mock_params, mock_connection_class, sample_rabbitmq_config):
        mock_channel = Mock()
        mock_connection = Mock()
        mock_connection.channel.return_value = mock_channel
        mock_connection_class.return_value = mock_connection
        
        client = RabbitMQClient(sample_rabbitmq_config)
        client.connect()
        
        mock_credentials.assert_called_once_with('guest', 'guest')
        mock_channel.queue_declare.assert_called_once_with(queue='test_queue', durable=True)
        assert client.connection == mock_connection
        assert client.channel == mock_channel
    
    @patch('src.clients.rabbitmq_client.pika.BlockingConnection')
    @patch('src.clients.rabbitmq_client.pika.ConnectionParameters')
    @patch('src.clients.rabbitmq_client.pika.PlainCredentials')
    def test_disconnect(self, mock_credentials, mock_params, mock_connection_class, sample_rabbitmq_config):
        mock_channel = Mock()
        mock_connection = Mock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_closed = False
        mock_connection_class.return_value = mock_connection
        
        client = RabbitMQClient(sample_rabbitmq_config)
        client.connect()
        client.disconnect()
        
        mock_connection.close.assert_called_once()
        assert client.connection is None
        assert client.channel is None
    
    @patch('src.clients.rabbitmq_client.pika.BlockingConnection')
    @patch('src.clients.rabbitmq_client.pika.ConnectionParameters')
    @patch('src.clients.rabbitmq_client.pika.PlainCredentials')
    def test_execute_query_success(self, mock_credentials, mock_params, mock_connection_class, sample_rabbitmq_config):
        mock_method1 = Mock()
        mock_method1.delivery_tag = 1
        mock_method1.exchange = ''
        mock_method1.routing_key = 'test_queue'
        
        mock_method2 = Mock()
        mock_method2.delivery_tag = 2
        mock_method2.exchange = ''
        mock_method2.routing_key = 'test_queue'
        
        mock_header1 = Mock()
        mock_header1.headers = {'type': 'event'}
        
        mock_header2 = Mock()
        mock_header2.headers = None
        
        mock_channel = Mock()
        mock_channel.basic_get.side_effect = [
            (mock_method1, mock_header1, b'{"event": "test1"}'),
            (mock_method2, mock_header2, b'{"event": "test2"}'),
            (None, None, None)
        ]
        
        mock_connection = Mock()
        mock_connection.channel.return_value = mock_channel
        mock_connection_class.return_value = mock_connection
        
        client = RabbitMQClient(sample_rabbitmq_config)
        client.connect()
        
        results = client.execute_query('', {'max_messages': 3})
        
        assert len(results) == 2
        assert results[0]['delivery_tag'] == 1
        assert results[0]['body'] == {'event': 'test1'}
        assert results[0]['headers'] == {'type': 'event'}
        assert results[1]['headers'] is None or results[1]['headers'] == {}
    
    @patch('src.clients.rabbitmq_client.pika.BlockingConnection')
    @patch('src.clients.rabbitmq_client.pika.ConnectionParameters')
    @patch('src.clients.rabbitmq_client.pika.PlainCredentials')
    def test_publish_message(self, mock_credentials, mock_params, mock_connection_class, sample_rabbitmq_config):
        mock_channel = Mock()
        mock_connection = Mock()
        mock_connection.channel.return_value = mock_channel
        mock_connection_class.return_value = mock_connection
        
        client = RabbitMQClient(sample_rabbitmq_config)
        client.connect()
        
        message = {'event': 'test', 'data': 'example'}
        client.publish_message(message, routing_key='test_queue')
        
        mock_channel.basic_publish.assert_called_once()
    
    def test_execute_query_not_connected(self, sample_rabbitmq_config):
        client = RabbitMQClient(sample_rabbitmq_config)
        
        with pytest.raises(ConnectionError, match="Client not connected"):
            client.execute_query('')
    
    def test_publish_message_not_connected(self, sample_rabbitmq_config):
        client = RabbitMQClient(sample_rabbitmq_config)
        
        with pytest.raises(ConnectionError, match="Client not connected"):
            client.publish_message({})
    
    @patch('src.clients.rabbitmq_client.pika.BlockingConnection')
    @patch('src.clients.rabbitmq_client.pika.ConnectionParameters')
    @patch('src.clients.rabbitmq_client.pika.PlainCredentials')
    def test_is_connected(self, mock_credentials, mock_params, mock_connection_class, sample_rabbitmq_config):
        mock_channel = Mock()
        mock_connection = Mock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_closed = False
        mock_connection_class.return_value = mock_connection
        
        client = RabbitMQClient(sample_rabbitmq_config)
        assert not client.is_connected()
        
        client.connect()
        assert client.is_connected()
        
        mock_connection.is_closed = True
        assert not client.is_connected()
