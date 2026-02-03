import pytest
from unittest.mock import Mock, patch, MagicMock
from src.clients.kafka_client import KafkaClient


class TestKafkaClient:
    
    @patch('src.clients.kafka_client.KafkaProducer')
    @patch('src.clients.kafka_client.KafkaConsumer')
    def test_connect_success(self, mock_consumer_class, mock_producer_class, sample_kafka_config):
        mock_consumer = Mock()
        mock_producer = Mock()
        mock_consumer_class.return_value = mock_consumer
        mock_producer_class.return_value = mock_producer
        
        client = KafkaClient(sample_kafka_config)
        client.connect()
        
        mock_consumer_class.assert_called_once()
        mock_producer_class.assert_called_once()
        assert client.consumer == mock_consumer
        assert client.producer == mock_producer
        assert client.connection == mock_consumer
    
    @patch('src.clients.kafka_client.KafkaProducer')
    @patch('src.clients.kafka_client.KafkaConsumer')
    def test_disconnect(self, mock_consumer_class, mock_producer_class, sample_kafka_config):
        mock_consumer = Mock()
        mock_producer = Mock()
        mock_consumer_class.return_value = mock_consumer
        mock_producer_class.return_value = mock_producer
        
        client = KafkaClient(sample_kafka_config)
        client.connect()
        client.disconnect()
        
        mock_consumer.close.assert_called_once()
        mock_producer.close.assert_called_once()
        assert client.consumer is None
        assert client.producer is None
    
    @patch('src.clients.kafka_client.KafkaProducer')
    @patch('src.clients.kafka_client.KafkaConsumer')
    def test_execute_query_success(self, mock_consumer_class, mock_producer_class, sample_kafka_config):
        mock_message1 = Mock()
        mock_message1.topic = 'test_topic'
        mock_message1.partition = 0
        mock_message1.offset = 100
        mock_message1.key = b'key1'
        mock_message1.value = {'event': 'user_login', 'user_id': 123}
        mock_message1.timestamp = 1234567890
        
        mock_message2 = Mock()
        mock_message2.topic = 'test_topic'
        mock_message2.partition = 0
        mock_message2.offset = 101
        mock_message2.key = None
        mock_message2.value = {'event': 'user_logout', 'user_id': 123}
        mock_message2.timestamp = 1234567900
        
        mock_consumer = Mock()
        mock_consumer.__iter__ = Mock(return_value=iter([mock_message1, mock_message2]))
        mock_producer = Mock()
        
        mock_consumer_class.return_value = mock_consumer
        mock_producer_class.return_value = mock_producer
        
        client = KafkaClient(sample_kafka_config)
        client.connect()
        
        results = client.execute_query('', {'max_messages': 2})
        
        assert len(results) == 2
        assert results[0]['topic'] == 'test_topic'
        assert results[0]['offset'] == 100
        assert results[0]['key'] == 'key1'
        assert results[1]['key'] is None
    
    @patch('src.clients.kafka_client.KafkaProducer')
    @patch('src.clients.kafka_client.KafkaConsumer')
    def test_send_message(self, mock_consumer_class, mock_producer_class, sample_kafka_config):
        mock_consumer = Mock()
        mock_producer = Mock()
        mock_consumer_class.return_value = mock_consumer
        mock_producer_class.return_value = mock_producer
        
        client = KafkaClient(sample_kafka_config)
        client.connect()
        
        message = {'event': 'test', 'data': 'example'}
        client.send_message('test_topic', message, key='test_key')
        
        mock_producer.send.assert_called_once()
        mock_producer.flush.assert_called_once()
    
    def test_execute_query_not_connected(self, sample_kafka_config):
        client = KafkaClient(sample_kafka_config)
        
        with pytest.raises(ConnectionError, match="Client not connected"):
            client.execute_query('')
    
    def test_send_message_not_connected(self, sample_kafka_config):
        client = KafkaClient(sample_kafka_config)
        
        with pytest.raises(ConnectionError, match="Producer not connected"):
            client.send_message('topic', {})
    
    @patch('src.clients.kafka_client.KafkaProducer')
    @patch('src.clients.kafka_client.KafkaConsumer')
    def test_is_connected(self, mock_consumer_class, mock_producer_class, sample_kafka_config):
        mock_consumer = Mock()
        mock_producer = Mock()
        mock_consumer_class.return_value = mock_consumer
        mock_producer_class.return_value = mock_producer
        
        client = KafkaClient(sample_kafka_config)
        assert not client.is_connected()
        
        client.connect()
        assert client.is_connected()
        
        client.disconnect()
        assert not client.is_connected()
