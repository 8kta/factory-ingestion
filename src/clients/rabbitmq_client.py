import pika
import json
from typing import Any, Dict, List, Optional
from .base_client import BaseClient


class RabbitMQClient(BaseClient):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 5672)
        self.username = config.get('username', 'guest')
        self.password = config.get('password', 'guest')
        self.virtual_host = config.get('virtual_host', '/')
        self.queue = config.get('queue')
        self.exchange = config.get('exchange', '')
        self.channel = None
    
    def connect(self) -> None:
        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.virtual_host,
            credentials=credentials
        )
        
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        
        if self.queue:
            self.channel.queue_declare(queue=self.queue, durable=True)
    
    def disconnect(self) -> None:
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            self.connection = None
            self.channel = None
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        if not self.channel:
            raise ConnectionError("Client not connected. Call connect() first.")
        
        max_messages = params.get('max_messages', 10) if params else 10
        messages = []
        
        for _ in range(max_messages):
            method_frame, header_frame, body = self.channel.basic_get(queue=self.queue)
            
            if method_frame:
                try:
                    message_data = json.loads(body.decode('utf-8'))
                except:
                    message_data = body.decode('utf-8')
                
                messages.append({
                    'delivery_tag': method_frame.delivery_tag,
                    'exchange': method_frame.exchange,
                    'routing_key': method_frame.routing_key,
                    'body': message_data,
                    'headers': header_frame.headers if header_frame else {}
                })
                
                self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            else:
                break
        
        return messages
    
    def publish_message(self, message: Dict[str, Any], routing_key: Optional[str] = None) -> None:
        if not self.channel:
            raise ConnectionError("Client not connected. Call connect() first.")
        
        routing_key = routing_key or self.queue
        
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=routing_key,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,
            )
        )
    
    def is_connected(self) -> bool:
        return self.connection is not None and not self.connection.is_closed
