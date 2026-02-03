from kafka import KafkaConsumer, KafkaProducer
import json
from typing import Any, Dict, List, Optional
from .base_client import BaseClient


class KafkaClient(BaseClient):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.bootstrap_servers = config.get('bootstrap_servers', ['localhost:9092'])
        self.topic = config.get('topic')
        self.group_id = config.get('group_id', 'default-group')
        self.auto_offset_reset = config.get('auto_offset_reset', 'earliest')
        self.consumer = None
        self.producer = None
    
    def connect(self) -> None:
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.connection = self.consumer
    
    def disconnect(self) -> None:
        if self.consumer:
            self.consumer.close()
            self.consumer = None
        if self.producer:
            self.producer.close()
            self.producer = None
        self.connection = None
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        if not self.consumer:
            raise ConnectionError("Client not connected. Call connect() first.")
        
        max_messages = params.get('max_messages', 100) if params else 100
        timeout_ms = params.get('timeout_ms', 1000) if params else 1000
        
        messages = []
        for message in self.consumer:
            messages.append({
                'topic': message.topic,
                'partition': message.partition,
                'offset': message.offset,
                'key': message.key.decode('utf-8') if message.key else None,
                'value': message.value,
                'timestamp': message.timestamp
            })
            
            if len(messages) >= max_messages:
                break
        
        return messages
    
    def send_message(self, topic: str, message: Dict[str, Any], key: Optional[str] = None) -> None:
        if not self.producer:
            raise ConnectionError("Producer not connected. Call connect() first.")
        
        key_bytes = key.encode('utf-8') if key else None
        self.producer.send(topic, value=message, key=key_bytes)
        self.producer.flush()
    
    def is_connected(self) -> bool:
        return self.consumer is not None
