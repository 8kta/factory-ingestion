import boto3
from typing import Any, Dict, List, Optional
from .base_client import BaseClient


class SNSClient(BaseClient):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.region_name = config.get('region_name', 'us-east-1')
        self.topic_arn = config.get('topic_arn')
    
    def connect(self) -> None:
        self.connection = boto3.client(
            'sns',
            region_name=self.region_name,
            aws_access_key_id=self.config.get('aws_access_key_id'),
            aws_secret_access_key=self.config.get('aws_secret_access_key')
        )
    
    def disconnect(self) -> None:
        self.connection = None
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        if not self.connection:
            raise ConnectionError("Client not connected. Call connect() first.")
        
        response = self.connection.list_subscriptions_by_topic(TopicArn=self.topic_arn)
        
        return response.get('Subscriptions', [])
    
    def publish_message(self, message: str, subject: Optional[str] = None, 
                       message_attributes: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if not self.connection:
            raise ConnectionError("Client not connected. Call connect() first.")
        
        params = {
            'TopicArn': self.topic_arn,
            'Message': message
        }
        
        if subject:
            params['Subject'] = subject
        
        if message_attributes:
            params['MessageAttributes'] = message_attributes
        
        response = self.connection.publish(**params)
        return response
    
    def is_connected(self) -> bool:
        return self.connection is not None
