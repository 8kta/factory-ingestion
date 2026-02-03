import boto3
from typing import Any, Dict, List, Optional
from .base_client import BaseClient


class SQSClient(BaseClient):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.region_name = config.get('region_name', 'us-east-1')
        self.queue_url = config.get('queue_url')
    
    def connect(self) -> None:
        self.connection = boto3.client(
            'sqs',
            region_name=self.region_name,
            aws_access_key_id=self.config.get('aws_access_key_id'),
            aws_secret_access_key=self.config.get('aws_secret_access_key')
        )
    
    def disconnect(self) -> None:
        self.connection = None
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        if not self.connection:
            raise ConnectionError("Client not connected. Call connect() first.")
        
        max_messages = params.get('max_messages', 10) if params else 10
        wait_time = params.get('wait_time', 0) if params else 0
        
        response = self.connection.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time
        )
        
        messages = []
        for msg in response.get('Messages', []):
            messages.append({
                'MessageId': msg['MessageId'],
                'ReceiptHandle': msg['ReceiptHandle'],
                'Body': msg['Body'],
                'Attributes': msg.get('Attributes', {}),
                'MessageAttributes': msg.get('MessageAttributes', {})
            })
        
        return messages
    
    def send_message(self, message_body: str, message_attributes: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if not self.connection:
            raise ConnectionError("Client not connected. Call connect() first.")
        
        params = {
            'QueueUrl': self.queue_url,
            'MessageBody': message_body
        }
        
        if message_attributes:
            params['MessageAttributes'] = message_attributes
        
        response = self.connection.send_message(**params)
        return response
    
    def delete_message(self, receipt_handle: str) -> None:
        if not self.connection:
            raise ConnectionError("Client not connected. Call connect() first.")
        
        self.connection.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle
        )
    
    def is_connected(self) -> bool:
        return self.connection is not None
