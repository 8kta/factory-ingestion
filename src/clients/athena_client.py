import boto3
from typing import Any, Dict, List, Optional
from .base_client import BaseClient


class AthenaClient(BaseClient):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.client = None
        self.s3_output_location = config.get('s3_output_location')
        self.database = config.get('database')
        self.region_name = config.get('region_name', 'us-east-1')
    
    def connect(self) -> None:
        self.client = boto3.client(
            'athena',
            region_name=self.region_name,
            aws_access_key_id=self.config.get('aws_access_key_id'),
            aws_secret_access_key=self.config.get('aws_secret_access_key')
        )
    
    def disconnect(self) -> None:
        self.client = None
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        if not self.client:
            raise ConnectionError("Client not connected. Call connect() first.")
        
        response = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': self.database},
            ResultConfiguration={'OutputLocation': self.s3_output_location}
        )
        
        query_execution_id = response['QueryExecutionId']
        
        while True:
            query_status = self.client.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_status['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
        
        if status == 'SUCCEEDED':
            results = self.client.get_query_results(QueryExecutionId=query_execution_id)
            columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            rows = []
            
            for row in results['ResultSet']['Rows'][1:]:
                row_data = {}
                for i, col in enumerate(columns):
                    row_data[col] = row['Data'][i].get('VarCharValue', None)
                rows.append(row_data)
            
            return rows
        else:
            raise Exception(f"Query failed with status: {status}")
    
    def is_connected(self) -> bool:
        return self.client is not None
