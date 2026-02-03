"""
AWS S3 Writer for Data Ingestion
"""

import json
import gzip
from typing import Any, Dict, List, Optional
from datetime import datetime
from io import BytesIO

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None
    ClientError = Exception

from .base_writer import BaseWriter
from ..exceptions import ConnectionError as ClientConnectionError, ValidationError


class S3Writer(BaseWriter):
    """Writer for AWS S3."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.bucket = config.get('bucket')
        self.prefix = config.get('prefix', '')
        self.region = config.get('region_name', 'us-east-1')
        self.aws_access_key = config.get('aws_access_key_id')
        self.aws_secret_key = config.get('aws_secret_access_key')
        self.format = config.get('format', 'json')  # json, jsonl, csv
        self.compression = config.get('compression', None)  # gzip, None
        self.client = None
        
        if not self.bucket:
            raise ValidationError("S3 bucket name is required")
        
        self.logger.info(f"S3Writer initialized for bucket: {self.bucket}")
    
    def connect(self) -> None:
        """Connect to S3."""
        try:
            if boto3 is None:
                raise ImportError("boto3 is not installed. Install it with: pip install boto3")
            
            self.logger.debug(f"Connecting to S3 bucket: {self.bucket}")
            
            session_config = {
                'region_name': self.region
            }
            
            if self.aws_access_key and self.aws_secret_key:
                session_config['aws_access_key_id'] = self.aws_access_key
                session_config['aws_secret_access_key'] = self.aws_secret_key
            
            self.client = boto3.client('s3', **session_config)
            
            # Test connection by checking if bucket exists
            self.client.head_bucket(Bucket=self.bucket)
            
            self.logger.info(f"Successfully connected to S3 bucket: {self.bucket}")
            
        except ClientError as e:
            self.logger.error(f"Failed to connect to S3: {e}", exc_info=True)
            raise ClientConnectionError(f"Failed to connect to S3 bucket {self.bucket}: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to S3: {e}", exc_info=True)
            raise ClientConnectionError(f"Unexpected S3 connection error: {e}")
    
    def disconnect(self) -> None:
        """Disconnect from S3."""
        if self.client:
            self.logger.debug("Disconnecting from S3")
            self.client = None
            self.logger.info("Disconnected from S3")
    
    def write(self, data: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """
        Write data to S3.
        
        Args:
            data: List of dictionaries to write
            **kwargs: Additional parameters
                - key: Custom S3 key (optional, auto-generated if not provided)
                - partition_by: Field to partition by (optional)
                
        Returns:
            Dictionary with write results
        """
        if not self.client:
            raise ClientConnectionError("Not connected to S3. Call connect() first.")
        
        try:
            key = kwargs.get('key')
            partition_by = kwargs.get('partition_by')
            
            # Auto-generate key if not provided
            if not key:
                timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                extension = self._get_extension()
                key = f"{self.prefix}data_{timestamp}.{extension}"
            else:
                key = f"{self.prefix}{key}"
            
            self.logger.debug(f"Writing {len(data)} records to S3 key: {key}")
            
            # Convert data to appropriate format
            content = self._format_data(data)
            
            # Compress if needed
            if self.compression == 'gzip':
                content = self._compress_gzip(content)
                if not key.endswith('.gz'):
                    key += '.gz'
            
            # Upload to S3
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=content,
                ContentType=self._get_content_type()
            )
            
            s3_uri = f"s3://{self.bucket}/{key}"
            self.logger.info(f"Successfully wrote {len(data)} records to {s3_uri}")
            
            return {
                'success': True,
                'records_written': len(data),
                's3_uri': s3_uri,
                'bucket': self.bucket,
                'key': key,
                'size_bytes': len(content)
            }
            
        except ClientError as e:
            self.logger.error(f"S3 write error: {e}", exc_info=True)
            return {
                'success': False,
                'records_written': 0,
                'error': str(e)
            }
        except Exception as e:
            self.logger.error(f"Unexpected error writing to S3: {e}", exc_info=True)
            return {
                'success': False,
                'records_written': 0,
                'error': str(e)
            }
    
    def _format_data(self, data: List[Dict[str, Any]]) -> bytes:
        """Format data according to specified format."""
        if self.format == 'json':
            # Pretty JSON
            content = json.dumps(data, indent=2, default=str)
        elif self.format == 'jsonl':
            # JSON Lines (one JSON object per line)
            lines = [json.dumps(record, default=str) for record in data]
            content = '\n'.join(lines)
        elif self.format == 'csv':
            # CSV format
            import csv
            from io import StringIO
            
            if not data:
                return b''
            
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
            content = output.getvalue()
        else:
            raise ValidationError(f"Unsupported format: {self.format}")
        
        return content.encode('utf-8')
    
    def _compress_gzip(self, content: bytes) -> bytes:
        """Compress content with gzip."""
        buffer = BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='wb') as gz:
            gz.write(content)
        return buffer.getvalue()
    
    def _get_extension(self) -> str:
        """Get file extension based on format."""
        extensions = {
            'json': 'json',
            'jsonl': 'jsonl',
            'csv': 'csv'
        }
        return extensions.get(self.format, 'json')
    
    def _get_content_type(self) -> str:
        """Get content type based on format."""
        content_types = {
            'json': 'application/json',
            'jsonl': 'application/x-ndjson',
            'csv': 'text/csv'
        }
        return content_types.get(self.format, 'application/json')
    
    def is_connected(self) -> bool:
        """Check if connected to S3."""
        return self.client is not None
