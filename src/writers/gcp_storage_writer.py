"""
Google Cloud Storage Writer for Data Ingestion
"""

import json
from typing import Any, Dict, List
from datetime import datetime

try:
    from google.cloud import storage
    from google.cloud.exceptions import GoogleCloudError
except ImportError:
    storage = None
    GoogleCloudError = Exception

from .base_writer import BaseWriter
from ..exceptions import ConnectionError as ClientConnectionError


class GCPStorageWriter(BaseWriter):
    """Writer for Google Cloud Storage."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.bucket_name = config.get('bucket')
        self.prefix = config.get('prefix', '')
        self.project_id = config.get('project_id')
        self.credentials_path = config.get('credentials_path')
        self.format = config.get('format', 'json')
        self.client = None
        self.bucket = None
        
        if not self.bucket_name:
            raise ValueError("Bucket name is required")
        
        self.logger.info(f"GCPStorageWriter initialized for bucket: {self.bucket_name}")
    
    def connect(self) -> None:
        """Connect to Google Cloud Storage."""
        try:
            if storage is None:
                raise ImportError("google-cloud-storage is not installed. Install it with: pip install google-cloud-storage")
            
            self.logger.debug(f"Connecting to GCS bucket: {self.bucket_name}")
            
            # Create client
            if self.credentials_path:
                self.client = storage.Client.from_service_account_json(
                    self.credentials_path,
                    project=self.project_id
                )
            else:
                self.client = storage.Client(project=self.project_id)
            
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Test connection
            self.bucket.exists()
            
            self.logger.info(f"Successfully connected to GCS bucket: {self.bucket_name}")
            
        except GoogleCloudError as e:
            self.logger.error(f"Failed to connect to GCS: {e}", exc_info=True)
            raise ClientConnectionError(f"Failed to connect to GCS: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to GCS: {e}", exc_info=True)
            raise ClientConnectionError(f"Unexpected GCS connection error: {e}")
    
    def disconnect(self) -> None:
        """Disconnect from GCS."""
        if self.client:
            self.logger.debug("Disconnecting from GCS")
            self.client.close()
            self.client = None
            self.bucket = None
            self.logger.info("Disconnected from GCS")
    
    def write(self, data: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """Write data to Google Cloud Storage."""
        if not self.bucket:
            raise ClientConnectionError("Not connected. Call connect() first.")
        
        try:
            blob_name = kwargs.get('blob_name')
            
            if not blob_name:
                timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                extension = 'json' if self.format == 'json' else 'jsonl'
                blob_name = f"{self.prefix}data_{timestamp}.{extension}"
            else:
                blob_name = f"{self.prefix}{blob_name}"
            
            self.logger.debug(f"Writing {len(data)} records to blob: {blob_name}")
            
            # Format data
            if self.format == 'json':
                content = json.dumps(data, indent=2, default=str)
            elif self.format == 'jsonl':
                lines = [json.dumps(record, default=str) for record in data]
                content = '\n'.join(lines)
            else:
                content = json.dumps(data, default=str)
            
            # Upload blob
            blob = self.bucket.blob(blob_name)
            blob.upload_from_string(content, content_type='application/json')
            
            gcs_uri = f"gs://{self.bucket_name}/{blob_name}"
            
            self.logger.info(f"Successfully wrote {len(data)} records to GCS: {blob_name}")
            
            return {
                'success': True,
                'records_written': len(data),
                'gcs_uri': gcs_uri,
                'bucket': self.bucket_name,
                'blob_name': blob_name
            }
            
        except GoogleCloudError as e:
            self.logger.error(f"GCS write error: {e}", exc_info=True)
            return {
                'success': False,
                'records_written': 0,
                'error': str(e)
            }
        except Exception as e:
            self.logger.error(f"Unexpected error writing to GCS: {e}", exc_info=True)
            return {
                'success': False,
                'records_written': 0,
                'error': str(e)
            }
    
    def is_connected(self) -> bool:
        """Check if connected."""
        return self.bucket is not None
