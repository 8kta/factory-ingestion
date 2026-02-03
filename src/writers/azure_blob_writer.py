"""
Azure Blob Storage Writer for Data Ingestion
"""

import json
from typing import Any, Dict, List
from datetime import datetime

try:
    from azure.storage.blob import BlobServiceClient
    from azure.core.exceptions import AzureError
except ImportError:
    BlobServiceClient = None
    AzureError = Exception

from .base_writer import BaseWriter
from ..exceptions import ConnectionError as ClientConnectionError


class AzureBlobWriter(BaseWriter):
    """Writer for Azure Blob Storage."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connection_string = config.get('connection_string')
        self.account_url = config.get('account_url')
        self.container = config.get('container')
        self.prefix = config.get('prefix', '')
        self.format = config.get('format', 'json')
        self.client = None
        self.container_client = None
        
        if not self.container:
            raise ValueError("Container name is required")
        
        self.logger.info(f"AzureBlobWriter initialized for container: {self.container}")
    
    def connect(self) -> None:
        """Connect to Azure Blob Storage."""
        try:
            if BlobServiceClient is None:
                raise ImportError("azure-storage-blob is not installed. Install it with: pip install azure-storage-blob")
            
            self.logger.debug(f"Connecting to Azure Blob Storage container: {self.container}")
            
            if self.connection_string:
                self.client = BlobServiceClient.from_connection_string(self.connection_string)
            elif self.account_url:
                self.client = BlobServiceClient(account_url=self.account_url)
            else:
                raise ValueError("Either connection_string or account_url is required")
            
            self.container_client = self.client.get_container_client(self.container)
            
            # Test connection
            self.container_client.get_container_properties()
            
            self.logger.info(f"Successfully connected to Azure Blob container: {self.container}")
            
        except AzureError as e:
            self.logger.error(f"Failed to connect to Azure Blob Storage: {e}", exc_info=True)
            raise ClientConnectionError(f"Failed to connect to Azure Blob Storage: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to Azure Blob: {e}", exc_info=True)
            raise ClientConnectionError(f"Unexpected Azure Blob connection error: {e}")
    
    def disconnect(self) -> None:
        """Disconnect from Azure Blob Storage."""
        if self.client:
            self.logger.debug("Disconnecting from Azure Blob Storage")
            self.client.close()
            self.client = None
            self.container_client = None
            self.logger.info("Disconnected from Azure Blob Storage")
    
    def write(self, data: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """Write data to Azure Blob Storage."""
        if not self.container_client:
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
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_client.upload_blob(content, overwrite=True)
            
            blob_url = f"https://{self.container_client.account_name}.blob.core.windows.net/{self.container}/{blob_name}"
            
            self.logger.info(f"Successfully wrote {len(data)} records to Azure Blob: {blob_name}")
            
            return {
                'success': True,
                'records_written': len(data),
                'blob_url': blob_url,
                'container': self.container,
                'blob_name': blob_name
            }
            
        except AzureError as e:
            self.logger.error(f"Azure Blob write error: {e}", exc_info=True)
            return {
                'success': False,
                'records_written': 0,
                'error': str(e)
            }
        except Exception as e:
            self.logger.error(f"Unexpected error writing to Azure Blob: {e}", exc_info=True)
            return {
                'success': False,
                'records_written': 0,
                'error': str(e)
            }
    
    def is_connected(self) -> bool:
        """Check if connected."""
        return self.container_client is not None
