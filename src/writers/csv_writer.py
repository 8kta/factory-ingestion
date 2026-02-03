"""
CSV Writer for Data Ingestion
"""

import csv
from typing import Any, Dict, List
from pathlib import Path

from .base_writer import BaseWriter
from ..exceptions import ValidationError


class CSVWriter(BaseWriter):
    """Writer for CSV files."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.file_path = config.get('file_path')
        self.mode = config.get('mode', 'w')  # w=write, a=append
        self.delimiter = config.get('delimiter', ',')
        self.include_header = config.get('include_header', True)
        self.file_handle = None
        
        if not self.file_path:
            raise ValidationError("file_path is required for CSVWriter")
        
        self.logger.info(f"CSVWriter initialized for file: {self.file_path}")
    
    def connect(self) -> None:
        """Open CSV file for writing."""
        try:
            self.logger.debug(f"Opening CSV file: {self.file_path}")
            
            # Create parent directories if they don't exist
            Path(self.file_path).parent.mkdir(parents=True, exist_ok=True)
            
            self.file_handle = open(self.file_path, self.mode, newline='', encoding='utf-8')
            self.logger.info(f"Successfully opened CSV file: {self.file_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to open CSV file: {e}", exc_info=True)
            raise
    
    def disconnect(self) -> None:
        """Close CSV file."""
        if self.file_handle:
            self.logger.debug("Closing CSV file")
            self.file_handle.close()
            self.file_handle = None
            self.logger.info("CSV file closed")
    
    def write(self, data: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """Write data to CSV file."""
        if not self.file_handle:
            raise ValidationError("CSV file not open. Call connect() first.")
        
        try:
            if not data:
                return {'success': True, 'records_written': 0}
            
            self.logger.debug(f"Writing {len(data)} records to CSV")
            
            # Get fieldnames from first record
            fieldnames = list(data[0].keys())
            
            writer = csv.DictWriter(
                self.file_handle,
                fieldnames=fieldnames,
                delimiter=self.delimiter
            )
            
            # Write header if needed
            if self.include_header and self.mode == 'w':
                writer.writeheader()
            
            # Write data
            writer.writerows(data)
            self.file_handle.flush()
            
            self.logger.info(f"Successfully wrote {len(data)} records to CSV")
            
            return {
                'success': True,
                'records_written': len(data),
                'file_path': self.file_path
            }
            
        except Exception as e:
            self.logger.error(f"Error writing to CSV: {e}", exc_info=True)
            return {
                'success': False,
                'records_written': 0,
                'error': str(e)
            }
    
    def is_connected(self) -> bool:
        """Check if file is open."""
        return self.file_handle is not None and not self.file_handle.closed
