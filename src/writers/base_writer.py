"""
Base Writer Interface for Data Ingestion

This module provides the abstract base class for all data writers.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from ..logging_config import get_logger


class BaseWriter(ABC):
    """Abstract base class for all data writers."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the writer.
        
        Args:
            config: Configuration dictionary for the writer
        """
        self.config = config
        self.logger = get_logger(self.__class__.__name__)
        self.logger.debug(f"Initializing {self.__class__.__name__} with config keys: {list(config.keys())}")
    
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the destination."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the destination."""
        pass
    
    @abstractmethod
    def write(self, data: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """
        Write data to the destination.
        
        Args:
            data: List of dictionaries to write
            **kwargs: Additional parameters for writing
            
        Returns:
            Dictionary with write results (records_written, errors, etc.)
        """
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if writer is connected."""
        pass
    
    def __enter__(self):
        """Context manager entry."""
        try:
            self.logger.debug(f"Entering context manager for {self.__class__.__name__}")
            self.connect()
            return self
        except Exception as e:
            self.logger.error(f"Failed to connect in context manager: {e}", exc_info=True)
            raise
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        try:
            if exc_type is not None:
                self.logger.error(f"Exception occurred in context: {exc_type.__name__}: {exc_val}")
            self.logger.debug(f"Exiting context manager for {self.__class__.__name__}")
            self.disconnect()
        except Exception as e:
            self.logger.error(f"Error during disconnect in context manager: {e}", exc_info=True)
        return False
