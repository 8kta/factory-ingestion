from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import logging

from ..logging_config import get_logger
from ..exceptions import ConnectionError as ClientConnectionError


class BaseClient(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self.logger = get_logger(self.__class__.__name__)
        self.logger.debug(f"Initializing {self.__class__.__name__} with config keys: {list(config.keys())}")
    
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data source."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the data source."""
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results."""
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if client is connected."""
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
