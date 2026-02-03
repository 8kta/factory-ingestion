"""
Writer Factory and Builder for Data Ingestion

This module provides factory and builder patterns for creating data writers.
"""

import yaml
import json
from typing import Any, Dict, Optional
from pathlib import Path

from .logging_config import get_logger
from .exceptions import ConfigurationError, InvalidSourceTypeError
from .writers.base_writer import BaseWriter
from .writers.s3_writer import S3Writer
from .writers.csv_writer import CSVWriter
from .writers.opensearch_writer import OpenSearchWriter
from .writers.solr_writer import SolrWriter
from .writers.cassandra_writer import CassandraWriter
from .writers.mongodb_writer import MongoDBWriter
from .writers.azure_blob_writer import AzureBlobWriter
from .writers.gcp_storage_writer import GCPStorageWriter


logger = get_logger('WriterFactory')


class WriterFactory:
    """Factory for creating data writers."""
    
    WRITER_MAPPING = {
        's3': S3Writer,
        'csv': CSVWriter,
        'opensearch': OpenSearchWriter,
        'solr': SolrWriter,
        'cassandra': CassandraWriter,
        'mongodb': MongoDBWriter,
        'mongo': MongoDBWriter,
        'azure_blob': AzureBlobWriter,
        'azure': AzureBlobWriter,
        'gcs': GCPStorageWriter,
        'gcp': GCPStorageWriter,
        'google_storage': GCPStorageWriter,
    }
    
    @staticmethod
    def create_writer(writer_type: str, config: Dict[str, Any]) -> BaseWriter:
        """
        Create a writer instance.
        
        Args:
            writer_type: Type of writer (s3, csv, opensearch, etc.)
            config: Configuration dictionary
            
        Returns:
            Writer instance
        """
        try:
            logger.debug(f"Creating writer for type: {writer_type}")
            writer_type_lower = writer_type.lower()
            
            if writer_type_lower not in WriterFactory.WRITER_MAPPING:
                logger.error(f"Unsupported writer type: {writer_type}")
                supported_types = ', '.join(sorted(WriterFactory.WRITER_MAPPING.keys()))
                raise InvalidSourceTypeError(
                    f"Unsupported writer type: {writer_type}. "
                    f"Supported types: {supported_types}"
                )
            
            writer_class = WriterFactory.WRITER_MAPPING[writer_type_lower]
            logger.info(f"Creating {writer_class.__name__} instance")
            writer = writer_class(config)
            logger.debug(f"Successfully created {writer_class.__name__} instance")
            return writer
        except InvalidSourceTypeError:
            raise
        except Exception as e:
            logger.error(f"Failed to create writer for {writer_type}: {e}", exc_info=True)
            raise ConfigurationError(f"Failed to create writer: {e}")
    
    @staticmethod
    def load_config(config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML or JSON file."""
        try:
            logger.debug(f"Loading configuration from: {config_path}")
            path = Path(config_path)
            
            if not path.exists():
                logger.error(f"Configuration file not found: {config_path}")
                raise ConfigurationError(f"Configuration file not found: {config_path}")
            
            with open(path, 'r') as f:
                if path.suffix in ['.yaml', '.yml']:
                    config = yaml.safe_load(f)
                    logger.info(f"Successfully loaded YAML configuration from {config_path}")
                    return config
                elif path.suffix == '.json':
                    config = json.load(f)
                    logger.info(f"Successfully loaded JSON configuration from {config_path}")
                    return config
                else:
                    logger.error(f"Unsupported configuration file format: {path.suffix}")
                    raise ConfigurationError(f"Unsupported configuration file format: {path.suffix}")
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse YAML configuration: {e}", exc_info=True)
            raise ConfigurationError(f"Invalid YAML format in {config_path}: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON configuration: {e}", exc_info=True)
            raise ConfigurationError(f"Invalid JSON format in {config_path}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error loading configuration: {e}", exc_info=True)
            raise ConfigurationError(f"Failed to load configuration: {e}")


class WriterBuilder:
    """Builder pattern for creating writer instances."""
    
    def __init__(self):
        self._writer_type: Optional[str] = None
        self._config: Dict[str, Any] = {}
        self._config_file: Optional[str] = None
        self._logger = get_logger('WriterBuilder')
        self._logger.debug("WriterBuilder initialized")
    
    def with_writer_type(self, writer_type: str) -> 'WriterBuilder':
        """Set the writer type."""
        self._logger.debug(f"Setting writer type: {writer_type}")
        self._writer_type = writer_type
        return self
    
    def with_config(self, config: Dict[str, Any]) -> 'WriterBuilder':
        """Set the configuration dictionary."""
        self._logger.debug(f"Setting config with keys: {list(config.keys())}")
        self._config = config
        return self
    
    def with_config_file(self, config_path: str) -> 'WriterBuilder':
        """Set the configuration file path."""
        self._logger.debug(f"Setting config file: {config_path}")
        self._config_file = config_path
        return self
    
    def add_config_param(self, key: str, value: Any) -> 'WriterBuilder':
        """Add a single configuration parameter."""
        self._logger.debug(f"Adding config parameter: {key}")
        self._config[key] = value
        return self
    
    def build(self) -> BaseWriter:
        """Build and return the writer instance."""
        try:
            self._logger.info("Building writer instance")
            
            if self._config_file:
                self._logger.debug(f"Building from config file: {self._config_file}")
                config_data = WriterFactory.load_config(self._config_file)
                
                # Extract writer type and config from file
                if 'type' in config_data:
                    self._writer_type = config_data['type']
                    self._config = {k: v for k, v in config_data.items() if k != 'type'}
                else:
                    raise ConfigurationError("Writer type not specified in configuration file")
            
            if not self._writer_type:
                self._logger.error("Writer type not specified")
                raise ConfigurationError("Writer type must be specified")
            
            self._logger.debug(f"Building writer of type '{self._writer_type}' with config")
            return WriterFactory.create_writer(self._writer_type, self._config)
        except Exception as e:
            self._logger.error(f"Failed to build writer: {e}", exc_info=True)
            raise
