import yaml
import json
from typing import Any, Dict, Optional
from pathlib import Path

from .logging_config import get_logger
from .exceptions import (
    ConfigurationError,
    InvalidSourceTypeError,
    ClientNotFoundError
)
from .clients.base_client import BaseClient
from .clients.athena_client import AthenaClient
from .clients.sqlserver_client import SQLServerClient
from .clients.mysql_client import MySQLClient
from .clients.sqlite_client import SQLiteClient
from .clients.postgres_client import PostgresClient
from .clients.opensearch_client import OpenSearchClient
from .clients.solr_client import SolrClient
from .clients.cassandra_client import CassandraClient
from .clients.kafka_client import KafkaClient
from .clients.sns_client import SNSClient
from .clients.sqs_client import SQSClient
from .clients.rabbitmq_client import RabbitMQClient


logger = get_logger('ClientFactory')


class ClientFactory:
    CLIENT_MAPPING = {
        'athena': AthenaClient,
        'sqlserver': SQLServerClient,
        'mysql': MySQLClient,
        'sqlite': SQLiteClient,
        'postgres': PostgresClient,
        'postgresql': PostgresClient,
        'opensearch': OpenSearchClient,
        'solr': SolrClient,
        'cassandra': CassandraClient,
        'kafka': KafkaClient,
        'sns': SNSClient,
        'sqs': SQSClient,
        'rabbitmq': RabbitMQClient,
    }
    
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
                    raise ConfigurationError(f"Unsupported configuration file format: {path.suffix}. Use .yaml, .yml, or .json")
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse YAML configuration: {e}", exc_info=True)
            raise ConfigurationError(f"Invalid YAML format in {config_path}: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON configuration: {e}", exc_info=True)
            raise ConfigurationError(f"Invalid JSON format in {config_path}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error loading configuration: {e}", exc_info=True)
            raise ConfigurationError(f"Failed to load configuration: {e}")
    
    @staticmethod
    def create_client(source_type: str, config: Dict[str, Any]) -> BaseClient:
        """Create a client instance for the specified source type."""
        try:
            logger.debug(f"Creating client for source type: {source_type}")
            source_type_lower = source_type.lower()
            
            if source_type_lower not in ClientFactory.CLIENT_MAPPING:
                logger.error(f"Unsupported source type: {source_type}")
                supported_types = ', '.join(sorted(ClientFactory.CLIENT_MAPPING.keys()))
                raise InvalidSourceTypeError(
                    f"Unsupported source type: {source_type}. "
                    f"Supported types: {supported_types}"
                )
            
            client_class = ClientFactory.CLIENT_MAPPING[source_type_lower]
            logger.info(f"Creating {client_class.__name__} instance")
            client = client_class(config)
            logger.debug(f"Successfully created {client_class.__name__} instance")
            return client
        except InvalidSourceTypeError:
            raise
        except Exception as e:
            logger.error(f"Failed to create client for {source_type}: {e}", exc_info=True)
            raise ConfigurationError(f"Failed to create client: {e}")
    
    @staticmethod
    def create_from_config_file(config_path: str, source_name: Optional[str] = None) -> BaseClient:
        """Create a client from a configuration file."""
        try:
            logger.info(f"Creating client from config file: {config_path}")
            config_data = ClientFactory.load_config(config_path)
            
            if 'sources' in config_data:
                logger.debug("Multi-source configuration detected")
                if not source_name:
                    available_sources = ', '.join(config_data['sources'].keys())
                    logger.error("Source name required for multi-source configuration")
                    raise ConfigurationError(
                        f"Configuration file contains multiple sources. Please specify source_name. "
                        f"Available sources: {available_sources}"
                    )
                
                if source_name not in config_data['sources']:
                    available_sources = ', '.join(config_data['sources'].keys())
                    logger.error(f"Source '{source_name}' not found in configuration")
                    raise ClientNotFoundError(
                        f"Source '{source_name}' not found in configuration. "
                        f"Available sources: {available_sources}"
                    )
                
                source_config = config_data['sources'][source_name]
                source_type = source_config.get('type')
                
                if not source_type:
                    logger.error(f"Source type not specified for '{source_name}'")
                    raise ConfigurationError(f"Source type not specified for '{source_name}'")
                
                logger.debug(f"Creating client for source '{source_name}' of type '{source_type}'")
                return ClientFactory.create_client(source_type, source_config)
            
            else:
                logger.debug("Single-source configuration detected")
                source_type = config_data.get('type')
                
                if not source_type:
                    logger.error("Source type not specified in configuration")
                    raise ConfigurationError("Source type not specified in configuration")
                
                return ClientFactory.create_client(source_type, config_data)
        except (ConfigurationError, ClientNotFoundError, InvalidSourceTypeError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating client from config file: {e}", exc_info=True)
            raise ConfigurationError(f"Failed to create client from config file: {e}")


class ClientBuilder:
    """Builder pattern for creating client instances."""
    
    def __init__(self):
        self._source_type: Optional[str] = None
        self._config: Dict[str, Any] = {}
        self._config_file: Optional[str] = None
        self._source_name: Optional[str] = None
        self._logger = get_logger('ClientBuilder')
        self._logger.debug("ClientBuilder initialized")
    
    def with_source_type(self, source_type: str) -> 'ClientBuilder':
        """Set the source type for the client."""
        self._logger.debug(f"Setting source type: {source_type}")
        self._source_type = source_type
        return self
    
    def with_config(self, config: Dict[str, Any]) -> 'ClientBuilder':
        """Set the configuration dictionary."""
        self._logger.debug(f"Setting config with keys: {list(config.keys())}")
        self._config = config
        return self
    
    def with_config_file(self, config_path: str, source_name: Optional[str] = None) -> 'ClientBuilder':
        """Set the configuration file path."""
        self._logger.debug(f"Setting config file: {config_path}, source: {source_name}")
        self._config_file = config_path
        self._source_name = source_name
        return self
    
    def add_config_param(self, key: str, value: Any) -> 'ClientBuilder':
        """Add a single configuration parameter."""
        self._logger.debug(f"Adding config parameter: {key}")
        self._config[key] = value
        return self
    
    def build(self) -> BaseClient:
        """Build and return the client instance."""
        try:
            self._logger.info("Building client instance")
            
            if self._config_file:
                self._logger.debug(f"Building from config file: {self._config_file}")
                return ClientFactory.create_from_config_file(self._config_file, self._source_name)
            
            if not self._source_type:
                self._logger.error("Source type not specified")
                raise ConfigurationError("Source type must be specified when not using config file")
            
            self._logger.debug(f"Building client of type '{self._source_type}' with direct config")
            return ClientFactory.create_client(self._source_type, self._config)
        except Exception as e:
            self._logger.error(f"Failed to build client: {e}", exc_info=True)
            raise
