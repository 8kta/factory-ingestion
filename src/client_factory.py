import yaml
import json
from typing import Any, Dict, Optional
from pathlib import Path

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
        path = Path(config_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(path, 'r') as f:
            if path.suffix in ['.yaml', '.yml']:
                return yaml.safe_load(f)
            elif path.suffix == '.json':
                return json.load(f)
            else:
                raise ValueError(f"Unsupported configuration file format: {path.suffix}")
    
    @staticmethod
    def create_client(source_type: str, config: Dict[str, Any]) -> BaseClient:
        source_type_lower = source_type.lower()
        
        if source_type_lower not in ClientFactory.CLIENT_MAPPING:
            raise ValueError(
                f"Unsupported source type: {source_type}. "
                f"Supported types: {', '.join(ClientFactory.CLIENT_MAPPING.keys())}"
            )
        
        client_class = ClientFactory.CLIENT_MAPPING[source_type_lower]
        return client_class(config)
    
    @staticmethod
    def create_from_config_file(config_path: str, source_name: Optional[str] = None) -> BaseClient:
        config_data = ClientFactory.load_config(config_path)
        
        if 'sources' in config_data:
            if not source_name:
                raise ValueError("Configuration file contains multiple sources. Please specify source_name.")
            
            if source_name not in config_data['sources']:
                raise ValueError(
                    f"Source '{source_name}' not found in configuration. "
                    f"Available sources: {', '.join(config_data['sources'].keys())}"
                )
            
            source_config = config_data['sources'][source_name]
            source_type = source_config.get('type')
            
            if not source_type:
                raise ValueError(f"Source type not specified for '{source_name}'")
            
            return ClientFactory.create_client(source_type, source_config)
        
        else:
            source_type = config_data.get('type')
            
            if not source_type:
                raise ValueError("Source type not specified in configuration")
            
            return ClientFactory.create_client(source_type, config_data)


class ClientBuilder:
    def __init__(self):
        self._source_type: Optional[str] = None
        self._config: Dict[str, Any] = {}
        self._config_file: Optional[str] = None
        self._source_name: Optional[str] = None
    
    def with_source_type(self, source_type: str) -> 'ClientBuilder':
        self._source_type = source_type
        return self
    
    def with_config(self, config: Dict[str, Any]) -> 'ClientBuilder':
        self._config = config
        return self
    
    def with_config_file(self, config_path: str, source_name: Optional[str] = None) -> 'ClientBuilder':
        self._config_file = config_path
        self._source_name = source_name
        return self
    
    def add_config_param(self, key: str, value: Any) -> 'ClientBuilder':
        self._config[key] = value
        return self
    
    def build(self) -> BaseClient:
        if self._config_file:
            return ClientFactory.create_from_config_file(self._config_file, self._source_name)
        
        if not self._source_type:
            raise ValueError("Source type must be specified")
        
        return ClientFactory.create_client(self._source_type, self._config)
