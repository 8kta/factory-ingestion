import pytest
import tempfile
import json
import yaml
from pathlib import Path
from unittest.mock import Mock, patch
from src.client_factory import ClientFactory, ClientBuilder
from src.clients.postgres_client import PostgresClient
from src.clients.mysql_client import MySQLClient
from src.clients.sqlite_client import SQLiteClient


class TestClientFactory:
    
    def test_load_config_yaml(self, tmp_path):
        config_data = {
            'type': 'postgres',
            'host': 'localhost',
            'port': 5432
        }
        
        config_file = tmp_path / "test_config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        loaded_config = ClientFactory.load_config(str(config_file))
        
        assert loaded_config == config_data
    
    def test_load_config_json(self, tmp_path):
        config_data = {
            'type': 'mysql',
            'host': 'localhost',
            'port': 3306
        }
        
        config_file = tmp_path / "test_config.json"
        with open(config_file, 'w') as f:
            json.dump(config_data, f)
        
        loaded_config = ClientFactory.load_config(str(config_file))
        
        assert loaded_config == config_data
    
    def test_load_config_file_not_found(self):
        with pytest.raises(FileNotFoundError, match="Configuration file not found"):
            ClientFactory.load_config("/nonexistent/config.yaml")
    
    def test_load_config_unsupported_format(self, tmp_path):
        config_file = tmp_path / "test_config.txt"
        config_file.write_text("some text")
        
        with pytest.raises(ValueError, match="Unsupported configuration file format"):
            ClientFactory.load_config(str(config_file))
    
    def test_create_client_postgres(self, sample_postgres_config):
        client = ClientFactory.create_client('postgres', sample_postgres_config)
        
        assert isinstance(client, PostgresClient)
        assert client.config == sample_postgres_config
    
    def test_create_client_mysql(self, sample_mysql_config):
        client = ClientFactory.create_client('mysql', sample_mysql_config)
        
        assert isinstance(client, MySQLClient)
        assert client.config == sample_mysql_config
    
    def test_create_client_sqlite(self, sample_sqlite_config):
        client = ClientFactory.create_client('sqlite', sample_sqlite_config)
        
        assert isinstance(client, SQLiteClient)
        assert client.config == sample_sqlite_config
    
    def test_create_client_case_insensitive(self, sample_postgres_config):
        client1 = ClientFactory.create_client('POSTGRES', sample_postgres_config)
        client2 = ClientFactory.create_client('PostgreSQL', sample_postgres_config)
        client3 = ClientFactory.create_client('postgres', sample_postgres_config)
        
        assert isinstance(client1, PostgresClient)
        assert isinstance(client2, PostgresClient)
        assert isinstance(client3, PostgresClient)
    
    def test_create_client_unsupported_type(self, sample_postgres_config):
        with pytest.raises(ValueError, match="Unsupported source type"):
            ClientFactory.create_client('unsupported_db', sample_postgres_config)
    
    def test_create_from_config_file_single_source(self, tmp_path, sample_postgres_config):
        config_data = {
            'type': 'postgres',
            **sample_postgres_config
        }
        
        config_file = tmp_path / "single_source.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        client = ClientFactory.create_from_config_file(str(config_file))
        
        assert isinstance(client, PostgresClient)
    
    def test_create_from_config_file_multi_source(self, tmp_path, sample_postgres_config, sample_mysql_config):
        config_data = {
            'sources': {
                'my_postgres': {
                    'type': 'postgres',
                    **sample_postgres_config
                },
                'my_mysql': {
                    'type': 'mysql',
                    **sample_mysql_config
                }
            }
        }
        
        config_file = tmp_path / "multi_source.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        client = ClientFactory.create_from_config_file(str(config_file), 'my_postgres')
        
        assert isinstance(client, PostgresClient)
    
    def test_create_from_config_file_multi_source_no_name(self, tmp_path, sample_postgres_config):
        config_data = {
            'sources': {
                'my_postgres': {
                    'type': 'postgres',
                    **sample_postgres_config
                }
            }
        }
        
        config_file = tmp_path / "multi_source.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        with pytest.raises(ValueError, match="Please specify source_name"):
            ClientFactory.create_from_config_file(str(config_file))
    
    def test_create_from_config_file_source_not_found(self, tmp_path, sample_postgres_config):
        config_data = {
            'sources': {
                'my_postgres': {
                    'type': 'postgres',
                    **sample_postgres_config
                }
            }
        }
        
        config_file = tmp_path / "multi_source.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        with pytest.raises(ValueError, match="Source 'nonexistent' not found"):
            ClientFactory.create_from_config_file(str(config_file), 'nonexistent')
    
    def test_create_from_config_file_no_type(self, tmp_path):
        config_data = {
            'host': 'localhost',
            'port': 5432
        }
        
        config_file = tmp_path / "no_type.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        with pytest.raises(ValueError, match="Source type not specified"):
            ClientFactory.create_from_config_file(str(config_file))


class TestClientBuilder:
    
    def test_with_source_type(self):
        builder = ClientBuilder()
        result = builder.with_source_type('postgres')
        
        assert result is builder
        assert builder._source_type == 'postgres'
    
    def test_with_config(self, sample_postgres_config):
        builder = ClientBuilder()
        result = builder.with_config(sample_postgres_config)
        
        assert result is builder
        assert builder._config == sample_postgres_config
    
    def test_with_config_file(self):
        builder = ClientBuilder()
        result = builder.with_config_file('config/sources.yaml', 'my_postgres')
        
        assert result is builder
        assert builder._config_file == 'config/sources.yaml'
        assert builder._source_name == 'my_postgres'
    
    def test_add_config_param(self):
        builder = ClientBuilder()
        result = builder.add_config_param('host', 'localhost')
        
        assert result is builder
        assert builder._config['host'] == 'localhost'
    
    def test_add_multiple_config_params(self):
        builder = ClientBuilder()
        builder.add_config_param('host', 'localhost')
        builder.add_config_param('port', 5432)
        builder.add_config_param('database', 'testdb')
        
        assert builder._config == {
            'host': 'localhost',
            'port': 5432,
            'database': 'testdb'
        }
    
    def test_build_with_source_type_and_config(self, sample_postgres_config):
        builder = ClientBuilder()
        client = (builder
                  .with_source_type('postgres')
                  .with_config(sample_postgres_config)
                  .build())
        
        assert isinstance(client, PostgresClient)
    
    def test_build_with_fluent_config(self):
        client = (ClientBuilder()
                  .with_source_type('sqlite')
                  .add_config_param('database_path', ':memory:')
                  .build())
        
        assert isinstance(client, SQLiteClient)
        assert client.config['database_path'] == ':memory:'
    
    def test_build_with_config_file(self, tmp_path, sample_postgres_config):
        config_data = {
            'type': 'postgres',
            **sample_postgres_config
        }
        
        config_file = tmp_path / "test_config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        client = (ClientBuilder()
                  .with_config_file(str(config_file))
                  .build())
        
        assert isinstance(client, PostgresClient)
    
    def test_build_without_source_type(self):
        builder = ClientBuilder()
        builder.add_config_param('host', 'localhost')
        
        with pytest.raises(ValueError, match="Source type must be specified"):
            builder.build()
    
    def test_build_config_file_priority(self, tmp_path, sample_mysql_config):
        config_data = {
            'type': 'mysql',
            **sample_mysql_config
        }
        
        config_file = tmp_path / "test_config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        client = (ClientBuilder()
                  .with_source_type('postgres')
                  .with_config_file(str(config_file))
                  .build())
        
        assert isinstance(client, MySQLClient)
    
    def test_chaining_methods(self):
        builder = (ClientBuilder()
                   .with_source_type('sqlite')
                   .add_config_param('database_path', ':memory:')
                   .with_config({'database_path': '/tmp/test.db'}))
        
        assert builder._source_type == 'sqlite'
        assert builder._config['database_path'] == '/tmp/test.db'
