import pytest
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture
def sample_postgres_config():
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_db',
        'username': 'test_user',
        'password': 'test_pass'
    }


@pytest.fixture
def sample_mysql_config():
    return {
        'host': 'localhost',
        'port': 3306,
        'database': 'test_db',
        'username': 'root',
        'password': 'password'
    }


@pytest.fixture
def sample_sqlserver_config():
    return {
        'host': 'localhost',
        'port': 1433,
        'database': 'test_db',
        'username': 'sa',
        'password': 'Password123',
        'driver': '{ODBC Driver 17 for SQL Server}'
    }


@pytest.fixture
def sample_sqlite_config():
    return {
        'database_path': ':memory:'
    }


@pytest.fixture
def sample_athena_config():
    return {
        'region_name': 'us-east-1',
        'database': 'test_db',
        's3_output_location': 's3://test-bucket/results/',
        'aws_access_key_id': 'test_key',
        'aws_secret_access_key': 'test_secret'
    }


@pytest.fixture
def sample_cassandra_config():
    return {
        'hosts': ['localhost'],
        'port': 9042,
        'keyspace': 'test_keyspace',
        'username': 'cassandra',
        'password': 'cassandra'
    }


@pytest.fixture
def sample_opensearch_config():
    return {
        'hosts': ['localhost:9200'],
        'username': 'admin',
        'password': 'admin',
        'use_ssl': True,
        'verify_certs': False
    }


@pytest.fixture
def sample_solr_config():
    return {
        'url': 'http://localhost:8983/solr/test_core',
        'timeout': 10
    }


@pytest.fixture
def sample_kafka_config():
    return {
        'bootstrap_servers': ['localhost:9092'],
        'topic': 'test_topic',
        'group_id': 'test_group',
        'auto_offset_reset': 'earliest'
    }


@pytest.fixture
def sample_rabbitmq_config():
    return {
        'host': 'localhost',
        'port': 5672,
        'username': 'guest',
        'password': 'guest',
        'virtual_host': '/',
        'queue': 'test_queue',
        'exchange': ''
    }


@pytest.fixture
def sample_sqs_config():
    return {
        'region_name': 'us-east-1',
        'queue_url': 'https://sqs.us-east-1.amazonaws.com/123456789012/test-queue',
        'aws_access_key_id': 'test_key',
        'aws_secret_access_key': 'test_secret'
    }


@pytest.fixture
def sample_sns_config():
    return {
        'region_name': 'us-east-1',
        'topic_arn': 'arn:aws:sns:us-east-1:123456789012:test-topic',
        'aws_access_key_id': 'test_key',
        'aws_secret_access_key': 'test_secret'
    }
