# Test Suite

Comprehensive unit tests for the Factory Ingestion client library with mocked database connections.

## Running Tests

### Run all tests
```bash
pytest
```

### Run with coverage report
```bash
pytest --cov=src --cov-report=html
```

### Run specific test file
```bash
pytest tests/test_postgres_client.py
```

### Run specific test class
```bash
pytest tests/test_postgres_client.py::TestPostgresClient
```

### Run specific test method
```bash
pytest tests/test_postgres_client.py::TestPostgresClient::test_connect_success
```

### Run tests with verbose output
```bash
pytest -v
```

### Run tests and stop at first failure
```bash
pytest -x
```

## Test Structure

```
tests/
├── conftest.py                  # Pytest fixtures and configuration
├── test_base_client.py          # Base client abstract class tests
├── test_client_factory.py       # Factory and Builder pattern tests
│
├── Database Clients
├── test_athena_client.py        # AWS Athena tests
├── test_postgres_client.py      # PostgreSQL tests
├── test_mysql_client.py         # MySQL tests
├── test_sqlserver_client.py     # SQL Server tests
├── test_sqlite_client.py        # SQLite tests
├── test_cassandra_client.py     # Cassandra tests
│
├── Search Engine Clients
├── test_opensearch_client.py    # OpenSearch tests
├── test_solr_client.py          # Solr tests
│
└── Messaging System Clients
    ├── test_kafka_client.py     # Kafka tests
    ├── test_rabbitmq_client.py  # RabbitMQ tests
    ├── test_sqs_client.py       # AWS SQS tests
    └── test_sns_client.py       # AWS SNS tests
```

## Test Coverage

All tests use mocked connections, so they can run locally without requiring actual database or service connections.

### What's Tested

- **Connection Management**: Connect, disconnect, and connection status
- **Query Execution**: Basic queries and parameterized queries
- **Error Handling**: Connection errors and invalid operations
- **Context Managers**: Automatic resource cleanup
- **Factory Pattern**: Client creation from configuration
- **Builder Pattern**: Fluent API for client construction
- **Configuration Loading**: YAML and JSON configuration files

### Mocking Strategy

Each test mocks the underlying client libraries:
- **psycopg2** for PostgreSQL
- **mysql.connector** for MySQL
- **pyodbc** for SQL Server
- **sqlite3** for SQLite
- **boto3** for AWS services (Athena, SQS, SNS)
- **cassandra-driver** for Cassandra
- **opensearch-py** for OpenSearch
- **pysolr** for Solr
- **kafka-python** for Kafka
- **pika** for RabbitMQ

## Fixtures

Common test fixtures are defined in `conftest.py`:

- `sample_postgres_config`: PostgreSQL configuration
- `sample_mysql_config`: MySQL configuration
- `sample_sqlserver_config`: SQL Server configuration
- `sample_sqlite_config`: SQLite configuration
- `sample_athena_config`: AWS Athena configuration
- `sample_cassandra_config`: Cassandra configuration
- `sample_opensearch_config`: OpenSearch configuration
- `sample_solr_config`: Solr configuration
- `sample_kafka_config`: Kafka configuration
- `sample_rabbitmq_config`: RabbitMQ configuration
- `sample_sqs_config`: AWS SQS configuration
- `sample_sns_config`: AWS SNS configuration

## Coverage Report

After running tests with coverage, view the HTML report:

```bash
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
start htmlcov/index.html  # Windows
```

## Continuous Integration

These tests are designed to run in CI/CD pipelines without external dependencies.

Example GitHub Actions workflow:

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      - run: pip install -r requirements-test.txt
      - run: pytest --cov=src --cov-report=xml
      - uses: codecov/codecov-action@v2
```

## Adding New Tests

When adding a new client:

1. Create a new test file: `tests/test_<client_name>_client.py`
2. Add configuration fixture in `conftest.py`
3. Mock the underlying library
4. Test all base client methods:
   - `connect()`
   - `disconnect()`
   - `execute_query()`
   - `is_connected()`
5. Test context manager behavior
6. Test error conditions

Example test structure:

```python
import pytest
from unittest.mock import Mock, patch
from src.clients.new_client import NewClient


class TestNewClient:
    
    @patch('src.clients.new_client.library.connect')
    def test_connect_success(self, mock_connect, sample_new_config):
        # Test implementation
        pass
    
    @patch('src.clients.new_client.library.connect')
    def test_disconnect(self, mock_connect, sample_new_config):
        # Test implementation
        pass
    
    @patch('src.clients.new_client.library.connect')
    def test_execute_query_success(self, mock_connect, sample_new_config):
        # Test implementation
        pass
    
    def test_execute_query_not_connected(self, sample_new_config):
        # Test implementation
        pass
    
    @patch('src.clients.new_client.library.connect')
    def test_is_connected(self, mock_connect, sample_new_config):
        # Test implementation
        pass
```
