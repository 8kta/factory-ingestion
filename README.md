# Factory Ingestion

A flexible Python client library for querying multiple data sources through a unified interface. Supports databases, search engines, message queues, and cloud services with configuration-based initialization.

## Features

- **Unified Interface**: Single API for multiple data sources
- **Configuration-Driven**: YAML/JSON configuration files
- **Builder Pattern**: Fluent API for client construction
- **Context Manager Support**: Automatic connection management
- **Extensible**: Easy to add new data sources

## Supported Data Sources

### Databases
- **AWS Athena** - Serverless SQL queries on S3
- **PostgreSQL** - Open-source relational database
- **MySQL** - Popular relational database
- **SQL Server** - Microsoft's relational database
- **SQLite** - Lightweight embedded database
- **Cassandra** - Distributed NoSQL database

### Search Engines
- **OpenSearch** - Open-source search and analytics
- **Apache Solr** - Enterprise search platform

### Messaging Systems
- **Apache Kafka** - Distributed streaming platform
- **RabbitMQ** - Message broker
- **AWS SQS** - Simple Queue Service
- **AWS SNS** - Simple Notification Service

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd factory-ingestion

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Quick Start

### Using Configuration File

```python
from src.client_factory import ClientBuilder

# Create client from configuration
client = (ClientBuilder()
          .with_config_file('config/sources.yaml', 'my_postgres')
          .build())

# Use with context manager
with client:
    results = client.execute_query("SELECT * FROM users LIMIT 10")
    print(results)
```

### Using Builder Pattern

```python
from src.client_factory import ClientBuilder

# Build client programmatically
client = (ClientBuilder()
          .with_source_type('postgres')
          .add_config_param('host', 'localhost')
          .add_config_param('port', 5432)
          .add_config_param('database', 'mydb')
          .add_config_param('username', 'user')
          .add_config_param('password', 'pass')
          .build())

with client:
    results = client.execute_query("SELECT version();")
```

### Using Factory Directly

```python
from src.client_factory import ClientFactory

config = {
    'host': 'localhost',
    'port': 3306,
    'database': 'mydb',
    'username': 'root',
    'password': 'password'
}

client = ClientFactory.create_client('mysql', config)

with client:
    results = client.execute_query("SELECT DATABASE();")
```

## Configuration

### Multi-Source Configuration (YAML)

```yaml
sources:
  my_postgres:
    type: postgres
    host: localhost
    port: 5432
    database: mydb
    username: user
    password: pass

  my_kafka:
    type: kafka
    bootstrap_servers:
      - localhost:9092
    topic: my_topic
    group_id: my_group
```

### Single Source Configuration (JSON)

```json
{
  "type": "postgres",
  "host": "localhost",
  "port": 5432,
  "database": "mydb",
  "username": "user",
  "password": "pass"
}
```

## Command Line Usage

```bash
# List available sources
python main.py --config config/sources.yaml --list-sources

# Query a specific source
python main.py --config config/sources.yaml --source my_postgres --query "SELECT * FROM users"

# Run examples
python main.py --examples
```

## Configuration Examples

See `config/sources.yaml` for comprehensive configuration examples for all supported data sources.

## Architecture

```
factory-ingestion/
├── src/
│   ├── clients/
│   │   ├── base_client.py       # Abstract base class
│   │   ├── athena_client.py     # AWS Athena implementation
│   │   ├── postgres_client.py   # PostgreSQL implementation
│   │   ├── mysql_client.py      # MySQL implementation
│   │   ├── sqlserver_client.py  # SQL Server implementation
│   │   ├── sqlite_client.py     # SQLite implementation
│   │   ├── opensearch_client.py # OpenSearch implementation
│   │   ├── solr_client.py       # Solr implementation
│   │   ├── cassandra_client.py  # Cassandra implementation
│   │   ├── kafka_client.py      # Kafka implementation
│   │   ├── sns_client.py        # AWS SNS implementation
│   │   ├── sqs_client.py        # AWS SQS implementation
│   │   └── rabbitmq_client.py   # RabbitMQ implementation
│   └── client_factory.py        # Factory and Builder classes
├── config/
│   ├── sources.yaml             # Multi-source configuration
│   └── single_source.json       # Single source configuration
├── main.py                      # CLI and examples
└── requirements.txt             # Python dependencies
```

## Error Handling and Logging

The library includes comprehensive error handling and structured logging throughout the application.

### Custom Exceptions

All exceptions inherit from `FactoryIngestionError`:
- `ConnectionError` - Connection failures
- `QueryExecutionError` - Query execution failures  
- `ConfigurationError` - Configuration issues
- `InvalidSourceTypeError` - Unsupported source type
- `AuthenticationError` - Authentication failures
- And more...

### Logging

Configure logging levels and output:

```python
from src.logging_config import setup_logging

# Setup with file output
setup_logging(
    level='DEBUG',
    log_file='logs/app.log',
    log_to_console=True
)
```

CLI logging options:

```bash
# Debug level logging
python main.py --config config.yaml --source my_db --query "SELECT 1" --log-level DEBUG

# Log to file
python main.py --config config.yaml --source my_db --query "SELECT 1" --log-file logs/app.log
```

See `ERROR_HANDLING_AND_LOGGING.md` for detailed documentation.

## Testing

The project includes comprehensive unit tests with mocked connections that can run locally without requiring actual database or service connections.

### Running Tests

```bash
# Install test dependencies
pip install -r requirements-test.txt

# Run all tests
pytest

# Run with coverage report
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/test_postgres_client.py

# Using Makefile
make test          # Run all tests
make test-cov      # Run with coverage
make test-verbose  # Run with verbose output
```

### Test Coverage

All clients are fully tested with mocked connections:
- Connection management (connect, disconnect, is_connected)
- Query execution (basic and parameterized queries)
- Error handling and edge cases
- Context manager behavior
- Factory and Builder patterns

See `tests/README.md` for detailed testing documentation.

## Adding New Data Sources

1. Create a new client class inheriting from `BaseClient`
2. Implement required methods: `connect()`, `disconnect()`, `execute_query()`, `is_connected()`
3. Add the client to `CLIENT_MAPPING` in `client_factory.py`
4. Create unit tests in `tests/test_<client_name>_client.py`

```python
from .base_client import BaseClient

class MyNewClient(BaseClient):
    def connect(self):
        # Connection logic
        pass
    
    def disconnect(self):
        # Disconnection logic
        pass
    
    def execute_query(self, query, params=None):
        # Query execution logic
        return []
    
    def is_connected(self):
        # Connection status check
        return True
```

## License

See LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
