# Project Structure

```
factory-ingestion/
│
├── README.md                    # Main documentation
├── USAGE_EXAMPLES.md           # Detailed usage examples
├── PROJECT_STRUCTURE.md        # This file
├── LICENSE                     # License file
├── .gitignore                  # Git ignore rules
├── requirements.txt            # Python dependencies
├── main.py                     # CLI interface and examples
│
├── config/                     # Configuration files
│   ├── sources.yaml           # Multi-source configuration (YAML)
│   └── single_source.json     # Single source configuration (JSON)
│
└── src/                        # Source code
    ├── __init__.py
    ├── client_factory.py       # Factory and Builder pattern implementation
    │
    └── clients/                # Client implementations
        ├── __init__.py
        ├── base_client.py      # Abstract base class
        │
        ├── athena_client.py    # AWS Athena
        ├── cassandra_client.py # Apache Cassandra
        ├── kafka_client.py     # Apache Kafka
        ├── mysql_client.py     # MySQL
        ├── opensearch_client.py # OpenSearch
        ├── postgres_client.py  # PostgreSQL
        ├── rabbitmq_client.py  # RabbitMQ
        ├── sns_client.py       # AWS SNS
        ├── solr_client.py      # Apache Solr
        ├── sqlite_client.py    # SQLite
        ├── sqlserver_client.py # Microsoft SQL Server
        └── sqs_client.py       # AWS SQS
```

## Key Components

### 1. Base Client (`src/clients/base_client.py`)
- Abstract base class defining the interface for all clients
- Methods: `connect()`, `disconnect()`, `execute_query()`, `is_connected()`
- Implements context manager protocol (`__enter__`, `__exit__`)

### 2. Client Factory (`src/client_factory.py`)
- **ClientFactory**: Factory class for creating clients
  - `load_config()`: Load YAML/JSON configuration files
  - `create_client()`: Create client by type and config
  - `create_from_config_file()`: Create client from config file
  
- **ClientBuilder**: Builder pattern for fluent API
  - `with_source_type()`: Set source type
  - `with_config()`: Set configuration dictionary
  - `with_config_file()`: Load from configuration file
  - `add_config_param()`: Add individual config parameters
  - `build()`: Build and return client instance

### 3. Individual Clients (`src/clients/`)
Each client implements the `BaseClient` interface for a specific data source:

#### Databases
- **AthenaClient**: AWS Athena serverless SQL
- **PostgresClient**: PostgreSQL database
- **MySQLClient**: MySQL database
- **SQLServerClient**: Microsoft SQL Server
- **SQLiteClient**: SQLite embedded database
- **CassandraClient**: Apache Cassandra NoSQL

#### Search Engines
- **OpenSearchClient**: OpenSearch distributed search
- **SolrClient**: Apache Solr search platform

#### Messaging Systems
- **KafkaClient**: Apache Kafka streaming
- **RabbitMQClient**: RabbitMQ message broker
- **SQSClient**: AWS Simple Queue Service
- **SNSClient**: AWS Simple Notification Service

### 4. Configuration Files (`config/`)
- **sources.yaml**: Multi-source configuration with all supported sources
- **single_source.json**: Example single source configuration

### 5. Main Entry Point (`main.py`)
- CLI interface for querying data sources
- Example demonstrations for all clients
- Command-line arguments for flexible usage

## Design Patterns

### Factory Pattern
The `ClientFactory` class creates appropriate client instances based on configuration:
```python
client = ClientFactory.create_client('postgres', config)
```

### Builder Pattern
The `ClientBuilder` provides a fluent API for constructing clients:
```python
client = (ClientBuilder()
          .with_source_type('postgres')
          .add_config_param('host', 'localhost')
          .build())
```

### Strategy Pattern
Each client implements the same interface (`BaseClient`), allowing them to be used interchangeably.

### Context Manager
All clients support the context manager protocol for automatic resource management:
```python
with client:
    results = client.execute_query("SELECT * FROM table")
```

## Configuration Format

### Multi-Source YAML
```yaml
sources:
  source_name:
    type: postgres
    host: localhost
    port: 5432
    # ... other parameters
```

### Single Source JSON
```json
{
  "type": "postgres",
  "host": "localhost",
  "port": 5432
}
```

## Extension Points

### Adding a New Client

1. Create new client file in `src/clients/`
2. Inherit from `BaseClient`
3. Implement required methods
4. Register in `ClientFactory.CLIENT_MAPPING`

Example:
```python
from .base_client import BaseClient

class NewClient(BaseClient):
    def connect(self):
        # Implementation
        pass
    
    def disconnect(self):
        # Implementation
        pass
    
    def execute_query(self, query, params=None):
        # Implementation
        return []
    
    def is_connected(self):
        # Implementation
        return True
```

Then add to `client_factory.py`:
```python
CLIENT_MAPPING = {
    # ... existing mappings
    'newclient': NewClient,
}
```

## Dependencies

See `requirements.txt` for complete list:
- Database drivers: `psycopg2-binary`, `mysql-connector-python`, `pyodbc`, `cassandra-driver`
- AWS services: `boto3`
- Search engines: `opensearch-py`, `pysolr`
- Messaging: `kafka-python`, `pika`
- Configuration: `PyYAML`

## Usage Patterns

### Pattern 1: Configuration File
```python
client = ClientBuilder().with_config_file('config/sources.yaml', 'my_postgres').build()
```

### Pattern 2: Programmatic Configuration
```python
client = ClientBuilder().with_source_type('postgres').add_config_param('host', 'localhost').build()
```

### Pattern 3: Direct Factory
```python
client = ClientFactory.create_client('postgres', config_dict)
```

### Pattern 4: CLI
```bash
python main.py --config config/sources.yaml --source my_postgres --query "SELECT * FROM users"
```
