# Factory Ingestion - Examples

This directory contains comprehensive examples demonstrating how to use every client type in the Factory Ingestion library.

## Available Examples

### 1. All Clients Examples (`all_clients_examples.py`)

**Comprehensive demonstration of all 12 client types:**

#### Database Clients (6)
- ✅ **PostgreSQL** - Relational database queries
- ✅ **MySQL** - MySQL database operations
- ✅ **SQL Server** - Microsoft SQL Server integration
- ✅ **SQLite** - Lightweight embedded database (works without external service!)
- ✅ **AWS Athena** - Serverless query service for S3 data
- ✅ **Cassandra** - Distributed NoSQL database

#### Search Engine Clients (2)
- ✅ **OpenSearch** - Full-text search and analytics
- ✅ **Apache Solr** - Enterprise search platform

#### Messaging Clients (4)
- ✅ **Kafka** - Distributed event streaming (produce/consume)
- ✅ **RabbitMQ** - Message broker (publish/consume)
- ✅ **AWS SQS** - Managed message queue service
- ✅ **AWS SNS** - Pub/sub notification service

#### Advanced Features
- ✅ **Schema Transformation** - Normalize data from multiple sources

### 2. Schema Transformation Examples (`schema_transformation_example.py`)

**7 examples demonstrating schema transformation:**
- Basic transformation
- Field mapping from different sources
- Nested data access
- Custom transformers
- Schema registry
- Arrays of objects
- Default values

## Running the Examples

### Run All Client Examples

```bash
# Using venv
./venv/bin/python3 examples/all_clients_examples.py

# Or from project root
cd /Users/octavalo/Documents/projects/factory-ingestion
./venv/bin/python3 examples/all_clients_examples.py
```

### Run Schema Transformation Examples

```bash
./venv/bin/python3 examples/schema_transformation_example.py
```

## What to Expect

### Expected Behavior

Most examples will show **connection errors** if the corresponding services aren't running. This is **expected and demonstrates proper error handling**:

```
❌ Connection failed (expected if PostgreSQL not running): Failed to connect...
```

### Working Examples

**SQLite example will work without any external services** since it uses an in-memory database:

```bash
./venv/bin/python3 -c "
import sys
sys.path.insert(0, '.')
from examples.all_clients_examples import example_sqlite
example_sqlite()
"
```

**Schema transformation examples work without any services** since they use simulated data:

```bash
./venv/bin/python3 examples/schema_transformation_example.py
```

## Example Structure

Each client example demonstrates:

1. **Configuration** - How to configure the client
2. **Connection** - Establishing connection with error handling
3. **Operations** - Executing queries/operations
4. **Resource Management** - Using context managers for cleanup
5. **Error Handling** - Proper exception handling

### Example Pattern

```python
def example_client():
    """Example: Client Name"""
    config = {
        'host': 'localhost',
        'port': 1234,
        # ... other config
    }
    
    try:
        client = ClientFactory.create_client('client_type', config)
        
        with client:
            print(f"✓ Connected: {client.is_connected()}")
            
            # Perform operations
            results = client.execute_query("QUERY")
            print(f"Results: {len(results)} rows")
        
        print("✓ Example completed")
        
    except ClientConnectionError as e:
        print(f"❌ Connection failed: {e}")
```

## Client-Specific Examples

### Database Queries

```python
# PostgreSQL
results = client.execute_query("SELECT * FROM users WHERE id = %(user_id)s", {'user_id': 123})

# MySQL
results = client.execute_query("SELECT * FROM users WHERE id = %s", {'id': 123})

# SQLite
results = client.execute_query("SELECT * FROM users WHERE id = ?", {'id': 123})
```

### Search Queries

```python
# OpenSearch
results = client.execute_query('error', {'index': 'logs', 'body': {'size': 10}})

# Solr
results = client.execute_query('title:python', {'rows': 10})
```

### Messaging Operations

```python
# Kafka - Produce
client.execute_query('produce', {'key': 'user1', 'value': {'action': 'login'}})

# Kafka - Consume
messages = client.execute_query('consume', {'max_messages': 10, 'timeout': 5})

# RabbitMQ - Publish
client.execute_query('publish', {'body': {'event': 'user_registered'}})

# RabbitMQ - Consume
messages = client.execute_query('consume', {'max_messages': 5})

# SQS - Send
client.execute_query('send', {'body': 'Message content'})

# SQS - Receive
messages = client.execute_query('receive', {'max_messages': 10})

# SNS - Publish
client.execute_query('publish', {'message': 'Alert!', 'subject': 'System Alert'})
```

## Testing Examples Locally

### Prerequisites for Full Testing

To run all examples successfully, you would need:

- PostgreSQL running on localhost:5432
- MySQL running on localhost:3306
- SQL Server running on localhost:1433
- Cassandra running on localhost:9042
- OpenSearch running on localhost:9200
- Solr running on localhost:8983
- Kafka running on localhost:9092
- RabbitMQ running on localhost:5672
- AWS credentials for Athena, SQS, SNS

### Quick Local Testing with Docker

```bash
# PostgreSQL
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=password postgres

# MySQL
docker run -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=password mysql

# RabbitMQ
docker run -d -p 5672:5672 rabbitmq

# Kafka (with Zookeeper)
docker-compose up -d  # Use docker-compose.yml with Kafka + Zookeeper
```

### Testing Without Services

**Schema transformation examples** work without any services:

```bash
./venv/bin/python3 examples/schema_transformation_example.py
```

**SQLite example** works without external services:

```python
from examples.all_clients_examples import example_sqlite
example_sqlite()
```

## Integration with Schema Transformer

The examples show how to combine clients with schema transformation:

```python
# Define schema
schema = {
    'type': 'object',
    'properties': {
        'user_id': {'type': 'integer', 'source': 'id'},
        'full_name': {'type': 'string', 'source': 'name'}
    }
}

# Query and transform
client = ClientFactory.create_from_config_file('config.yaml', 'my_db')
transformer = SchemaTransformer(schema)

with client:
    raw_data = client.execute_query("SELECT * FROM users")
    normalized_data = transformer.transform(raw_data)
```

## Customizing Examples

### Modify Configuration

Edit the config dictionaries in each example function:

```python
config = {
    'host': 'your-host',
    'port': your-port,
    'database': 'your-database',
    # ...
}
```

### Add Your Own Queries

Add queries to the `queries` list in each example:

```python
queries = [
    "SELECT * FROM your_table;",
    "SELECT COUNT(*) FROM your_table WHERE condition;",
    # Add more queries
]
```

### Use Configuration Files

Instead of hardcoded configs, use configuration files:

```python
client = ClientFactory.create_from_config_file('config/sources.yaml', 'my_postgres')
```

## Error Handling Patterns

All examples demonstrate proper error handling:

```python
try:
    # Client operations
    with client:
        results = client.execute_query(query)
except ClientConnectionError as e:
    # Handle connection errors
    print(f"Connection failed: {e}")
except QueryExecutionError as e:
    # Handle query errors
    print(f"Query failed: {e}")
except Exception as e:
    # Handle unexpected errors
    print(f"Unexpected error: {e}")
```

## Logging

Examples use the logging system. Adjust log level:

```python
from src.logging_config import setup_logging

# Debug level for detailed logs
setup_logging(level='DEBUG')

# Info level (default)
setup_logging(level='INFO')

# Error level only
setup_logging(level='ERROR')
```

## Summary

- ✅ **12 client types** with complete examples
- ✅ **Connection management** demonstrated
- ✅ **Query execution** patterns shown
- ✅ **Error handling** best practices
- ✅ **Schema transformation** integration
- ✅ **Resource cleanup** with context managers
- ✅ **Logging** throughout all examples

All examples follow the same patterns for consistency and ease of learning!

## Next Steps

1. Review the examples for your specific client type
2. Modify configurations for your environment
3. Run examples to verify connectivity
4. Integrate patterns into your application
5. Add schema transformation for data normalization

For detailed documentation, see:
- `../README.md` - Main project documentation
- `../SCHEMA_TRANSFORMER.md` - Schema transformation guide
- `../ERROR_HANDLING_AND_LOGGING.md` - Error handling guide
