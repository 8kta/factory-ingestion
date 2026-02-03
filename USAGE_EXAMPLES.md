# Usage Examples

This document provides detailed examples for using the Factory Ingestion client library.

## Table of Contents

1. [Database Examples](#database-examples)
2. [Search Engine Examples](#search-engine-examples)
3. [Messaging System Examples](#messaging-system-examples)
4. [AWS Service Examples](#aws-service-examples)
5. [Advanced Usage](#advanced-usage)

## Database Examples

### PostgreSQL

```python
from src.client_factory import ClientBuilder

# Using configuration file
client = ClientBuilder().with_config_file('config/sources.yaml', 'my_postgres').build()

with client:
    # Simple query
    results = client.execute_query("SELECT * FROM users WHERE active = true")
    
    # Parameterized query
    results = client.execute_query(
        "SELECT * FROM users WHERE id = %(user_id)s",
        {'user_id': 123}
    )
```

### MySQL

```python
client = ClientBuilder().with_config_file('config/sources.yaml', 'my_mysql').build()

with client:
    results = client.execute_query("SELECT * FROM products WHERE price > 100")
    for row in results:
        print(f"Product: {row['name']}, Price: {row['price']}")
```

### AWS Athena

```python
client = ClientBuilder().with_config_file('config/sources.yaml', 'my_athena').build()

with client:
    # Query S3 data using Athena
    query = """
        SELECT date, COUNT(*) as event_count
        FROM events
        WHERE date >= '2024-01-01'
        GROUP BY date
        ORDER BY date DESC
    """
    results = client.execute_query(query)
```

### SQLite

```python
client = ClientBuilder().with_config_file('config/sources.yaml', 'my_sqlite').build()

with client:
    # Create table
    client.execute_query("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY,
            message TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Query data
    results = client.execute_query("SELECT * FROM logs ORDER BY timestamp DESC LIMIT 100")
```

### Cassandra

```python
client = ClientBuilder().with_config_file('config/sources.yaml', 'my_cassandra').build()

with client:
    # Query with CQL
    results = client.execute_query("""
        SELECT * FROM user_events 
        WHERE user_id = ? AND event_date >= ?
    """, {'user_id': 'user123', 'event_date': '2024-01-01'})
```

## Search Engine Examples

### OpenSearch

```python
client = ClientBuilder().with_config_file('config/sources.yaml', 'my_opensearch').build()

with client:
    # Simple search
    results = client.execute_query(
        'status:active AND category:electronics',
        {'index': 'products', 'body': {'size': 20}}
    )
    
    # Complex search with aggregations
    results = client.execute_query(
        '',
        {
            'index': 'products',
            'body': {
                'query': {
                    'bool': {
                        'must': [
                            {'match': {'category': 'electronics'}},
                            {'range': {'price': {'gte': 100, 'lte': 1000}}}
                        ]
                    }
                },
                'aggs': {
                    'avg_price': {'avg': {'field': 'price'}}
                },
                'size': 50
            }
        }
    )
```

### Apache Solr

```python
client = ClientBuilder().with_config_file('config/sources.yaml', 'my_solr').build()

with client:
    # Basic search
    results = client.execute_query('title:python', {'rows': 10, 'sort': 'score desc'})
    
    # Faceted search
    results = client.execute_query(
        '*:*',
        {
            'rows': 20,
            'facet': 'true',
            'facet.field': ['category', 'author']
        }
    )
```

## Messaging System Examples

### Apache Kafka

```python
from src.client_factory import ClientBuilder

client = ClientBuilder().with_config_file('config/sources.yaml', 'my_kafka').build()

with client:
    # Consume messages
    messages = client.execute_query('', {'max_messages': 100, 'timeout_ms': 5000})
    
    for msg in messages:
        print(f"Topic: {msg['topic']}, Offset: {msg['offset']}")
        print(f"Value: {msg['value']}")
    
    # Produce messages
    client.send_message(
        topic='my_topic',
        message={'event': 'user_login', 'user_id': 123, 'timestamp': '2024-01-01T10:00:00'},
        key='user_123'
    )
```

### RabbitMQ

```python
client = ClientBuilder().with_config_file('config/sources.yaml', 'my_rabbitmq').build()

with client:
    # Publish message
    client.publish_message({
        'event_type': 'order_created',
        'order_id': 'ORD-12345',
        'amount': 99.99
    })
    
    # Consume messages
    messages = client.execute_query('', {'max_messages': 10})
    
    for msg in messages:
        print(f"Routing Key: {msg['routing_key']}")
        print(f"Body: {msg['body']}")
```

## AWS Service Examples

### AWS SQS

```python
client = ClientBuilder().with_config_file('config/sources.yaml', 'my_sqs').build()

with client:
    # Send message
    response = client.send_message(
        message_body='{"event": "user_signup", "user_id": 123}',
        message_attributes={
            'EventType': {'StringValue': 'signup', 'DataType': 'String'}
        }
    )
    print(f"Message ID: {response['MessageId']}")
    
    # Receive messages
    messages = client.execute_query('', {'max_messages': 10, 'wait_time': 5})
    
    for msg in messages:
        print(f"Message: {msg['Body']}")
        # Process message...
        # Delete after processing
        client.delete_message(msg['ReceiptHandle'])
```

### AWS SNS

```python
client = ClientBuilder().with_config_file('config/sources.yaml', 'my_sns').build()

with client:
    # Publish notification
    response = client.publish_message(
        message='System alert: High CPU usage detected',
        subject='Alert: System Performance',
        message_attributes={
            'severity': {
                'DataType': 'String',
                'StringValue': 'high'
            }
        }
    )
    print(f"Published message: {response['MessageId']}")
    
    # List subscriptions
    subscriptions = client.execute_query('')
    for sub in subscriptions:
        print(f"Endpoint: {sub['Endpoint']}, Protocol: {sub['Protocol']}")
```

## Advanced Usage

### Multiple Sources in One Application

```python
from src.client_factory import ClientFactory

# Load configuration once
config_path = 'config/sources.yaml'

# Create multiple clients
postgres_client = ClientFactory.create_from_config_file(config_path, 'my_postgres')
kafka_client = ClientFactory.create_from_config_file(config_path, 'my_kafka')
opensearch_client = ClientFactory.create_from_config_file(config_path, 'my_opensearch')

# Use them together
with postgres_client, kafka_client, opensearch_client:
    # Read from database
    users = postgres_client.execute_query("SELECT * FROM users WHERE created_today = true")
    
    # Send to Kafka
    for user in users:
        kafka_client.send_message('new_users', user)
    
    # Index in OpenSearch
    for user in users:
        opensearch_client.connection.index(
            index='users',
            id=user['id'],
            body=user
        )
```

### Dynamic Client Creation

```python
from src.client_factory import ClientBuilder

def get_client(source_type, config):
    """Factory function to create clients dynamically"""
    return (ClientBuilder()
            .with_source_type(source_type)
            .with_config(config)
            .build())

# Create clients based on runtime conditions
if environment == 'production':
    db_client = get_client('postgres', prod_config)
else:
    db_client = get_client('sqlite', {'database_path': 'test.db'})

with db_client:
    results = db_client.execute_query("SELECT * FROM data")
```

### Error Handling

```python
from src.client_factory import ClientBuilder

try:
    client = ClientBuilder().with_config_file('config/sources.yaml', 'my_postgres').build()
    
    with client:
        if not client.is_connected():
            raise ConnectionError("Failed to connect to database")
        
        results = client.execute_query("SELECT * FROM users")
        
except FileNotFoundError as e:
    print(f"Configuration file not found: {e}")
except ValueError as e:
    print(f"Configuration error: {e}")
except ConnectionError as e:
    print(f"Connection failed: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

### Custom Query Wrapper

```python
from src.client_factory import ClientFactory
from typing import List, Dict, Any

class DataService:
    def __init__(self, config_path: str, source_name: str):
        self.client = ClientFactory.create_from_config_file(config_path, source_name)
    
    def __enter__(self):
        self.client.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.disconnect()
    
    def get_users(self, active_only: bool = True) -> List[Dict[str, Any]]:
        query = "SELECT * FROM users"
        if active_only:
            query += " WHERE active = true"
        return self.client.execute_query(query)
    
    def get_user_by_id(self, user_id: int) -> Dict[str, Any]:
        results = self.client.execute_query(
            "SELECT * FROM users WHERE id = %(id)s",
            {'id': user_id}
        )
        return results[0] if results else None

# Usage
with DataService('config/sources.yaml', 'my_postgres') as service:
    users = service.get_users()
    user = service.get_user_by_id(123)
```

### Batch Processing

```python
from src.client_factory import ClientFactory

def batch_process_data(source_config, batch_size=1000):
    client = ClientFactory.create_from_config_file('config/sources.yaml', source_config)
    
    with client:
        offset = 0
        while True:
            # Fetch batch
            query = f"SELECT * FROM large_table LIMIT {batch_size} OFFSET {offset}"
            batch = client.execute_query(query)
            
            if not batch:
                break
            
            # Process batch
            for row in batch:
                process_row(row)
            
            offset += batch_size
            print(f"Processed {offset} rows")

def process_row(row):
    # Your processing logic here
    pass

# Run batch processing
batch_process_data('my_postgres', batch_size=5000)
```

### Environment-Specific Configuration

```python
import os
from src.client_factory import ClientBuilder

# Determine environment
env = os.getenv('ENVIRONMENT', 'development')

# Load environment-specific config
config_file = f'config/{env}_sources.yaml'

client = ClientBuilder().with_config_file(config_file, 'my_database').build()

with client:
    results = client.execute_query("SELECT * FROM data")
```
