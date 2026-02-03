"""
Comprehensive Examples for All Client Types

This file demonstrates how to use every client type in the Factory Ingestion library.
Each example shows connection, query execution, and proper resource management.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.client_factory import ClientFactory, ClientBuilder
from src.schema_transformer import SchemaTransformer
from src.logging_config import setup_logging
from src.exceptions import ConnectionError as ClientConnectionError

# Setup logging
setup_logging(level='INFO')


def example_postgresql():
    """Example: PostgreSQL Client"""
    print("\n" + "=" * 70)
    print("PostgreSQL Client Example")
    print("=" * 70)
    
    # Using Builder pattern
    config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'mydb',
        'username': 'postgres',
        'password': 'password'
    }
    
    try:
        client = ClientBuilder().with_source_type('postgres').with_config(config).build()
        
        with client:
            print(f"✓ Connected to PostgreSQL: {client.is_connected()}")
            
            # Example queries
            queries = [
                "SELECT version();",
                "SELECT current_database();",
                "SELECT * FROM users LIMIT 5;",
                "SELECT COUNT(*) as total FROM users;"
            ]
            
            for query in queries:
                print(f"\nQuery: {query}")
                try:
                    results = client.execute_query(query)
                    print(f"Results: {len(results)} rows")
                    if results:
                        print(f"Sample: {results[0]}")
                except Exception as e:
                    print(f"Query failed (expected if table doesn't exist): {e}")
        
        print("\n✓ PostgreSQL example completed")
        
    except ClientConnectionError as e:
        print(f"❌ Connection failed (expected if PostgreSQL not running): {e}")


def example_mysql():
    """Example: MySQL Client"""
    print("\n" + "=" * 70)
    print("MySQL Client Example")
    print("=" * 70)
    
    config = {
        'host': 'localhost',
        'port': 3306,
        'database': 'mydb',
        'username': 'root',
        'password': 'password'
    }
    
    try:
        client = ClientFactory.create_client('mysql', config)
        
        with client:
            print(f"✓ Connected to MySQL: {client.is_connected()}")
            
            queries = [
                "SELECT VERSION();",
                "SELECT DATABASE();",
                "SHOW TABLES;",
                "SELECT * FROM customers LIMIT 5;"
            ]
            
            for query in queries:
                print(f"\nQuery: {query}")
                try:
                    results = client.execute_query(query)
                    print(f"Results: {len(results)} rows")
                except Exception as e:
                    print(f"Query failed: {e}")
        
        print("\n✓ MySQL example completed")
        
    except ClientConnectionError as e:
        print(f"❌ Connection failed (expected if MySQL not running): {e}")


def example_sqlserver():
    """Example: SQL Server Client"""
    print("\n" + "=" * 70)
    print("SQL Server Client Example")
    print("=" * 70)
    
    config = {
        'host': 'localhost',
        'port': 1433,
        'database': 'mydb',
        'username': 'sa',
        'password': 'YourStrong@Passw0rd',
        'driver': '{ODBC Driver 17 for SQL Server}'
    }
    
    try:
        client = ClientFactory.create_client('sqlserver', config)
        
        with client:
            print(f"✓ Connected to SQL Server: {client.is_connected()}")
            
            queries = [
                "SELECT @@VERSION;",
                "SELECT DB_NAME();",
                "SELECT * FROM sys.tables;",
                "SELECT TOP 5 * FROM orders;"
            ]
            
            for query in queries:
                print(f"\nQuery: {query}")
                try:
                    results = client.execute_query(query)
                    print(f"Results: {len(results)} rows")
                except Exception as e:
                    print(f"Query failed: {e}")
        
        print("\n✓ SQL Server example completed")
        
    except ClientConnectionError as e:
        print(f"❌ Connection failed (expected if SQL Server not running): {e}")


def example_sqlite():
    """Example: SQLite Client"""
    print("\n" + "=" * 70)
    print("SQLite Client Example")
    print("=" * 70)
    
    # SQLite uses in-memory database for this example
    config = {
        'database_path': ':memory:'
    }
    
    try:
        client = ClientFactory.create_client('sqlite', config)
        
        with client:
            print(f"✓ Connected to SQLite: {client.is_connected()}")
            
            # Create a sample table
            client.execute_query("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    email TEXT
                )
            """)
            print("✓ Created users table")
            
            # Insert sample data
            client.execute_query(
                "INSERT INTO users (name, email) VALUES (?, ?)",
                {'name': 'Alice', 'email': 'alice@example.com'}
            )
            print("✓ Inserted sample data")
            
            # Query data
            results = client.execute_query("SELECT * FROM users")
            print(f"\nQuery results: {len(results)} rows")
            for row in results:
                print(f"  {row}")
        
        print("\n✓ SQLite example completed")
        
    except Exception as e:
        print(f"❌ Error: {e}")


def example_athena():
    """Example: AWS Athena Client"""
    print("\n" + "=" * 70)
    print("AWS Athena Client Example")
    print("=" * 70)
    
    config = {
        'region_name': 'us-east-1',
        'database': 'mydb',
        's3_output_location': 's3://my-bucket/athena-results/',
        'aws_access_key_id': 'YOUR_ACCESS_KEY',
        'aws_secret_access_key': 'YOUR_SECRET_KEY'
    }
    
    try:
        client = ClientFactory.create_client('athena', config)
        
        with client:
            print(f"✓ Connected to Athena: {client.is_connected()}")
            
            queries = [
                "SHOW DATABASES;",
                "SHOW TABLES;",
                "SELECT * FROM my_table LIMIT 10;"
            ]
            
            for query in queries:
                print(f"\nQuery: {query}")
                try:
                    results = client.execute_query(query)
                    print(f"Results: {len(results)} rows")
                except Exception as e:
                    print(f"Query failed: {e}")
        
        print("\n✓ Athena example completed")
        
    except ClientConnectionError as e:
        print(f"❌ Connection failed (expected without AWS credentials): {e}")


def example_cassandra():
    """Example: Cassandra Client"""
    print("\n" + "=" * 70)
    print("Cassandra Client Example")
    print("=" * 70)
    
    config = {
        'hosts': ['localhost'],
        'port': 9042,
        'keyspace': 'mykeyspace',
        'username': 'cassandra',
        'password': 'cassandra'
    }
    
    try:
        client = ClientFactory.create_client('cassandra', config)
        
        with client:
            print(f"✓ Connected to Cassandra: {client.is_connected()}")
            
            queries = [
                "SELECT release_version FROM system.local;",
                "SELECT * FROM users LIMIT 5;",
                "SELECT COUNT(*) FROM events;"
            ]
            
            for query in queries:
                print(f"\nQuery: {query}")
                try:
                    results = client.execute_query(query)
                    print(f"Results: {len(results)} rows")
                except Exception as e:
                    print(f"Query failed: {e}")
        
        print("\n✓ Cassandra example completed")
        
    except ClientConnectionError as e:
        print(f"❌ Connection failed (expected if Cassandra not running): {e}")


def example_opensearch():
    """Example: OpenSearch Client"""
    print("\n" + "=" * 70)
    print("OpenSearch Client Example")
    print("=" * 70)
    
    config = {
        'hosts': ['localhost:9200'],
        'username': 'admin',
        'password': 'admin',
        'use_ssl': True,
        'verify_certs': False
    }
    
    try:
        client = ClientFactory.create_client('opensearch', config)
        
        with client:
            print(f"✓ Connected to OpenSearch: {client.is_connected()}")
            
            # Search queries
            queries = [
                ('*', {'index': 'logs', 'body': {'size': 10}}),
                ('error', {'index': 'logs', 'body': {'size': 5}}),
                ('status:200', {'index': 'access-logs'})
            ]
            
            for query, params in queries:
                print(f"\nSearch: {query} with params: {params}")
                try:
                    results = client.execute_query(query, params)
                    print(f"Results: {len(results)} documents")
                except Exception as e:
                    print(f"Search failed: {e}")
        
        print("\n✓ OpenSearch example completed")
        
    except ClientConnectionError as e:
        print(f"❌ Connection failed (expected if OpenSearch not running): {e}")


def example_solr():
    """Example: Apache Solr Client"""
    print("\n" + "=" * 70)
    print("Apache Solr Client Example")
    print("=" * 70)
    
    config = {
        'url': 'http://localhost:8983/solr/mycore',
        'timeout': 10
    }
    
    try:
        client = ClientFactory.create_client('solr', config)
        
        with client:
            print(f"✓ Connected to Solr: {client.is_connected()}")
            
            # Solr queries
            queries = [
                ('*:*', {'rows': 10}),
                ('title:python', {'rows': 5}),
                ('category:technology', {'rows': 20, 'sort': 'date desc'})
            ]
            
            for query, params in queries:
                print(f"\nQuery: {query} with params: {params}")
                try:
                    results = client.execute_query(query, params)
                    print(f"Results: {len(results)} documents")
                except Exception as e:
                    print(f"Query failed: {e}")
        
        print("\n✓ Solr example completed")
        
    except ClientConnectionError as e:
        print(f"❌ Connection failed (expected if Solr not running): {e}")


def example_kafka():
    """Example: Kafka Client"""
    print("\n" + "=" * 70)
    print("Kafka Client Example")
    print("=" * 70)
    
    config = {
        'bootstrap_servers': ['localhost:9092'],
        'topic': 'my-topic',
        'group_id': 'my-consumer-group',
        'auto_offset_reset': 'earliest'
    }
    
    try:
        client = ClientFactory.create_client('kafka', config)
        
        with client:
            print(f"✓ Connected to Kafka: {client.is_connected()}")
            
            # Produce messages
            messages = [
                {'key': 'user1', 'value': {'action': 'login', 'timestamp': '2024-01-01T10:00:00'}},
                {'key': 'user2', 'value': {'action': 'purchase', 'amount': 99.99}}
            ]
            
            print("\nProducing messages:")
            for msg in messages:
                try:
                    result = client.execute_query('produce', msg)
                    print(f"  ✓ Produced: {msg['key']}")
                except Exception as e:
                    print(f"  ❌ Failed to produce: {e}")
            
            # Consume messages
            print("\nConsuming messages:")
            try:
                results = client.execute_query('consume', {'max_messages': 5, 'timeout': 5})
                print(f"  Consumed {len(results)} messages")
                for msg in results:
                    print(f"    - {msg}")
            except Exception as e:
                print(f"  Failed to consume: {e}")
        
        print("\n✓ Kafka example completed")
        
    except ClientConnectionError as e:
        print(f"❌ Connection failed (expected if Kafka not running): {e}")


def example_rabbitmq():
    """Example: RabbitMQ Client"""
    print("\n" + "=" * 70)
    print("RabbitMQ Client Example")
    print("=" * 70)
    
    config = {
        'host': 'localhost',
        'port': 5672,
        'username': 'guest',
        'password': 'guest',
        'virtual_host': '/',
        'queue': 'my-queue',
        'exchange': ''
    }
    
    try:
        client = ClientFactory.create_client('rabbitmq', config)
        
        with client:
            print(f"✓ Connected to RabbitMQ: {client.is_connected()}")
            
            # Publish messages
            messages = [
                {'body': {'event': 'user_registered', 'user_id': 123}},
                {'body': {'event': 'order_placed', 'order_id': 456}, 'properties': {'priority': 5}}
            ]
            
            print("\nPublishing messages:")
            for msg in messages:
                try:
                    client.execute_query('publish', msg)
                    print(f"  ✓ Published: {msg['body']}")
                except Exception as e:
                    print(f"  ❌ Failed to publish: {e}")
            
            # Consume messages
            print("\nConsuming messages:")
            try:
                results = client.execute_query('consume', {'max_messages': 5})
                print(f"  Consumed {len(results)} messages")
                for msg in results:
                    print(f"    - {msg.get('body')}")
            except Exception as e:
                print(f"  Failed to consume: {e}")
        
        print("\n✓ RabbitMQ example completed")
        
    except ClientConnectionError as e:
        print(f"❌ Connection failed (expected if RabbitMQ not running): {e}")


def example_sqs():
    """Example: AWS SQS Client"""
    print("\n" + "=" * 70)
    print("AWS SQS Client Example")
    print("=" * 70)
    
    config = {
        'region_name': 'us-east-1',
        'queue_url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'aws_access_key_id': 'YOUR_ACCESS_KEY',
        'aws_secret_access_key': 'YOUR_SECRET_KEY'
    }
    
    try:
        client = ClientFactory.create_client('sqs', config)
        
        with client:
            print(f"✓ Connected to SQS: {client.is_connected()}")
            
            # Send messages
            messages = [
                {'body': 'Hello from SQS!', 'attributes': {'Type': 'Notification'}},
                {'body': 'Order #12345 processed', 'attributes': {'Priority': 'High'}}
            ]
            
            print("\nSending messages:")
            for msg in messages:
                try:
                    result = client.execute_query('send', msg)
                    print(f"  ✓ Sent: {msg['body']}")
                except Exception as e:
                    print(f"  ❌ Failed to send: {e}")
            
            # Receive messages
            print("\nReceiving messages:")
            try:
                results = client.execute_query('receive', {'max_messages': 5})
                print(f"  Received {len(results)} messages")
                for msg in results:
                    print(f"    - {msg.get('Body')}")
            except Exception as e:
                print(f"  Failed to receive: {e}")
        
        print("\n✓ SQS example completed")
        
    except ClientConnectionError as e:
        print(f"❌ Connection failed (expected without AWS credentials): {e}")


def example_sns():
    """Example: AWS SNS Client"""
    print("\n" + "=" * 70)
    print("AWS SNS Client Example")
    print("=" * 70)
    
    config = {
        'region_name': 'us-east-1',
        'topic_arn': 'arn:aws:sns:us-east-1:123456789012:my-topic',
        'aws_access_key_id': 'YOUR_ACCESS_KEY',
        'aws_secret_access_key': 'YOUR_SECRET_KEY'
    }
    
    try:
        client = ClientFactory.create_client('sns', config)
        
        with client:
            print(f"✓ Connected to SNS: {client.is_connected()}")
            
            # List subscriptions
            print("\nListing subscriptions:")
            try:
                subscriptions = client.execute_query('list_subscriptions')
                print(f"  Found {len(subscriptions)} subscriptions")
            except Exception as e:
                print(f"  Failed to list: {e}")
            
            # Publish messages
            messages = [
                {'message': 'System alert: High CPU usage', 'subject': 'Alert'},
                {'message': 'Daily report ready', 'subject': 'Report'}
            ]
            
            print("\nPublishing messages:")
            for msg in messages:
                try:
                    result = client.execute_query('publish', msg)
                    print(f"  ✓ Published: {msg['subject']}")
                except Exception as e:
                    print(f"  ❌ Failed to publish: {e}")
        
        print("\n✓ SNS example completed")
        
    except ClientConnectionError as e:
        print(f"❌ Connection failed (expected without AWS credentials): {e}")


def example_with_schema_transformation():
    """Example: Using Schema Transformer with Multiple Clients"""
    print("\n" + "=" * 70)
    print("Schema Transformation Example - Normalizing Multi-Source Data")
    print("=" * 70)
    
    # Define unified schema for user data
    user_schema = {
        'type': 'object',
        'properties': {
            'user_id': {'type': 'integer'},
            'full_name': {'type': 'string'},
            'email': {'type': 'string', 'format': 'email'},
            'is_active': {'type': 'boolean'}
        }
    }
    
    transformer = SchemaTransformer(user_schema)
    
    # Simulate data from different sources with different structures
    postgres_data = [
        {'user_id': '1', 'full_name': 'Alice Johnson', 'email': 'alice@example.com', 'is_active': 1}
    ]
    
    mysql_data = [
        {'user_id': '2', 'full_name': 'Bob Smith', 'email': '  BOB@EXAMPLE.COM  ', 'is_active': True}
    ]
    
    sqlite_data = [
        {'user_id': 3, 'full_name': 'Charlie Brown', 'email': 'charlie@example.com', 'is_active': '1'}
    ]
    
    # Transform all data to consistent format
    normalized_pg = transformer.transform(postgres_data)
    normalized_mysql = transformer.transform(mysql_data)
    normalized_sqlite = transformer.transform(sqlite_data)
    
    # Combine all data
    all_users = normalized_pg + normalized_mysql + normalized_sqlite
    
    print("\nNormalized user data from multiple sources:")
    for user in all_users:
        print(f"  User {user['user_id']}: {user['full_name']} ({user['email']}) - Active: {user['is_active']}")
    
    print("\n✓ All data normalized to consistent format!")


def main():
    """Run all client examples."""
    print("\n" + "=" * 80)
    print(" " * 20 + "FACTORY INGESTION - ALL CLIENTS EXAMPLES")
    print("=" * 80)
    print("\nThis demonstrates all 12 client types in the library.")
    print("Note: Most examples will show connection errors if services aren't running.")
    print("This is expected and demonstrates proper error handling.\n")
    
    examples = [
        ("Database Clients", [
            example_postgresql,
            example_mysql,
            example_sqlserver,
            example_sqlite,
            example_athena,
            example_cassandra
        ]),
        ("Search Engine Clients", [
            example_opensearch,
            example_solr
        ]),
        ("Messaging Clients", [
            example_kafka,
            example_rabbitmq,
            example_sqs,
            example_sns
        ]),
        ("Advanced Features", [
            example_with_schema_transformation
        ])
    ]
    
    for category, category_examples in examples:
        print("\n" + "=" * 80)
        print(f" {category}")
        print("=" * 80)
        
        for example_func in category_examples:
            try:
                example_func()
            except Exception as e:
                print(f"\n❌ Example failed with unexpected error: {e}")
                import traceback
                traceback.print_exc()
    
    print("\n" + "=" * 80)
    print(" " * 25 + "ALL EXAMPLES COMPLETED!")
    print("=" * 80)
    print("\nSummary:")
    print("  ✓ 12 client types demonstrated")
    print("  ✓ Connection management shown")
    print("  ✓ Query execution examples provided")
    print("  ✓ Error handling demonstrated")
    print("  ✓ Schema transformation integrated")
    print("\nSee individual example functions for detailed usage patterns.")
    print("=" * 80 + "\n")


if __name__ == '__main__':
    main()
