"""
Client Examples with Schema Transformation

This file demonstrates how to use Schema Transformer with every client type
to normalize data from different sources into consistent formats.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.client_factory import ClientFactory, ClientBuilder
from src.schema_transformer import SchemaTransformer, SchemaRegistry
from src.logging_config import setup_logging
from src.exceptions import ConnectionError as ClientConnectionError

# Setup logging
setup_logging(level='INFO')


def example_postgres_with_schema():
    """PostgreSQL with Schema Transformation"""
    print("\n" + "=" * 70)
    print("PostgreSQL + Schema Transformation")
    print("=" * 70)
    
    # Define schema for normalized user data
    user_schema = {
        'type': 'object',
        'properties': {
            'user_id': {'type': 'integer', 'source': 'id'},
            'full_name': {'type': 'string', 'source': 'name'},
            'email': {'type': 'string', 'format': 'email', 'source': 'email'},
            'created_date': {'type': 'string', 'format': 'date', 'source': 'created_at'},
            'is_active': {'type': 'boolean', 'source': 'active'}
        }
    }
    
    transformer = SchemaTransformer(user_schema)
    
    # Simulate PostgreSQL response
    raw_data = [
        {'id': '1', 'name': 'Alice Johnson', 'email': '  ALICE@EXAMPLE.COM  ', 'created_at': '2024-01-15', 'active': 1},
        {'id': '2', 'name': 'Bob Smith', 'email': 'bob@example.com', 'created_at': '2024-02-20', 'active': 0}
    ]
    
    print("\nRaw PostgreSQL data:")
    for row in raw_data:
        print(f"  {row}")
    
    # Transform to normalized format
    normalized = transformer.transform(raw_data)
    
    print("\nNormalized data:")
    for user in normalized:
        print(f"  User {user['user_id']}: {user['full_name']} ({user['email']})")
        print(f"    Created: {user['created_date']}, Active: {user['is_active']}")
    
    print("\n✓ PostgreSQL data normalized successfully")


def example_mysql_with_schema():
    """MySQL with Schema Transformation"""
    print("\n" + "=" * 70)
    print("MySQL + Schema Transformation")
    print("=" * 70)
    
    # MySQL has different field names
    customer_schema = {
        'type': 'object',
        'properties': {
            'user_id': {'type': 'integer', 'source': 'customer_id'},
            'full_name': {'type': 'string', 'source': 'full_name'},
            'email': {'type': 'string', 'format': 'email', 'source': 'email_address'},
            'created_date': {'type': 'string', 'format': 'date', 'source': 'registration_date'},
            'is_active': {'type': 'boolean', 'source': 'status'}
        }
    }
    
    transformer = SchemaTransformer(customer_schema)
    
    # Simulate MySQL response with different field names
    raw_data = [
        {'customer_id': 101, 'full_name': 'Charlie Brown', 'email_address': 'CHARLIE@EXAMPLE.COM', 'registration_date': '2024-03-10', 'status': True},
        {'customer_id': 102, 'full_name': 'Diana Prince', 'email_address': '  diana@example.com  ', 'registration_date': '2024-04-05', 'status': False}
    ]
    
    print("\nRaw MySQL data (different field names):")
    for row in raw_data:
        print(f"  {row}")
    
    # Transform to same normalized format as PostgreSQL
    normalized = transformer.transform(raw_data)
    
    print("\nNormalized data (same format as PostgreSQL):")
    for user in normalized:
        print(f"  User {user['user_id']}: {user['full_name']} ({user['email']})")
        print(f"    Created: {user['created_date']}, Active: {user['is_active']}")
    
    print("\n✓ MySQL data normalized to same format as PostgreSQL")


def example_sqlite_with_schema():
    """SQLite with Schema Transformation - Working Example"""
    print("\n" + "=" * 70)
    print("SQLite + Schema Transformation (Working Example)")
    print("=" * 70)
    
    # Define schema
    product_schema = {
        'type': 'object',
        'properties': {
            'product_id': {'type': 'integer', 'source': 'id'},
            'product_name': {'type': 'string', 'source': 'name'},
            'price': {'type': 'number', 'source': 'price'},
            'discounted_price': {'type': 'number', 'source': 'price'},
            'in_stock': {'type': 'boolean', 'source': 'stock'}
        }
    }
    
    transformer = SchemaTransformer(product_schema)
    
    # Add custom transformer for discount calculation
    transformer.add_custom_transformer('discounted_price', lambda x: x * 0.85)
    
    config = {'database_path': ':memory:'}
    
    try:
        client = ClientFactory.create_client('sqlite', config)
        
        with client:
            # Create and populate table
            client.execute_query("""
                CREATE TABLE products (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    price REAL,
                    stock INTEGER
                )
            """)
            
            client.execute_query("INSERT INTO products VALUES (1, 'Laptop', 999.99, 1)")
            client.execute_query("INSERT INTO products VALUES (2, 'Mouse', 29.99, 1)")
            client.execute_query("INSERT INTO products VALUES (3, 'Keyboard', 79.99, 0)")
            
            # Query data
            raw_data = client.execute_query("SELECT * FROM products")
            
            print("\nRaw SQLite data:")
            for row in raw_data:
                print(f"  {row}")
            
            # Transform with automatic discount calculation
            normalized = transformer.transform(raw_data)
            
            print("\nNormalized data with calculated discounts:")
            for product in normalized:
                print(f"  {product['product_name']}:")
                print(f"    Price: ${product['price']:.2f}")
                print(f"    Discounted: ${product['discounted_price']:.2f} (15% off)")
                print(f"    In Stock: {product['in_stock']}")
        
        print("\n✓ SQLite data transformed with custom calculations")
        
    except Exception as e:
        print(f"❌ Error: {e}")


def example_cassandra_with_schema():
    """Cassandra with Schema Transformation"""
    print("\n" + "=" * 70)
    print("Cassandra + Schema Transformation")
    print("=" * 70)
    
    # Cassandra event data schema
    event_schema = {
        'type': 'object',
        'properties': {
            'event_id': {'type': 'string', 'source': 'id'},
            'event_type': {'type': 'string', 'source': 'type'},
            'user_id': {'type': 'integer', 'source': 'user.id'},
            'user_name': {'type': 'string', 'source': 'user.name'},
            'timestamp': {'type': 'string', 'format': 'date-time', 'source': 'created_at'},
            'metadata': {'type': 'object', 'source': 'meta'}
        }
    }
    
    transformer = SchemaTransformer(event_schema)
    
    # Simulate nested Cassandra response
    raw_data = [
        {
            'id': 'evt-001',
            'type': 'login',
            'user': {'id': '123', 'name': 'Alice'},
            'created_at': '2024-01-15T10:30:00Z',
            'meta': {'ip': '192.168.1.1', 'device': 'mobile'}
        },
        {
            'id': 'evt-002',
            'type': 'purchase',
            'user': {'id': '456', 'name': 'Bob'},
            'created_at': '2024-01-15T11:45:00Z',
            'meta': {'amount': 99.99, 'currency': 'USD'}
        }
    ]
    
    print("\nRaw Cassandra data (nested structure):")
    for row in raw_data:
        print(f"  {row}")
    
    # Transform and flatten
    normalized = transformer.transform(raw_data)
    
    print("\nNormalized and flattened data:")
    for event in normalized:
        print(f"  Event {event['event_id']} ({event['event_type']}):")
        print(f"    User: {event['user_name']} (ID: {event['user_id']})")
        print(f"    Time: {event['timestamp']}")
        print(f"    Metadata: {event['metadata']}")
    
    print("\n✓ Cassandra nested data flattened successfully")


def example_opensearch_with_schema():
    """OpenSearch with Schema Transformation"""
    print("\n" + "=" * 70)
    print("OpenSearch + Schema Transformation")
    print("=" * 70)
    
    # Log entry schema
    log_schema = {
        'type': 'object',
        'properties': {
            'log_id': {'type': 'string', 'source': '_id'},
            'timestamp': {'type': 'string', 'format': 'date-time', 'source': '@timestamp'},
            'level': {'type': 'string', 'source': 'level'},
            'message': {'type': 'string', 'source': 'message'},
            'service': {'type': 'string', 'source': 'service.name'},
            'host': {'type': 'string', 'source': 'host.name'}
        }
    }
    
    transformer = SchemaTransformer(log_schema)
    
    # Simulate OpenSearch response
    raw_data = [
        {
            '_id': 'log-001',
            '@timestamp': '2024-01-15T10:00:00Z',
            'level': 'ERROR',
            'message': 'Database connection failed',
            'service': {'name': 'api-server', 'version': '1.0'},
            'host': {'name': 'server-01', 'ip': '10.0.0.1'}
        },
        {
            '_id': 'log-002',
            '@timestamp': '2024-01-15T10:05:00Z',
            'level': 'INFO',
            'message': 'Request processed successfully',
            'service': {'name': 'api-server', 'version': '1.0'},
            'host': {'name': 'server-02', 'ip': '10.0.0.2'}
        }
    ]
    
    print("\nRaw OpenSearch data:")
    for row in raw_data[:1]:  # Show first one
        print(f"  {row}")
    
    # Transform
    normalized = transformer.transform(raw_data)
    
    print("\nNormalized log entries:")
    for log in normalized:
        print(f"  [{log['level']}] {log['timestamp']}")
        print(f"    {log['message']}")
        print(f"    Service: {log['service']}, Host: {log['host']}")
    
    print("\n✓ OpenSearch logs normalized successfully")


def example_kafka_with_schema():
    """Kafka with Schema Transformation"""
    print("\n" + "=" * 70)
    print("Kafka + Schema Transformation")
    print("=" * 70)
    
    # Event message schema
    message_schema = {
        'type': 'object',
        'properties': {
            'message_id': {'type': 'string', 'source': 'key'},
            'event_type': {'type': 'string', 'source': 'value.event'},
            'user_id': {'type': 'integer', 'source': 'value.user_id'},
            'timestamp': {'type': 'string', 'format': 'date-time', 'source': 'value.timestamp'},
            'data': {'type': 'object', 'source': 'value.data'}
        }
    }
    
    transformer = SchemaTransformer(message_schema)
    
    # Simulate Kafka messages
    raw_messages = [
        {
            'key': 'msg-001',
            'value': {
                'event': 'user_login',
                'user_id': '123',
                'timestamp': '2024-01-15T10:00:00Z',
                'data': {'ip': '192.168.1.1', 'device': 'mobile'}
            },
            'offset': 100,
            'partition': 0
        },
        {
            'key': 'msg-002',
            'value': {
                'event': 'order_placed',
                'user_id': '456',
                'timestamp': '2024-01-15T10:30:00Z',
                'data': {'order_id': 'ORD-789', 'amount': 99.99}
            },
            'offset': 101,
            'partition': 0
        }
    ]
    
    print("\nRaw Kafka messages:")
    for msg in raw_messages[:1]:
        print(f"  {msg}")
    
    # Transform
    normalized = transformer.transform(raw_messages)
    
    print("\nNormalized events:")
    for event in normalized:
        print(f"  Event: {event['event_type']} (ID: {event['message_id']})")
        print(f"    User: {event['user_id']}, Time: {event['timestamp']}")
        print(f"    Data: {event['data']}")
    
    print("\n✓ Kafka messages normalized successfully")


def example_multi_source_normalization():
    """Multi-Source Data Normalization"""
    print("\n" + "=" * 70)
    print("Multi-Source Data Normalization - The Power of Schema Transformation")
    print("=" * 70)
    
    # Define ONE unified schema for user data
    unified_user_schema = {
        'type': 'object',
        'properties': {
            'user_id': {'type': 'integer'},
            'full_name': {'type': 'string'},
            'email': {'type': 'string', 'format': 'email'},
            'created_date': {'type': 'string'},
            'is_active': {'type': 'boolean'},
            'source_system': {'type': 'string', 'default': 'unknown'}
        }
    }
    
    # Create source-specific schemas with field mappings
    postgres_schema = {
        'type': 'object',
        'properties': {
            'user_id': {'type': 'integer', 'source': 'id'},
            'full_name': {'type': 'string', 'source': 'name'},
            'email': {'type': 'string', 'format': 'email', 'source': 'email'},
            'created_date': {'type': 'string', 'source': 'created_at'},
            'is_active': {'type': 'boolean', 'source': 'active'},
            'source_system': {'type': 'string', 'default': 'PostgreSQL'}
        }
    }
    
    mysql_schema = {
        'type': 'object',
        'properties': {
            'user_id': {'type': 'integer', 'source': 'customer_id'},
            'full_name': {'type': 'string', 'source': 'full_name'},
            'email': {'type': 'string', 'format': 'email', 'source': 'email_addr'},
            'created_date': {'type': 'string', 'source': 'reg_date'},
            'is_active': {'type': 'boolean', 'source': 'status'},
            'source_system': {'type': 'string', 'default': 'MySQL'}
        }
    }
    
    mongo_schema = {
        'type': 'object',
        'properties': {
            'user_id': {'type': 'integer', 'source': '_id'},
            'full_name': {'type': 'string', 'source': 'profile.name'},
            'email': {'type': 'string', 'format': 'email', 'source': 'profile.email'},
            'created_date': {'type': 'string', 'source': 'metadata.created'},
            'is_active': {'type': 'boolean', 'source': 'metadata.active'},
            'source_system': {'type': 'string', 'default': 'MongoDB'}
        }
    }
    
    # Create transformers
    pg_transformer = SchemaTransformer(postgres_schema)
    mysql_transformer = SchemaTransformer(mysql_schema)
    mongo_transformer = SchemaTransformer(mongo_schema)
    
    # Simulate data from different sources
    print("\n1. PostgreSQL data (fields: id, name, email, created_at, active):")
    postgres_data = [
        {'id': '1', 'name': 'Alice Johnson', 'email': '  ALICE@EXAMPLE.COM  ', 'created_at': '2024-01-15', 'active': 1}
    ]
    print(f"   {postgres_data[0]}")
    
    print("\n2. MySQL data (fields: customer_id, full_name, email_addr, reg_date, status):")
    mysql_data = [
        {'customer_id': 2, 'full_name': 'Bob Smith', 'email_addr': 'BOB@EXAMPLE.COM', 'reg_date': '2024-02-20', 'status': True}
    ]
    print(f"   {mysql_data[0]}")
    
    print("\n3. MongoDB data (nested structure):")
    mongo_data = [
        {
            '_id': 3,
            'profile': {'name': 'Charlie Brown', 'email': 'charlie@example.com'},
            'metadata': {'created': '2024-03-10', 'active': False}
        }
    ]
    print(f"   {mongo_data[0]}")
    
    # Transform all to unified format
    normalized_pg = pg_transformer.transform(postgres_data)
    normalized_mysql = mysql_transformer.transform(mysql_data)
    normalized_mongo = mongo_transformer.transform(mongo_data)
    
    # Combine all data
    all_users = normalized_pg + normalized_mysql + normalized_mongo
    
    print("\n" + "=" * 70)
    print("UNIFIED DATA - All sources normalized to same format:")
    print("=" * 70)
    for user in all_users:
        print(f"\nUser {user['user_id']} from {user['source_system']}:")
        print(f"  Name: {user['full_name']}")
        print(f"  Email: {user['email']}")
        print(f"  Created: {user['created_date']}")
        print(f"  Active: {user['is_active']}")
    
    print("\n✓ Successfully normalized data from 3 different sources!")
    print("  - Different field names → Unified")
    print("  - Different structures (flat vs nested) → Unified")
    print("  - Different types (string vs int vs bool) → Unified")
    print("  - All data now has identical structure!")


def example_schema_registry_workflow():
    """Schema Registry for Managing Multiple Schemas"""
    print("\n" + "=" * 70)
    print("Schema Registry - Centralized Schema Management")
    print("=" * 70)
    
    # Create registry
    registry = SchemaRegistry()
    
    # Register schemas for different data types
    user_schema = {
        'type': 'object',
        'properties': {
            'id': {'type': 'integer'},
            'name': {'type': 'string'},
            'email': {'type': 'string', 'format': 'email'}
        }
    }
    
    product_schema = {
        'type': 'object',
        'properties': {
            'sku': {'type': 'string'},
            'name': {'type': 'string'},
            'price': {'type': 'number'}
        }
    }
    
    order_schema = {
        'type': 'object',
        'properties': {
            'order_id': {'type': 'integer'},
            'user_id': {'type': 'integer'},
            'total': {'type': 'number'},
            'items': {'type': 'array'}
        }
    }
    
    # Register all schemas
    registry.register('users', user_schema)
    registry.register('products', product_schema)
    registry.register('orders', order_schema)
    
    print(f"\nRegistered schemas: {registry.list_schemas()}")
    
    # Simulate data from different clients
    user_data = {'id': '1', 'name': 'Alice', 'email': '  ALICE@EXAMPLE.COM  '}
    product_data = {'sku': 'LAP-001', 'name': 'Laptop', 'price': '999.99'}
    order_data = {'order_id': '100', 'user_id': '1', 'total': '999.99', 'items': ['LAP-001']}
    
    # Transform using registry
    normalized_user = registry.transform('users', user_data)
    normalized_product = registry.transform('products', product_data)
    normalized_order = registry.transform('orders', order_data)
    
    print("\nTransformed data:")
    print(f"  User: {normalized_user}")
    print(f"  Product: {normalized_product}")
    print(f"  Order: {normalized_order}")
    
    print("\n✓ Schema Registry allows centralized schema management!")


def main():
    """Run all schema transformation examples."""
    print("\n" + "=" * 80)
    print(" " * 15 + "SCHEMA TRANSFORMATION WITH ALL CLIENTS")
    print("=" * 80)
    print("\nDemonstrating how to normalize data from different sources")
    print("into consistent formats using Schema Transformer.\n")
    
    examples = [
        ("Database Clients with Schema Transformation", [
            example_postgres_with_schema,
            example_mysql_with_schema,
            example_sqlite_with_schema,
            example_cassandra_with_schema
        ]),
        ("Search Engine Clients with Schema Transformation", [
            example_opensearch_with_schema
        ]),
        ("Messaging Clients with Schema Transformation", [
            example_kafka_with_schema
        ]),
        ("Advanced Multi-Source Examples", [
            example_multi_source_normalization,
            example_schema_registry_workflow
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
                print(f"\n❌ Example failed: {e}")
                import traceback
                traceback.print_exc()
    
    print("\n" + "=" * 80)
    print(" " * 25 + "ALL EXAMPLES COMPLETED!")
    print("=" * 80)
    print("\nKey Takeaways:")
    print("  ✓ Schema Transformer normalizes data from ANY source")
    print("  ✓ Different field names → Unified format")
    print("  ✓ Different structures (flat/nested) → Unified format")
    print("  ✓ Automatic type conversion")
    print("  ✓ Custom transformations (calculations, formatting)")
    print("  ✓ Schema Registry for centralized management")
    print("  ✓ Perfect for multi-source data integration!")
    print("=" * 80 + "\n")


if __name__ == '__main__':
    main()
