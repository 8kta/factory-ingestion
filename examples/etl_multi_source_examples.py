"""
Multi-Source ETL Examples

This file demonstrates ETL pipelines using different reader clients:
- PostgreSQL → S3
- MySQL → MongoDB
- SQLite → CSV (fully working example!)
- Cassandra → OpenSearch
"""

import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.client_factory import ClientFactory
from src.writer_factory import WriterFactory
from src.schema_transformer import SchemaTransformer
from src.logging_config import setup_logging

# Setup logging
setup_logging(level='INFO')


def etl_postgres_to_s3():
    """ETL: PostgreSQL → Transform → S3"""
    print("\n" + "=" * 80)
    print("ETL: PostgreSQL → Transform → S3")
    print("=" * 80)
    
    # EXTRACT from PostgreSQL
    print("\n[EXTRACT] Reading from PostgreSQL...")
    
    pg_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'mydb',
        'username': 'postgres',
        'password': 'password'
    }
    
    try:
        pg_reader = ClientFactory.create_client('postgres', pg_config)
        
        with pg_reader:
            print(f"✓ Connected to PostgreSQL")
            
            # Extract users data
            raw_data = pg_reader.execute_query("""
                SELECT 
                    id, 
                    name, 
                    email, 
                    created_at, 
                    active 
                FROM users 
                WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
                ORDER BY created_at DESC
            """)
            
            print(f"✓ Extracted {len(raw_data)} records")
    
    except Exception as e:
        print(f"⚠ PostgreSQL not available: {e}")
        print("  Using simulated data...")
        raw_data = [
            {'id': 1, 'name': 'Alice', 'email': 'alice@example.com', 'created_at': '2024-01-15', 'active': True},
            {'id': 2, 'name': 'Bob', 'email': 'bob@example.com', 'created_at': '2024-01-16', 'active': True}
        ]
        print(f"✓ Using {len(raw_data)} simulated records")
    
    # TRANSFORM
    print("\n[TRANSFORM] Normalizing data...")
    
    schema = {
        'type': 'object',
        'properties': {
            'user_id': {'type': 'integer', 'source': 'id'},
            'full_name': {'type': 'string', 'source': 'name'},
            'email': {'type': 'string', 'format': 'email', 'source': 'email'},
            'registration_date': {'type': 'string', 'source': 'created_at'},
            'is_active': {'type': 'boolean', 'source': 'active'},
            'source_system': {'type': 'string', 'default': 'PostgreSQL'}
        }
    }
    
    transformer = SchemaTransformer(schema)
    transformed_data = transformer.transform(raw_data)
    
    print(f"✓ Transformed {len(transformed_data)} records")
    
    # LOAD to S3
    print("\n[LOAD] Writing to S3...")
    
    s3_config = {
        'bucket': 'data-warehouse',
        'prefix': 'users/postgres/',
        'region_name': 'us-east-1',
        'format': 'jsonl',
        'compression': 'gzip'
    }
    
    try:
        s3_writer = WriterFactory.create_writer('s3', s3_config)
        
        with s3_writer:
            result = s3_writer.write(
                transformed_data,
                key=f"users_{datetime.utcnow().strftime('%Y%m%d')}.jsonl"
            )
            
            if result['success']:
                print(f"✓ Wrote {result['records_written']} records to S3")
                print(f"  URI: {result['s3_uri']}")
    
    except Exception as e:
        print(f"⚠ S3 not available: {e}")
    
    print("\n✓ ETL Complete!")


def etl_mysql_to_mongodb():
    """ETL: MySQL → Transform → MongoDB"""
    print("\n" + "=" * 80)
    print("ETL: MySQL → Transform → MongoDB")
    print("=" * 80)
    
    # EXTRACT from MySQL
    print("\n[EXTRACT] Reading from MySQL...")
    
    mysql_config = {
        'host': 'localhost',
        'port': 3306,
        'database': 'ecommerce',
        'username': 'root',
        'password': 'password'
    }
    
    try:
        mysql_reader = ClientFactory.create_client('mysql', mysql_config)
        
        with mysql_reader:
            print(f"✓ Connected to MySQL")
            
            # Extract orders
            raw_data = mysql_reader.execute_query("""
                SELECT 
                    order_id,
                    customer_id,
                    order_date,
                    total_amount,
                    status
                FROM orders
                WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
            """)
            
            print(f"✓ Extracted {len(raw_data)} records")
    
    except Exception as e:
        print(f"⚠ MySQL not available: {e}")
        print("  Using simulated data...")
        raw_data = [
            {'order_id': 1001, 'customer_id': 1, 'order_date': '2024-01-15', 'total_amount': 99.99, 'status': 'completed'},
            {'order_id': 1002, 'customer_id': 2, 'order_date': '2024-01-15', 'total_amount': 149.50, 'status': 'pending'}
        ]
        print(f"✓ Using {len(raw_data)} simulated records")
    
    # TRANSFORM
    print("\n[TRANSFORM] Normalizing data...")
    
    schema = {
        'type': 'object',
        'properties': {
            'order_id': {'type': 'integer', 'source': 'order_id'},
            'customer_id': {'type': 'integer', 'source': 'customer_id'},
            'order_date': {'type': 'string', 'source': 'order_date'},
            'total': {'type': 'number', 'source': 'total_amount'},
            'status': {'type': 'string', 'source': 'status'},
            'migrated_at': {'type': 'string', 'default': datetime.utcnow().isoformat()}
        }
    }
    
    transformer = SchemaTransformer(schema)
    transformed_data = transformer.transform(raw_data)
    
    print(f"✓ Transformed {len(transformed_data)} records")
    
    # LOAD to MongoDB
    print("\n[LOAD] Writing to MongoDB...")
    
    mongo_config = {
        'host': 'localhost',
        'port': 27017,
        'database': 'analytics',
        'collection': 'orders'
    }
    
    try:
        mongo_writer = WriterFactory.create_writer('mongodb', mongo_config)
        
        with mongo_writer:
            result = mongo_writer.write(transformed_data)
            
            if result['success']:
                print(f"✓ Wrote {result['records_written']} documents to MongoDB")
                print(f"  Collection: {result['collection']}")
    
    except Exception as e:
        print(f"⚠ MongoDB not available: {e}")
    
    print("\n✓ ETL Complete!")


def etl_sqlite_to_csv():
    """ETL: SQLite → Transform → CSV (FULLY WORKING!)"""
    print("\n" + "=" * 80)
    print("ETL: SQLite → Transform → CSV (FULLY WORKING!)")
    print("=" * 80)
    
    # EXTRACT from SQLite
    print("\n[EXTRACT] Reading from SQLite...")
    
    sqlite_config = {
        'database_path': ':memory:'  # In-memory database
    }
    
    sqlite_reader = ClientFactory.create_client('sqlite', sqlite_config)
    
    with sqlite_reader:
        print(f"✓ Connected to SQLite")
        
        # Create and populate sample table
        sqlite_reader.execute_query("""
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name TEXT,
                category TEXT,
                price REAL,
                stock INTEGER
            )
        """)
        
        # Insert sample data
        products = [
            (1, 'Laptop', 'Electronics', 999.99, 10),
            (2, 'Mouse', 'Electronics', 29.99, 50),
            (3, 'Desk', 'Furniture', 299.99, 5),
            (4, 'Chair', 'Furniture', 199.99, 8)
        ]
        
        for product in products:
            sqlite_reader.execute_query(
                "INSERT INTO products VALUES (?, ?, ?, ?, ?)",
                product  # Pass tuple directly for positional parameters
            )
        
        # Extract data
        raw_data = sqlite_reader.execute_query("SELECT * FROM products")
        
        print(f"✓ Extracted {len(raw_data)} records")
    
    # TRANSFORM
    print("\n[TRANSFORM] Normalizing data...")
    
    schema = {
        'type': 'object',
        'properties': {
            'product_id': {'type': 'integer', 'source': 'id'},
            'product_name': {'type': 'string', 'source': 'name'},
            'category': {'type': 'string', 'source': 'category'},
            'price_usd': {'type': 'number', 'source': 'price'},
            'in_stock': {'type': 'boolean', 'source': 'stock'},
            'export_date': {'type': 'string', 'default': datetime.utcnow().strftime('%Y-%m-%d')}
        }
    }
    
    transformer = SchemaTransformer(schema)
    
    # Add custom transformer to convert stock to boolean
    transformer.add_custom_transformer('in_stock', lambda x: x > 0)
    
    transformed_data = transformer.transform(raw_data)
    
    print(f"✓ Transformed {len(transformed_data)} records")
    print(f"  Sample: {transformed_data[0]}")
    
    # LOAD to CSV
    print("\n[LOAD] Writing to CSV...")
    
    csv_config = {
        'file_path': 'output/products_export.csv',
        'mode': 'w',
        'include_header': True
    }
    
    csv_writer = WriterFactory.create_writer('csv', csv_config)
    
    with csv_writer:
        result = csv_writer.write(transformed_data)
        
        if result['success']:
            print(f"✓ Wrote {result['records_written']} records to CSV")
            print(f"  File: {result['file_path']}")
    
    print("\n✓ ETL Complete! Check output/products_export.csv")


def etl_cassandra_to_opensearch():
    """ETL: Cassandra → Transform → OpenSearch"""
    print("\n" + "=" * 80)
    print("ETL: Cassandra → Transform → OpenSearch")
    print("=" * 80)
    
    # EXTRACT from Cassandra
    print("\n[EXTRACT] Reading from Cassandra...")
    
    cassandra_config = {
        'hosts': ['localhost'],
        'port': 9042,
        'keyspace': 'events',
        'username': 'cassandra',
        'password': 'cassandra'
    }
    
    try:
        cassandra_reader = ClientFactory.create_client('cassandra', cassandra_config)
        
        with cassandra_reader:
            print(f"✓ Connected to Cassandra")
            
            # Extract events
            raw_data = cassandra_reader.execute_query("""
                SELECT 
                    event_id,
                    user_id,
                    event_type,
                    timestamp,
                    properties
                FROM user_events
                WHERE timestamp >= dateOf(now()) - 86400000
                ALLOW FILTERING
            """)
            
            print(f"✓ Extracted {len(raw_data)} records")
    
    except Exception as e:
        print(f"⚠ Cassandra not available: {e}")
        print("  Using simulated data...")
        raw_data = [
            {'event_id': 'evt-001', 'user_id': 123, 'event_type': 'login', 
             'timestamp': '2024-01-15T10:00:00Z', 'properties': {'device': 'mobile'}},
            {'event_id': 'evt-002', 'user_id': 456, 'event_type': 'purchase', 
             'timestamp': '2024-01-15T11:00:00Z', 'properties': {'amount': 99.99}}
        ]
        print(f"✓ Using {len(raw_data)} simulated records")
    
    # TRANSFORM
    print("\n[TRANSFORM] Normalizing data...")
    
    schema = {
        'type': 'object',
        'properties': {
            'id': {'type': 'string', 'source': 'event_id'},
            'user_id': {'type': 'integer', 'source': 'user_id'},
            'event': {'type': 'string', 'source': 'event_type'},
            'timestamp': {'type': 'string', 'format': 'date-time', 'source': 'timestamp'},
            'metadata': {'type': 'object', 'source': 'properties'}
        }
    }
    
    transformer = SchemaTransformer(schema)
    transformed_data = transformer.transform(raw_data)
    
    print(f"✓ Transformed {len(transformed_data)} records")
    
    # LOAD to OpenSearch
    print("\n[LOAD] Writing to OpenSearch...")
    
    opensearch_config = {
        'hosts': ['localhost:9200'],
        'username': 'admin',
        'password': 'admin',
        'use_ssl': True,
        'verify_certs': False,
        'index': 'events'
    }
    
    try:
        opensearch_writer = WriterFactory.create_writer('opensearch', opensearch_config)
        
        with opensearch_writer:
            result = opensearch_writer.write(transformed_data, index='user-events-2024')
            
            if result['success']:
                print(f"✓ Indexed {result['records_written']} documents")
                print(f"  Index: {result['index']}")
    
    except Exception as e:
        print(f"⚠ OpenSearch not available: {e}")
    
    print("\n✓ ETL Complete!")


def main():
    """Run all multi-source ETL examples."""
    print("\n" + "=" * 80)
    print(" " * 20 + "MULTI-SOURCE ETL EXAMPLES")
    print("=" * 80)
    print("\nDemonstrating ETL pipelines with different source/destination combinations.")
    print("Each example uses the actual reader client for extraction.\n")
    
    examples = [
        ("PostgreSQL → S3", etl_postgres_to_s3),
        ("MySQL → MongoDB", etl_mysql_to_mongodb),
        ("SQLite → CSV (Working!)", etl_sqlite_to_csv),
        ("Cassandra → OpenSearch", etl_cassandra_to_opensearch)
    ]
    
    for title, example_func in examples:
        try:
            print(f"\n{'=' * 80}")
            print(f" {title}")
            print(f"{'=' * 80}")
            example_func()
        except Exception as e:
            print(f"\n❌ Example failed: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 80)
    print(" " * 25 + "ALL EXAMPLES COMPLETED!")
    print("=" * 80)
    print("\nKey Patterns Demonstrated:")
    print("  ✓ Extract using actual reader clients (12 types)")
    print("  ✓ Transform with Schema Transformer")
    print("  ✓ Load using writer clients (8 destinations)")
    print("  ✓ Graceful fallback when services unavailable")
    print("  ✓ SQLite → CSV works without external services!")
    print("\nComplete ETL Framework:")
    print("  12 Readers × Schema Transform × 8 Writers = 96 possible pipelines!")
    print("=" * 80 + "\n")


if __name__ == '__main__':
    main()
