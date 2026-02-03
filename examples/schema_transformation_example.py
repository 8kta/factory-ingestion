"""
Example: Using Schema Transformer with Multiple Data Sources

This example demonstrates how to use the SchemaTransformer to normalize
data from different sources into a consistent format.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.client_factory import ClientFactory
from src.schema_transformer import SchemaTransformer, SchemaRegistry
from src.logging_config import setup_logging

# Setup logging
setup_logging(level='INFO')


def example_basic_transformation():
    """Basic schema transformation example."""
    print("\n=== Example 1: Basic Schema Transformation ===\n")
    
    # Define schema
    schema = {
        'type': 'object',
        'properties': {
            'user_id': {'type': 'integer'},
            'full_name': {'type': 'string'},
            'email': {'type': 'string', 'format': 'email'},
            'is_active': {'type': 'boolean'}
        }
    }
    
    # Simulate client response
    raw_data = [
        {'user_id': '1', 'full_name': 'Alice Johnson', 'email': '  ALICE@EXAMPLE.COM  ', 'is_active': 1},
        {'user_id': '2', 'full_name': 'Bob Smith', 'email': 'bob@example.com', 'is_active': 0}
    ]
    
    # Transform
    transformer = SchemaTransformer(schema)
    result = transformer.transform(raw_data)
    
    print("Transformed Data:")
    for user in result:
        print(f"  User {user['user_id']}: {user['full_name']} ({user['email']}) - Active: {user['is_active']}")


def example_field_mapping():
    """Example with field mapping from different sources."""
    print("\n=== Example 2: Field Mapping from Different Sources ===\n")
    
    # PostgreSQL schema (fields: id, name, email, active)
    postgres_schema = {
        'type': 'object',
        'properties': {
            'user_id': {'type': 'integer', 'source': 'id'},
            'full_name': {'type': 'string', 'source': 'name'},
            'email_address': {'type': 'string', 'source': 'email'},
            'is_active': {'type': 'boolean', 'source': 'active'}
        }
    }
    
    # MySQL schema (fields: user_id, full_name, email_addr, status)
    mysql_schema = {
        'type': 'object',
        'properties': {
            'user_id': {'type': 'integer', 'source': 'user_id'},
            'full_name': {'type': 'string', 'source': 'full_name'},
            'email_address': {'type': 'string', 'source': 'email_addr'},
            'is_active': {'type': 'boolean', 'source': 'status'}
        }
    }
    
    # Simulate data from different sources
    postgres_data = [
        {'id': 1, 'name': 'Alice', 'email': 'alice@example.com', 'active': True}
    ]
    
    mysql_data = [
        {'user_id': 2, 'full_name': 'Bob', 'email_addr': 'bob@example.com', 'status': False}
    ]
    
    # Transform both
    pg_transformer = SchemaTransformer(postgres_schema)
    mysql_transformer = SchemaTransformer(mysql_schema)
    
    normalized_pg = pg_transformer.transform(postgres_data)
    normalized_mysql = mysql_transformer.transform(mysql_data)
    
    # Now both have the same structure!
    all_users = normalized_pg + normalized_mysql
    
    print("Normalized Data from Multiple Sources:")
    for user in all_users:
        print(f"  User {user['user_id']}: {user['full_name']} - {user['email_address']}")


def example_nested_data():
    """Example with nested data access."""
    print("\n=== Example 3: Nested Data Access ===\n")
    
    schema = {
        'type': 'object',
        'properties': {
            'user_id': {'type': 'integer', 'source': 'id'},
            'name': {'type': 'string', 'source': 'profile.name'},
            'email': {'type': 'string', 'source': 'profile.contact.email'},
            'city': {'type': 'string', 'source': 'profile.address.city'},
            'country': {'type': 'string', 'source': 'profile.address.country'}
        }
    }
    
    # Nested data structure
    raw_data = {
        'id': 1,
        'profile': {
            'name': 'John Doe',
            'contact': {
                'email': 'john@example.com',
                'phone': '555-0123'
            },
            'address': {
                'city': 'New York',
                'country': 'USA'
            }
        }
    }
    
    transformer = SchemaTransformer(schema)
    result = transformer.transform(raw_data)
    
    print("Flattened Nested Data:")
    print(f"  User: {result['name']}")
    print(f"  Email: {result['email']}")
    print(f"  Location: {result['city']}, {result['country']}")


def example_custom_transformers():
    """Example with custom transformation logic."""
    print("\n=== Example 4: Custom Transformers ===\n")
    
    schema = {
        'type': 'object',
        'properties': {
            'product_id': {'type': 'integer'},
            'name': {'type': 'string'},
            'price': {'type': 'number'},
            'discounted_price': {'type': 'number', 'source': 'price'},
            'tax_amount': {'type': 'number', 'source': 'price'},
            'final_price': {'type': 'number', 'source': 'price'}
        }
    }
    
    raw_data = [
        {'product_id': 1, 'name': 'Laptop', 'price': 1000},
        {'product_id': 2, 'name': 'Mouse', 'price': 25}
    ]
    
    transformer = SchemaTransformer(schema)
    
    # Add custom transformers
    transformer.add_custom_transformer('discounted_price', lambda x: x * 0.9)  # 10% discount
    transformer.add_custom_transformer('tax_amount', lambda x: x * 0.08)  # 8% tax
    transformer.add_custom_transformer('final_price', lambda x: x * 0.9 * 1.08)  # Final with discount and tax
    
    result = transformer.transform(raw_data)
    
    print("Products with Calculated Prices:")
    for product in result:
        print(f"  {product['name']}:")
        print(f"    Original: ${product['price']:.2f}")
        print(f"    Discounted: ${product['discounted_price']:.2f}")
        print(f"    Tax: ${product['tax_amount']:.2f}")
        print(f"    Final: ${product['final_price']:.2f}")


def example_schema_registry():
    """Example using Schema Registry for multiple schemas."""
    print("\n=== Example 5: Schema Registry ===\n")
    
    # Create registry
    registry = SchemaRegistry()
    
    # Register multiple schemas
    user_schema = {
        'type': 'object',
        'properties': {
            'id': {'type': 'integer'},
            'name': {'type': 'string'},
            'email': {'type': 'string'}
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
            'total': {'type': 'number'}
        }
    }
    
    registry.register('user', user_schema)
    registry.register('product', product_schema)
    registry.register('order', order_schema)
    
    # Transform different data types
    user_data = {'id': '1', 'name': 'Alice', 'email': 'alice@example.com'}
    product_data = {'sku': 'LAP-001', 'name': 'Laptop', 'price': '999.99'}
    order_data = {'order_id': '100', 'user_id': '1', 'total': '999.99'}
    
    transformed_user = registry.transform('user', user_data)
    transformed_product = registry.transform('product', product_data)
    transformed_order = registry.transform('order', order_data)
    
    print(f"Available schemas: {registry.list_schemas()}")
    print(f"\nUser: {transformed_user}")
    print(f"Product: {transformed_product}")
    print(f"Order: {transformed_order}")


def example_array_of_objects():
    """Example with arrays of nested objects."""
    print("\n=== Example 6: Arrays of Objects ===\n")
    
    schema = {
        'type': 'object',
        'properties': {
            'order_id': {'type': 'integer'},
            'customer_name': {'type': 'string'},
            'items': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'product_id': {'type': 'integer'},
                        'product_name': {'type': 'string'},
                        'quantity': {'type': 'integer'},
                        'price': {'type': 'number'}
                    }
                }
            }
        }
    }
    
    raw_data = {
        'order_id': '1001',
        'customer_name': 'Alice Johnson',
        'items': [
            {'product_id': '1', 'product_name': 'Laptop', 'quantity': '1', 'price': '999.99'},
            {'product_id': '2', 'product_name': 'Mouse', 'quantity': '2', 'price': '25.00'}
        ]
    }
    
    transformer = SchemaTransformer(schema)
    result = transformer.transform(raw_data)
    
    print(f"Order #{result['order_id']} for {result['customer_name']}:")
    for item in result['items']:
        total = item['quantity'] * item['price']
        print(f"  - {item['product_name']}: {item['quantity']} x ${item['price']:.2f} = ${total:.2f}")


def example_with_defaults():
    """Example with default values."""
    print("\n=== Example 7: Default Values ===\n")
    
    schema = {
        'type': 'object',
        'properties': {
            'id': {'type': 'integer'},
            'name': {'type': 'string'},
            'status': {'type': 'string', 'default': 'pending'},
            'priority': {'type': 'integer', 'default': 0},
            'tags': {'type': 'array', 'default': []}
        }
    }
    
    # Data with missing optional fields
    raw_data = [
        {'id': 1, 'name': 'Task 1'},
        {'id': 2, 'name': 'Task 2', 'status': 'completed'},
        {'id': 3, 'name': 'Task 3', 'priority': 5, 'tags': ['urgent', 'bug']}
    ]
    
    transformer = SchemaTransformer(schema)
    result = transformer.transform(raw_data)
    
    print("Tasks with Default Values:")
    for task in result:
        print(f"  Task {task['id']}: {task['name']}")
        print(f"    Status: {task['status']}, Priority: {task['priority']}, Tags: {task['tags']}")


def main():
    """Run all examples."""
    print("=" * 70)
    print("Schema Transformer Examples")
    print("=" * 70)
    
    examples = [
        example_basic_transformation,
        example_field_mapping,
        example_nested_data,
        example_custom_transformers,
        example_schema_registry,
        example_array_of_objects,
        example_with_defaults
    ]
    
    for example in examples:
        try:
            example()
        except Exception as e:
            print(f"\nExample failed: {e}")
    
    print("\n" + "=" * 70)
    print("All examples completed!")
    print("=" * 70)


if __name__ == '__main__':
    main()
