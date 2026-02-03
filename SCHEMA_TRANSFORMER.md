# Schema Transformer Guide

## Overview

The Schema Transformer provides a powerful interface for transforming client responses into structured data that matches a JSON schema. This allows you to normalize data from different sources into a consistent format.

## Features

- ✅ **Type Conversions** - Automatic conversion between data types
- ✅ **Field Mapping** - Rename and remap fields
- ✅ **Nested Data** - Access nested fields with dot notation
- ✅ **Default Values** - Specify defaults for missing fields
- ✅ **Custom Transformers** - Add custom transformation logic
- ✅ **Array Support** - Transform arrays and arrays of objects
- ✅ **Format Handling** - Special handling for dates, emails, URIs, etc.
- ✅ **Validation** - Validate data against schema
- ✅ **Schema Registry** - Manage multiple schemas

## Quick Start

### Basic Usage

```python
from src.schema_transformer import SchemaTransformer

# Define your schema
schema = {
    'type': 'object',
    'properties': {
        'id': {'type': 'integer'},
        'name': {'type': 'string'},
        'email': {'type': 'string', 'format': 'email'},
        'active': {'type': 'boolean'}
    }
}

# Create transformer
transformer = SchemaTransformer(schema)

# Transform data from any client
raw_data = {
    'id': '123',
    'name': 'John Doe',
    'email': '  JOHN@EXAMPLE.COM  ',
    'active': 1
}

result = transformer.transform(raw_data)
# Result: {
#     'id': 123,
#     'name': 'John Doe',
#     'email': 'john@example.com',
#     'active': True
# }
```

### With Client Integration

```python
from src.client_factory import ClientBuilder
from src.schema_transformer import SchemaTransformer

# Define schema for database response
schema = {
    'type': 'object',
    'properties': {
        'user_id': {'type': 'integer', 'source': 'id'},
        'full_name': {'type': 'string', 'source': 'name'},
        'email_address': {'type': 'string', 'source': 'email'},
        'is_active': {'type': 'boolean', 'source': 'active'}
    }
}

# Create client and transformer
client = ClientBuilder().with_config_file('config.yaml', 'my_db').build()
transformer = SchemaTransformer(schema)

# Query and transform
with client:
    raw_results = client.execute_query("SELECT * FROM users")
    transformed_results = transformer.transform(raw_results)

# Now all results match your schema format
for user in transformed_results:
    print(f"User ID: {user['user_id']}, Name: {user['full_name']}")
```

## Schema Definition

### Basic Schema Structure

```json
{
  "type": "object",
  "title": "User Schema",
  "properties": {
    "id": {
      "type": "integer",
      "description": "User ID"
    },
    "name": {
      "type": "string",
      "description": "User name"
    }
  },
  "required": ["id"]
}
```

### Supported Types

- `string` - Text values
- `integer` - Whole numbers
- `number` - Floating point numbers
- `boolean` - True/false values
- `array` - Lists of values
- `object` - Nested objects
- `null` - Null values

## Advanced Features

### 1. Field Mapping

Rename fields from source to target:

```python
schema = {
    'type': 'object',
    'properties': {
        'user_id': {'type': 'integer', 'source': 'id'},
        'full_name': {'type': 'string', 'source': 'name'},
        'email_address': {'type': 'string', 'source': 'email'}
    }
}

data = {'id': 1, 'name': 'John', 'email': 'john@example.com'}
result = transformer.transform(data)
# Result: {'user_id': 1, 'full_name': 'John', 'email_address': 'john@example.com'}
```

### 2. Nested Field Access

Access nested data with dot notation:

```python
schema = {
    'type': 'object',
    'properties': {
        'email': {'type': 'string', 'source': 'user.contact.email'},
        'city': {'type': 'string', 'source': 'user.address.city'}
    }
}

data = {
    'user': {
        'contact': {'email': 'test@example.com'},
        'address': {'city': 'New York'}
    }
}

result = transformer.transform(data)
# Result: {'email': 'test@example.com', 'city': 'New York'}
```

### 3. Default Values

Specify defaults for missing fields:

```python
schema = {
    'type': 'object',
    'properties': {
        'id': {'type': 'integer'},
        'status': {'type': 'string', 'default': 'pending'},
        'count': {'type': 'integer', 'default': 0}
    }
}

data = {'id': 1}
result = transformer.transform(data)
# Result: {'id': 1, 'status': 'pending', 'count': 0}
```

### 4. Arrays

Transform arrays and arrays of objects:

```python
schema = {
    'type': 'object',
    'properties': {
        'tags': {
            'type': 'array',
            'items': {'type': 'string'}
        },
        'users': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'id': {'type': 'integer'},
                    'name': {'type': 'string'}
                }
            }
        }
    }
}

data = {
    'tags': ['python', 'data'],
    'users': [
        {'id': '1', 'name': 'Alice'},
        {'id': '2', 'name': 'Bob'}
    ]
}

result = transformer.transform(data)
# Arrays and nested objects are properly transformed
```

### 5. Custom Transformers

Add custom transformation logic:

```python
schema = {
    'type': 'object',
    'properties': {
        'price': {'type': 'number'},
        'discounted_price': {'type': 'number', 'source': 'price'},
        'tax_amount': {'type': 'number', 'source': 'price'}
    }
}

transformer = SchemaTransformer(schema)

# Add custom transformers
transformer.add_custom_transformer('discounted_price', lambda x: x * 0.9)
transformer.add_custom_transformer('tax_amount', lambda x: x * 0.08)

data = {'price': 100}
result = transformer.transform(data)
# Result: {
#     'price': 100.0,
#     'discounted_price': 90.0,
#     'tax_amount': 8.0
# }
```

### 6. Format Handling

Special format conversions:

```python
schema = {
    'type': 'object',
    'properties': {
        'birth_date': {'type': 'string', 'format': 'date'},
        'created_at': {'type': 'string', 'format': 'date-time'},
        'email': {'type': 'string', 'format': 'email'},
        'website': {'type': 'string', 'format': 'uri'}
    }
}

data = {
    'birth_date': '1990-01-15',
    'created_at': '2024-01-01T10:30:00Z',
    'email': '  USER@EXAMPLE.COM  ',
    'website': '  https://example.com  '
}

result = transformer.transform(data)
# Formats are properly converted and normalized
```

### 7. Strict Mode

Enforce required fields:

```python
schema = {
    'type': 'object',
    'properties': {
        'id': {'type': 'integer'},
        'name': {'type': 'string'}
    },
    'required': ['id', 'name']
}

# Strict mode - raises ValidationError if required fields missing
transformer = SchemaTransformer(schema, strict=True)

# Non-strict mode - returns None for missing fields
transformer = SchemaTransformer(schema, strict=False)
```

## Schema Registry

Manage multiple schemas:

```python
from src.schema_transformer import SchemaRegistry

# Create registry
registry = SchemaRegistry()

# Register schemas
user_schema = {
    'type': 'object',
    'properties': {
        'id': {'type': 'integer'},
        'name': {'type': 'string'}
    }
}

product_schema = {
    'type': 'object',
    'properties': {
        'sku': {'type': 'string'},
        'price': {'type': 'number'}
    }
}

registry.register('user', user_schema)
registry.register('product', product_schema)

# Use registered schemas
user_data = {'id': '1', 'name': 'John'}
product_data = {'sku': 'ABC123', 'price': '99.99'}

transformed_user = registry.transform('user', user_data)
transformed_product = registry.transform('product', product_data)

# List all schemas
print(registry.list_schemas())  # ['user', 'product']
```

## Loading Schemas from Files

```python
# Save schema to file
schema_file = 'schemas/user_schema.json'

# Load from file
transformer = SchemaTransformer.from_file(schema_file)

# Or with registry
registry = SchemaRegistry()
registry.register('user', schema_file)
```

## Complete Example: Multi-Source Data Normalization

```python
from src.client_factory import ClientFactory
from src.schema_transformer import SchemaRegistry

# Define unified schema for user data
user_schema = {
    'type': 'object',
    'properties': {
        'user_id': {'type': 'integer'},
        'full_name': {'type': 'string'},
        'email': {'type': 'string', 'format': 'email'},
        'created_date': {'type': 'string', 'format': 'date'},
        'is_active': {'type': 'boolean'}
    },
    'required': ['user_id', 'full_name']
}

# Create registry and register schema
registry = SchemaRegistry()
registry.register('user', user_schema)

# Get data from PostgreSQL
postgres_schema = {
    'type': 'object',
    'properties': {
        'user_id': {'type': 'integer', 'source': 'id'},
        'full_name': {'type': 'string', 'source': 'name'},
        'email': {'type': 'string', 'source': 'email'},
        'created_date': {'type': 'string', 'source': 'created_at'},
        'is_active': {'type': 'boolean', 'source': 'active'}
    }
}

# Get data from MySQL (different field names)
mysql_schema = {
    'type': 'object',
    'properties': {
        'user_id': {'type': 'integer', 'source': 'user_id'},
        'full_name': {'type': 'string', 'source': 'full_name'},
        'email': {'type': 'string', 'source': 'email_address'},
        'created_date': {'type': 'string', 'source': 'registration_date'},
        'is_active': {'type': 'boolean', 'source': 'status'}
    }
}

# Query both databases
pg_client = ClientFactory.create_from_config_file('config.yaml', 'postgres_db')
mysql_client = ClientFactory.create_from_config_file('config.yaml', 'mysql_db')

pg_transformer = SchemaTransformer(postgres_schema)
mysql_transformer = SchemaTransformer(mysql_schema)

# Get and transform data
with pg_client:
    pg_data = pg_client.execute_query("SELECT * FROM users")
    normalized_pg_data = pg_transformer.transform(pg_data)

with mysql_client:
    mysql_data = mysql_client.execute_query("SELECT * FROM customers")
    normalized_mysql_data = mysql_transformer.transform(mysql_data)

# Now both datasets have the same structure!
all_users = normalized_pg_data + normalized_mysql_data

for user in all_users:
    print(f"User {user['user_id']}: {user['full_name']} ({user['email']})")
```

## Validation

Validate data without transforming:

```python
transformer = SchemaTransformer(schema, strict=True)

if transformer.validate(data):
    print("Data is valid!")
    result = transformer.transform(data)
else:
    print("Data is invalid!")
```

## Utility Methods

```python
# Get all field names in schema
fields = transformer.get_schema_fields()

# Get required field names
required = transformer.get_required_fields()

# Get schema as dictionary
schema_dict = transformer.to_dict()
```

## Best Practices

1. **Define schemas once** - Reuse schemas across your application
2. **Use field mapping** - Normalize different source field names
3. **Set defaults** - Provide sensible defaults for optional fields
4. **Use strict mode in production** - Catch data quality issues early
5. **Leverage custom transformers** - For complex business logic
6. **Use Schema Registry** - Manage multiple schemas centrally
7. **Document your schemas** - Add descriptions and examples

## Error Handling

```python
from src.exceptions import ValidationError

try:
    result = transformer.transform(data)
except ValidationError as e:
    logger.error(f"Validation failed: {e}")
    # Handle validation error
```

## Summary

The Schema Transformer provides:
- ✅ Consistent data structure across all sources
- ✅ Type safety and validation
- ✅ Flexible field mapping and transformation
- ✅ Easy integration with any client
- ✅ Production-ready error handling

Transform any client response into the exact format you need!
