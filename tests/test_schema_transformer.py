import pytest
import json
from pathlib import Path
from src.schema_transformer import SchemaTransformer, SchemaRegistry
from src.exceptions import ValidationError


class TestSchemaTransformer:
    """Test SchemaTransformer functionality."""
    
    def test_simple_schema_transformation(self):
        """Test basic schema transformation."""
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': 'integer'},
                'name': {'type': 'string'},
                'active': {'type': 'boolean'}
            }
        }
        
        data = {'id': '123', 'name': 'John', 'active': 1}
        
        transformer = SchemaTransformer(schema)
        result = transformer.transform(data)
        
        assert result['id'] == 123
        assert result['name'] == 'John'
        assert result['active'] is True
    
    def test_field_mapping(self):
        """Test field name mapping."""
        schema = {
            'type': 'object',
            'properties': {
                'user_id': {'type': 'integer', 'source': 'id'},
                'full_name': {'type': 'string', 'source': 'name'}
            }
        }
        
        data = {'id': 456, 'name': 'Jane Doe'}
        
        transformer = SchemaTransformer(schema)
        result = transformer.transform(data)
        
        assert result['user_id'] == 456
        assert result['full_name'] == 'Jane Doe'
    
    def test_nested_field_access(self):
        """Test accessing nested fields with dot notation."""
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
        
        transformer = SchemaTransformer(schema)
        result = transformer.transform(data)
        
        assert result['email'] == 'test@example.com'
        assert result['city'] == 'New York'
    
    def test_default_values(self):
        """Test default values for missing fields."""
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': 'integer'},
                'status': {'type': 'string', 'default': 'pending'},
                'count': {'type': 'integer', 'default': 0}
            }
        }
        
        data = {'id': 1}
        
        transformer = SchemaTransformer(schema)
        result = transformer.transform(data)
        
        assert result['id'] == 1
        assert result['status'] == 'pending'
        assert result['count'] == 0
    
    def test_array_transformation(self):
        """Test array field transformation."""
        schema = {
            'type': 'object',
            'properties': {
                'tags': {
                    'type': 'array',
                    'items': {'type': 'string'}
                },
                'scores': {
                    'type': 'array',
                    'items': {'type': 'integer'}
                }
            }
        }
        
        data = {
            'tags': ['python', 'data', 'api'],
            'scores': ['10', '20', '30']
        }
        
        transformer = SchemaTransformer(schema)
        result = transformer.transform(data)
        
        assert result['tags'] == ['python', 'data', 'api']
        assert result['scores'] == [10, 20, 30]
    
    def test_nested_object_transformation(self):
        """Test nested object transformation."""
        schema = {
            'type': 'object',
            'properties': {
                'user': {
                    'type': 'object',
                    'properties': {
                        'id': {'type': 'integer'},
                        'name': {'type': 'string'}
                    }
                }
            }
        }
        
        data = {
            'user': {'id': '789', 'name': 'Alice'}
        }
        
        transformer = SchemaTransformer(schema)
        result = transformer.transform(data)
        
        assert result['user']['id'] == 789
        assert result['user']['name'] == 'Alice'
    
    def test_array_of_objects(self):
        """Test array of objects transformation."""
        schema = {
            'type': 'object',
            'properties': {
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
            'users': [
                {'id': '1', 'name': 'User1'},
                {'id': '2', 'name': 'User2'}
            ]
        }
        
        transformer = SchemaTransformer(schema)
        result = transformer.transform(data)
        
        assert len(result['users']) == 2
        assert result['users'][0]['id'] == 1
        assert result['users'][1]['name'] == 'User2'
    
    def test_list_transformation(self):
        """Test transforming a list of items."""
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': 'integer'},
                'value': {'type': 'string'}
            }
        }
        
        data = [
            {'id': '1', 'value': 'first'},
            {'id': '2', 'value': 'second'}
        ]
        
        transformer = SchemaTransformer(schema)
        result = transformer.transform(data)
        
        assert len(result) == 2
        assert result[0]['id'] == 1
        assert result[1]['value'] == 'second'
    
    def test_custom_transformer(self):
        """Test custom field transformer."""
        schema = {
            'type': 'object',
            'properties': {
                'price': {'type': 'number'},
                'discount_price': {'type': 'number', 'source': 'price'}
            }
        }
        
        data = {'price': 100}
        
        transformer = SchemaTransformer(schema)
        transformer.add_custom_transformer('discount_price', lambda x: x * 0.9)
        
        result = transformer.transform(data)
        
        assert result['price'] == 100.0
        assert result['discount_price'] == 90.0
    
    def test_type_conversions(self):
        """Test various type conversions."""
        schema = {
            'type': 'object',
            'properties': {
                'str_field': {'type': 'string'},
                'int_field': {'type': 'integer'},
                'float_field': {'type': 'number'},
                'bool_field': {'type': 'boolean'}
            }
        }
        
        data = {
            'str_field': 123,
            'int_field': '456',
            'float_field': '78.9',
            'bool_field': 1
        }
        
        transformer = SchemaTransformer(schema)
        result = transformer.transform(data)
        
        assert result['str_field'] == '123'
        assert result['int_field'] == 456
        assert result['float_field'] == 78.9
        assert result['bool_field'] is True
    
    def test_strict_mode_missing_required(self):
        """Test strict mode with missing required fields."""
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': 'integer'},
                'name': {'type': 'string'}
            },
            'required': ['id', 'name']
        }
        
        data = {'id': 1}
        
        transformer = SchemaTransformer(schema, strict=True)
        
        with pytest.raises(ValidationError, match="Required field"):
            transformer.transform(data)
    
    def test_non_strict_mode_missing_required(self):
        """Test non-strict mode with missing required fields."""
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': 'integer'},
                'name': {'type': 'string'}
            },
            'required': ['id', 'name']
        }
        
        data = {'id': 1}
        
        transformer = SchemaTransformer(schema, strict=False)
        result = transformer.transform(data)
        
        assert result['id'] == 1
        assert result['name'] is None
    
    def test_format_date(self):
        """Test date format conversion."""
        schema = {
            'type': 'object',
            'properties': {
                'birth_date': {'type': 'string', 'format': 'date'}
            }
        }
        
        data = {'birth_date': '1990-01-15'}
        
        transformer = SchemaTransformer(schema)
        result = transformer.transform(data)
        
        assert result['birth_date'].year == 1990
        assert result['birth_date'].month == 1
        assert result['birth_date'].day == 15
    
    def test_format_email(self):
        """Test email format conversion."""
        schema = {
            'type': 'object',
            'properties': {
                'email': {'type': 'string', 'format': 'email'}
            }
        }
        
        data = {'email': '  TEST@EXAMPLE.COM  '}
        
        transformer = SchemaTransformer(schema)
        result = transformer.transform(data)
        
        assert result['email'] == 'test@example.com'
    
    def test_validate_method(self):
        """Test validate method."""
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': 'integer'}
            },
            'required': ['id']
        }
        
        transformer = SchemaTransformer(schema, strict=True)
        
        assert transformer.validate({'id': 1}) is True
        assert transformer.validate({}) is False
    
    def test_get_schema_fields(self):
        """Test getting schema fields."""
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': 'integer'},
                'name': {'type': 'string'},
                'email': {'type': 'string'}
            }
        }
        
        transformer = SchemaTransformer(schema)
        fields = transformer.get_schema_fields()
        
        assert 'id' in fields
        assert 'name' in fields
        assert 'email' in fields
        assert len(fields) == 3
    
    def test_get_required_fields(self):
        """Test getting required fields."""
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': 'integer'},
                'name': {'type': 'string'}
            },
            'required': ['id']
        }
        
        transformer = SchemaTransformer(schema)
        required = transformer.get_required_fields()
        
        assert 'id' in required
        assert 'name' not in required
    
    def test_from_file(self, tmp_path):
        """Test loading schema from file."""
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': 'integer'}
            }
        }
        
        schema_file = tmp_path / "schema.json"
        with open(schema_file, 'w') as f:
            json.dump(schema, f)
        
        transformer = SchemaTransformer.from_file(str(schema_file))
        
        assert transformer.schema == schema
    
    def test_from_file_not_found(self):
        """Test loading schema from non-existent file."""
        with pytest.raises(FileNotFoundError):
            SchemaTransformer.from_file('/nonexistent/schema.json')
    
    def test_null_values(self):
        """Test handling null values."""
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': 'integer'},
                'name': {'type': 'string'}
            }
        }
        
        data = {'id': None, 'name': None}
        
        transformer = SchemaTransformer(schema)
        result = transformer.transform(data)
        
        assert result['id'] is None
        assert result['name'] is None


class TestSchemaRegistry:
    """Test SchemaRegistry functionality."""
    
    def test_register_schema_dict(self):
        """Test registering schema from dict."""
        registry = SchemaRegistry()
        
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': 'integer'}
            }
        }
        
        registry.register('user_schema', schema)
        
        assert 'user_schema' in registry.list_schemas()
    
    def test_register_schema_file(self, tmp_path):
        """Test registering schema from file."""
        registry = SchemaRegistry()
        
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': 'integer'}
            }
        }
        
        schema_file = tmp_path / "schema.json"
        with open(schema_file, 'w') as f:
            json.dump(schema, f)
        
        registry.register('file_schema', str(schema_file))
        
        assert 'file_schema' in registry.list_schemas()
    
    def test_get_schema(self):
        """Test getting registered schema."""
        registry = SchemaRegistry()
        
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': 'integer'}
            }
        }
        
        registry.register('test_schema', schema)
        transformer = registry.get('test_schema')
        
        assert transformer is not None
        assert isinstance(transformer, SchemaTransformer)
    
    def test_get_nonexistent_schema(self):
        """Test getting non-existent schema."""
        registry = SchemaRegistry()
        
        transformer = registry.get('nonexistent')
        
        assert transformer is None
    
    def test_transform_with_registry(self):
        """Test transforming data using registry."""
        registry = SchemaRegistry()
        
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': 'integer'},
                'name': {'type': 'string'}
            }
        }
        
        registry.register('user_schema', schema)
        
        data = {'id': '123', 'name': 'Test User'}
        result = registry.transform('user_schema', data)
        
        assert result['id'] == 123
        assert result['name'] == 'Test User'
    
    def test_transform_nonexistent_schema(self):
        """Test transforming with non-existent schema."""
        registry = SchemaRegistry()
        
        with pytest.raises(ValueError, match="Schema .* not found"):
            registry.transform('nonexistent', {})
    
    def test_list_schemas(self):
        """Test listing all schemas."""
        registry = SchemaRegistry()
        
        registry.register('schema1', {'type': 'object', 'properties': {}})
        registry.register('schema2', {'type': 'object', 'properties': {}})
        
        schemas = registry.list_schemas()
        
        assert len(schemas) == 2
        assert 'schema1' in schemas
        assert 'schema2' in schemas
