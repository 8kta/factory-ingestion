"""
Schema Transformer - Transform client responses to match JSON schemas.

This module provides a flexible interface for transforming data from any client
into a structured format defined by a JSON schema.
"""

from typing import Any, Dict, List, Optional, Union, Callable
from datetime import datetime, date
from decimal import Decimal
import json
from pathlib import Path

from .logging_config import get_logger
from .exceptions import ValidationError


logger = get_logger('SchemaTransformer')


class SchemaTransformer:
    """
    Transform client responses to match a given JSON schema.
    
    Supports:
    - Type conversions (string, number, integer, boolean, array, object)
    - Nested schemas
    - Field mapping and renaming
    - Default values
    - Custom transformers
    - Data validation
    """
    
    # Type conversion functions
    TYPE_CONVERTERS = {
        'string': lambda x: str(x) if x is not None else None,
        'number': lambda x: float(x) if x is not None else None,
        'integer': lambda x: int(x) if x is not None else None,
        'boolean': lambda x: bool(x) if x is not None else None,
        'array': lambda x: list(x) if x is not None else [],
        'object': lambda x: dict(x) if x is not None else {},
        'null': lambda x: None,
    }
    
    def __init__(self, schema: Dict[str, Any], strict: bool = False):
        """
        Initialize the schema transformer.
        
        Args:
            schema: JSON schema definition
            strict: If True, raise errors for missing required fields
        """
        self.schema = schema
        self.strict = strict
        self.custom_transformers: Dict[str, Callable] = {}
        
        logger.debug(f"Initialized SchemaTransformer with schema: {schema.get('title', 'Untitled')}")
    
    @classmethod
    def from_file(cls, schema_path: str, strict: bool = False) -> 'SchemaTransformer':
        """
        Create a transformer from a JSON schema file.
        
        Args:
            schema_path: Path to JSON schema file
            strict: If True, raise errors for missing required fields
            
        Returns:
            SchemaTransformer instance
        """
        logger.info(f"Loading schema from file: {schema_path}")
        
        path = Path(schema_path)
        if not path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        with open(path, 'r') as f:
            schema = json.load(f)
        
        return cls(schema, strict=strict)
    
    def add_custom_transformer(self, field_name: str, transformer: Callable) -> None:
        """
        Add a custom transformation function for a specific field.
        
        Args:
            field_name: Name of the field to transform
            transformer: Function that takes a value and returns transformed value
        """
        logger.debug(f"Adding custom transformer for field: {field_name}")
        self.custom_transformers[field_name] = transformer
    
    def transform(self, data: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
        """
        Transform data to match the schema.
        
        Args:
            data: Raw data from client (dict or list of dicts)
            
        Returns:
            Transformed data matching the schema
        """
        if isinstance(data, list):
            logger.debug(f"Transforming list of {len(data)} items")
            return [self._transform_single(item) for item in data]
        else:
            logger.debug("Transforming single item")
            return self._transform_single(data)
    
    def _transform_single(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single data item."""
        try:
            result = {}
            properties = self.schema.get('properties', {})
            required_fields = self.schema.get('required', [])
            
            # Process each field in the schema
            for field_name, field_schema in properties.items():
                try:
                    value = self._transform_field(field_name, field_schema, data)
                    result[field_name] = value
                except Exception as e:
                    if field_name in required_fields and self.strict:
                        logger.error(f"Failed to transform required field '{field_name}': {e}")
                        raise ValidationError(f"Required field '{field_name}' transformation failed: {e}")
                    else:
                        logger.warning(f"Failed to transform optional field '{field_name}': {e}")
                        result[field_name] = field_schema.get('default', None)
            
            logger.debug(f"Successfully transformed item with {len(result)} fields")
            return result
            
        except Exception as e:
            logger.error(f"Error transforming data: {e}", exc_info=True)
            raise ValidationError(f"Data transformation failed: {e}")
    
    def _transform_field(self, field_name: str, field_schema: Dict[str, Any], data: Dict[str, Any]) -> Any:
        """Transform a single field."""
        # Check for custom transformer
        if field_name in self.custom_transformers:
            logger.debug(f"Using custom transformer for field: {field_name}")
            source_field = field_schema.get('source', field_name)
            value = self._get_nested_value(data, source_field)
            return self.custom_transformers[field_name](value)
        
        # Get the source field name (supports field mapping)
        source_field = field_schema.get('source', field_name)
        value = self._get_nested_value(data, source_field)
        
        # Handle missing values
        if value is None:
            if 'default' in field_schema:
                return field_schema['default']
            elif field_name in self.schema.get('required', []) and self.strict:
                raise ValidationError(f"Required field '{field_name}' is missing")
            else:
                return None
        
        # Get field type
        field_type = field_schema.get('type', 'string')
        
        # Handle arrays
        if field_type == 'array':
            return self._transform_array(value, field_schema)
        
        # Handle nested objects
        if field_type == 'object':
            return self._transform_object(value, field_schema)
        
        # Apply type conversion
        return self._convert_type(value, field_type, field_schema)
    
    def _get_nested_value(self, data: Dict[str, Any], path: str) -> Any:
        """
        Get a value from nested dictionary using dot notation.
        
        Example: 'user.profile.name' -> data['user']['profile']['name']
        """
        if '.' not in path:
            return data.get(path)
        
        keys = path.split('.')
        value = data
        
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
            
            if value is None:
                return None
        
        return value
    
    def _transform_array(self, value: Any, field_schema: Dict[str, Any]) -> List[Any]:
        """Transform array values."""
        if not isinstance(value, (list, tuple)):
            value = [value]
        
        items_schema = field_schema.get('items', {})
        item_type = items_schema.get('type', 'string')
        
        # If items are objects with their own schema
        if item_type == 'object' and 'properties' in items_schema:
            return [self._transform_object(item, items_schema) for item in value]
        
        # Simple type conversion for array items
        return [self._convert_type(item, item_type, items_schema) for item in value]
    
    def _transform_object(self, value: Any, field_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Transform nested object."""
        if not isinstance(value, dict):
            return {}
        
        result = {}
        properties = field_schema.get('properties', {})
        
        for prop_name, prop_schema in properties.items():
            source_field = prop_schema.get('source', prop_name)
            prop_value = value.get(source_field)
            
            if prop_value is not None:
                prop_type = prop_schema.get('type', 'string')
                result[prop_name] = self._convert_type(prop_value, prop_type, prop_schema)
            else:
                result[prop_name] = prop_schema.get('default', None)
        
        return result
    
    def _convert_type(self, value: Any, target_type: str, field_schema: Dict[str, Any]) -> Any:
        """Convert value to target type."""
        try:
            # Handle null values
            if value is None:
                return None
            
            # Check for format-specific conversions
            format_type = field_schema.get('format')
            if format_type:
                return self._convert_format(value, format_type)
            
            # Apply standard type conversion
            if target_type in self.TYPE_CONVERTERS:
                return self.TYPE_CONVERTERS[target_type](value)
            
            # Return as-is if no converter found
            return value
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Type conversion failed for value '{value}' to type '{target_type}': {e}")
            return field_schema.get('default', None)
    
    def _convert_format(self, value: Any, format_type: str) -> Any:
        """Convert value based on format specification."""
        try:
            if format_type == 'date':
                if isinstance(value, str):
                    return datetime.strptime(value, '%Y-%m-%d').date()
                return value
            
            elif format_type == 'date-time':
                if isinstance(value, str):
                    return datetime.fromisoformat(value.replace('Z', '+00:00'))
                return value
            
            elif format_type == 'email':
                return str(value).lower().strip()
            
            elif format_type == 'uri':
                return str(value).strip()
            
            elif format_type == 'uuid':
                return str(value).strip()
            
            else:
                return value
                
        except Exception as e:
            logger.warning(f"Format conversion failed for format '{format_type}': {e}")
            return value
    
    def validate(self, data: Union[Dict, List[Dict]]) -> bool:
        """
        Validate data against the schema without transforming.
        
        Args:
            data: Data to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            self.transform(data)
            return True
        except ValidationError:
            return False
    
    def get_schema_fields(self) -> List[str]:
        """Get list of all field names in the schema."""
        return list(self.schema.get('properties', {}).keys())
    
    def get_required_fields(self) -> List[str]:
        """Get list of required field names."""
        return self.schema.get('required', [])
    
    def to_dict(self) -> Dict[str, Any]:
        """Return the schema as a dictionary."""
        return self.schema.copy()


class SchemaRegistry:
    """
    Registry for managing multiple schemas.
    """
    
    def __init__(self):
        self.schemas: Dict[str, SchemaTransformer] = {}
        logger.debug("Initialized SchemaRegistry")
    
    def register(self, name: str, schema: Union[Dict[str, Any], str], strict: bool = False) -> None:
        """
        Register a schema.
        
        Args:
            name: Schema name
            schema: Schema dict or path to schema file
            strict: Strict validation mode
        """
        logger.info(f"Registering schema: {name}")
        
        if isinstance(schema, str):
            transformer = SchemaTransformer.from_file(schema, strict=strict)
        else:
            transformer = SchemaTransformer(schema, strict=strict)
        
        self.schemas[name] = transformer
    
    def get(self, name: str) -> Optional[SchemaTransformer]:
        """Get a registered schema transformer."""
        return self.schemas.get(name)
    
    def transform(self, name: str, data: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
        """Transform data using a registered schema."""
        transformer = self.get(name)
        if not transformer:
            raise ValueError(f"Schema '{name}' not found in registry")
        
        return transformer.transform(data)
    
    def list_schemas(self) -> List[str]:
        """List all registered schema names."""
        return list(self.schemas.keys())
