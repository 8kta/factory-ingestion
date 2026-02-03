# Data Writers and ETL Pipeline Guide

## Overview

The Factory Ingestion library now includes a comprehensive **data writing system** that allows you to write transformed data to multiple destinations. Combined with the readers and schema transformer, this enables complete **ETL (Extract, Transform, Load) pipelines**.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   EXTRACT   │ --> │  TRANSFORM   │ --> │    LOAD     │
│  (Readers)  │     │   (Schema)   │     │  (Writers)  │
└─────────────┘     └──────────────┘     └─────────────┘
     12 Sources           Normalize           8 Destinations
```

## Available Writers

### Cloud Storage (3)
1. ✅ **AWS S3** - Scalable object storage
2. ✅ **Azure Blob Storage** - Microsoft cloud storage
3. ✅ **Google Cloud Storage (GCS)** - Google cloud storage

### Databases (2)
4. ✅ **MongoDB** - Document database
5. ✅ **Cassandra** - Distributed NoSQL database

### Search Engines (2)
6. ✅ **OpenSearch** - Full-text search and analytics
7. ✅ **Apache Solr** - Enterprise search platform

### Local Storage (1)
8. ✅ **CSV** - Local CSV files

## Quick Start

### Basic Writer Usage

```python
from src.writer_factory import WriterFactory, WriterBuilder

# Create a writer
config = {
    'bucket': 'my-bucket',
    'region_name': 'us-east-1',
    'format': 'json'
}

writer = WriterFactory.create_writer('s3', config)

# Write data
with writer:
    result = writer.write(data)
    print(f"Wrote {result['records_written']} records")
```

### Complete ETL Pipeline

```python
from src.client_factory import ClientFactory
from src.schema_transformer import SchemaTransformer
from src.writer_factory import WriterFactory

# EXTRACT
reader = ClientFactory.create_client('opensearch', reader_config)
with reader:
    raw_data = reader.execute_query('*', {'index': 'logs'})

# TRANSFORM
transformer = SchemaTransformer(schema)
transformed_data = transformer.transform(raw_data)

# LOAD
writer = WriterFactory.create_writer('s3', writer_config)
with writer:
    result = writer.write(transformed_data)
```

## Writer Details

### 1. AWS S3 Writer

Write data to Amazon S3 with support for multiple formats and compression.

**Configuration:**
```python
config = {
    'bucket': 'my-bucket',
    'prefix': 'data/logs/',
    'region_name': 'us-east-1',
    'aws_access_key_id': 'YOUR_KEY',
    'aws_secret_access_key': 'YOUR_SECRET',
    'format': 'json',  # json, jsonl, csv
    'compression': 'gzip'  # gzip or None
}
```

**Features:**
- Multiple formats: JSON, JSON Lines, CSV
- Gzip compression
- Auto-generated keys with timestamps
- Partitioning support

**Example:**
```python
writer = WriterFactory.create_writer('s3', config)
with writer:
    result = writer.write(data, key='custom/path/data.json')
    print(f"S3 URI: {result['s3_uri']}")
```

### 2. CSV Writer

Write data to local CSV files.

**Configuration:**
```python
config = {
    'file_path': 'output/data.csv',
    'mode': 'w',  # w=write, a=append
    'delimiter': ',',
    'include_header': True
}
```

**Example:**
```python
writer = WriterFactory.create_writer('csv', config)
with writer:
    result = writer.write(data)
    print(f"File: {result['file_path']}")
```

### 3. OpenSearch Writer

Write documents to OpenSearch using bulk API.

**Configuration:**
```python
config = {
    'hosts': ['localhost:9200'],
    'username': 'admin',
    'password': 'admin',
    'use_ssl': True,
    'verify_certs': False,
    'index': 'my-index'
}
```

**Example:**
```python
writer = WriterFactory.create_writer('opensearch', config)
with writer:
    result = writer.write(data, index='logs-2024')
    print(f"Indexed {result['records_written']} documents")
```

### 4. Apache Solr Writer

Write documents to Solr.

**Configuration:**
```python
config = {
    'url': 'http://localhost:8983/solr/mycore',
    'timeout': 10,
    'commit': True
}
```

**Example:**
```python
writer = WriterFactory.create_writer('solr', config)
with writer:
    result = writer.write(data, commit=True)
```

### 5. Cassandra Writer

Write records to Cassandra tables.

**Configuration:**
```python
config = {
    'hosts': ['localhost'],
    'port': 9042,
    'keyspace': 'mykeyspace',
    'table': 'mytable',
    'username': 'cassandra',
    'password': 'cassandra'
}
```

**Example:**
```python
writer = WriterFactory.create_writer('cassandra', config)
with writer:
    result = writer.write(data, table='events')
```

### 6. MongoDB Writer

Write documents to MongoDB collections.

**Configuration:**
```python
config = {
    'host': 'localhost',
    'port': 27017,
    'database': 'mydb',
    'collection': 'mycollection',
    'username': 'user',
    'password': 'pass'
}
```

**Example:**
```python
writer = WriterFactory.create_writer('mongodb', config)
with writer:
    result = writer.write(data, collection='logs')
    print(f"Inserted IDs: {result['inserted_ids']}")
```

### 7. Azure Blob Storage Writer

Write data to Azure Blob Storage.

**Configuration:**
```python
config = {
    'connection_string': 'DefaultEndpointsProtocol=https;...',
    'container': 'mycontainer',
    'prefix': 'data/',
    'format': 'json'
}
```

**Example:**
```python
writer = WriterFactory.create_writer('azure_blob', config)
with writer:
    result = writer.write(data, blob_name='data.json')
    print(f"Blob URL: {result['blob_url']}")
```

### 8. Google Cloud Storage Writer

Write data to GCS.

**Configuration:**
```python
config = {
    'bucket': 'my-bucket',
    'project_id': 'my-project',
    'credentials_path': 'path/to/credentials.json',
    'prefix': 'data/',
    'format': 'json'
}
```

**Example:**
```python
writer = WriterFactory.create_writer('gcs', config)
with writer:
    result = writer.write(data, blob_name='data.json')
    print(f"GCS URI: {result['gcs_uri']}")
```

## Writer Builder Pattern

Use the builder pattern for fluent API:

```python
from src.writer_factory import WriterBuilder

writer = (WriterBuilder()
          .with_writer_type('s3')
          .add_config_param('bucket', 'my-bucket')
          .add_config_param('region_name', 'us-east-1')
          .add_config_param('format', 'jsonl')
          .build())

with writer:
    result = writer.write(data)
```

## Complete ETL Examples

### Example 1: OpenSearch to S3

```python
# Extract from OpenSearch
opensearch_reader = ClientFactory.create_client('opensearch', {
    'hosts': ['localhost:9200'],
    'username': 'admin',
    'password': 'admin'
})

with opensearch_reader:
    raw_data = opensearch_reader.execute_query('*', {'index': 'logs'})

# Transform
schema = {
    'type': 'object',
    'properties': {
        'log_id': {'type': 'string', 'source': '_id'},
        'timestamp': {'type': 'string', 'source': '@timestamp'},
        'message': {'type': 'string', 'source': 'message'}
    }
}

transformer = SchemaTransformer(schema)
transformed_data = transformer.transform(raw_data)

# Load to S3
s3_writer = WriterFactory.create_writer('s3', {
    'bucket': 'data-lake',
    'prefix': 'logs/',
    'format': 'jsonl',
    'compression': 'gzip'
})

with s3_writer:
    result = s3_writer.write(transformed_data)
    print(f"✓ Wrote {result['records_written']} records to {result['s3_uri']}")
```

### Example 2: PostgreSQL to MongoDB

```python
# Extract from PostgreSQL
pg_reader = ClientFactory.create_client('postgres', {
    'host': 'localhost',
    'database': 'mydb',
    'username': 'user',
    'password': 'pass'
})

with pg_reader:
    raw_data = pg_reader.execute_query("SELECT * FROM users")

# Transform
schema = {
    'type': 'object',
    'properties': {
        'user_id': {'type': 'integer', 'source': 'id'},
        'name': {'type': 'string', 'source': 'name'},
        'email': {'type': 'string', 'source': 'email'}
    }
}

transformer = SchemaTransformer(schema)
transformed_data = transformer.transform(raw_data)

# Load to MongoDB
mongo_writer = WriterFactory.create_writer('mongodb', {
    'host': 'localhost',
    'database': 'analytics',
    'collection': 'users'
})

with mongo_writer:
    result = mongo_writer.write(transformed_data)
    print(f"✓ Inserted {result['records_written']} documents")
```

### Example 3: Multi-Source to S3 Data Lake

```python
from src.schema_transformer import SchemaRegistry

# Setup unified schema
registry = SchemaRegistry()
registry.register('user_schema', unified_schema)

# Extract from multiple sources
sources = [
    ('postgres', pg_config, "SELECT * FROM users"),
    ('mysql', mysql_config, "SELECT * FROM customers"),
    ('mongodb', mongo_config, {'collection': 'profiles'})
]

all_data = []

for source_type, config, query in sources:
    reader = ClientFactory.create_client(source_type, config)
    with reader:
        raw_data = reader.execute_query(query)
        normalized = registry.transform('user_schema', raw_data)
        all_data.extend(normalized)

# Load to S3
s3_writer = WriterFactory.create_writer('s3', s3_config)
with s3_writer:
    result = s3_writer.write(all_data)
    print(f"✓ Consolidated {len(all_data)} records to S3")
```

## Production ETL Pattern

```python
from datetime import datetime

def production_etl_pipeline(source_config, dest_config, schema):
    """Production-ready ETL pipeline with metrics and error handling."""
    
    metrics = {
        'start_time': datetime.utcnow(),
        'records_extracted': 0,
        'records_transformed': 0,
        'records_loaded': 0,
        'errors': []
    }
    
    try:
        # Extract
        reader = ClientFactory.create_from_config_file(source_config)
        with reader:
            raw_data = reader.execute_query(query)
            metrics['records_extracted'] = len(raw_data)
        
        # Transform
        transformer = SchemaTransformer(schema)
        transformed_data = transformer.transform(raw_data)
        metrics['records_transformed'] = len(transformed_data)
        
        # Load
        writer = WriterFactory.create_writer(dest_type, dest_config)
        with writer:
            result = writer.write(transformed_data)
            metrics['records_loaded'] = result['records_written']
        
        # Metrics
        metrics['end_time'] = datetime.utcnow()
        metrics['duration'] = (metrics['end_time'] - metrics['start_time']).total_seconds()
        metrics['success_rate'] = (metrics['records_loaded'] / metrics['records_extracted']) * 100
        
        return metrics
        
    except Exception as e:
        metrics['errors'].append(str(e))
        raise
```

## Error Handling

All writers implement comprehensive error handling:

```python
try:
    writer = WriterFactory.create_writer('s3', config)
    with writer:
        result = writer.write(data)
        
        if result['success']:
            print(f"✓ Success: {result['records_written']} records")
        else:
            print(f"❌ Failed: {result['error']}")
            
except ClientConnectionError as e:
    print(f"Connection failed: {e}")
except ValidationError as e:
    print(f"Validation failed: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Best Practices

### 1. Use Context Managers
```python
with writer:
    result = writer.write(data)
# Automatic cleanup
```

### 2. Batch Processing
```python
batch_size = 1000
for i in range(0, len(data), batch_size):
    batch = data[i:i+batch_size]
    result = writer.write(batch)
```

### 3. Partitioning
```python
# Partition by date
date = datetime.utcnow().strftime('%Y/%m/%d')
result = writer.write(data, key=f"{date}/data.json")
```

### 4. Compression
```python
# Use compression for large datasets
config = {
    'bucket': 'my-bucket',
    'format': 'jsonl',
    'compression': 'gzip'  # Reduces storage costs
}
```

### 5. Monitoring
```python
result = writer.write(data)
print(f"Records: {result['records_written']}")
print(f"Size: {result.get('size_bytes', 0)} bytes")
print(f"Location: {result.get('s3_uri', result.get('file_path'))}")
```

## Summary

The Data Writers system provides:

- ✅ **8 destination types** (S3, Azure, GCP, MongoDB, Cassandra, OpenSearch, Solr, CSV)
- ✅ **Builder pattern** for fluent API
- ✅ **Context managers** for automatic cleanup
- ✅ **Multiple formats** (JSON, JSON Lines, CSV)
- ✅ **Compression support** (gzip)
- ✅ **Batch writing** for performance
- ✅ **Error handling** and logging
- ✅ **Metrics collection** for monitoring
- ✅ **Complete ETL pipelines** with 12 readers + 8 writers

Combined with readers and schema transformation, you can build complete data pipelines from any source to any destination!
