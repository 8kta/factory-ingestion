# Error Handling and Logging Guide

## Overview

The Factory Ingestion library implements comprehensive error handling and logging throughout the application to provide:
- **Clear error messages** for debugging and troubleshooting
- **Structured logging** with multiple levels and output options
- **Custom exceptions** for different error scenarios
- **Graceful error recovery** where possible

## Table of Contents

1. [Custom Exceptions](#custom-exceptions)
2. [Logging Configuration](#logging-configuration)
3. [Error Handling Patterns](#error-handling-patterns)
4. [Usage Examples](#usage-examples)
5. [Best Practices](#best-practices)

## Custom Exceptions

All custom exceptions inherit from `FactoryIngestionError` base class.

### Exception Hierarchy

```
FactoryIngestionError (Base)
├── ConnectionError - Connection failures
├── QueryExecutionError - Query execution failures
├── ConfigurationError - Configuration issues
├── ClientNotFoundError - Client not found in config
├── InvalidSourceTypeError - Unsupported source type
├── MissingDependencyError - Missing required dependency
├── AuthenticationError - Authentication failures
├── TimeoutError - Operation timeouts
└── ValidationError - Data validation failures
```

### Exception Details

#### `ConnectionError`
Raised when connection to a data source fails.

```python
from src.exceptions import ConnectionError as ClientConnectionError

try:
    client.connect()
except ClientConnectionError as e:
    print(f"Failed to connect: {e}")
```

#### `QueryExecutionError`
Raised when query execution fails.

```python
from src.exceptions import QueryExecutionError

try:
    results = client.execute_query("SELECT * FROM users")
except QueryExecutionError as e:
    print(f"Query failed: {e}")
```

#### `ConfigurationError`
Raised when there's an error in configuration.

```python
from src.exceptions import ConfigurationError

try:
    config = ClientFactory.load_config("invalid.yaml")
except ConfigurationError as e:
    print(f"Configuration error: {e}")
```

#### `InvalidSourceTypeError`
Raised when an unsupported source type is specified.

```python
from src.exceptions import InvalidSourceTypeError

try:
    client = ClientFactory.create_client('unsupported_db', config)
except InvalidSourceTypeError as e:
    print(f"Invalid source type: {e}")
```

#### `AuthenticationError`
Raised when authentication fails.

```python
from src.exceptions import AuthenticationError

try:
    client.connect()
except AuthenticationError as e:
    print(f"Authentication failed: {e}")
```

## Logging Configuration

### Setup Logging

The library provides a flexible logging configuration system.

```python
from src.logging_config import setup_logging

# Basic setup
logger = setup_logging(level='INFO')

# With file output
logger = setup_logging(
    level='DEBUG',
    log_file='logs/app.log',
    log_to_console=True
)

# Custom format
logger = setup_logging(
    level='INFO',
    format_string='%(asctime)s - %(name)s - [%(levelname)s] - %(message)s'
)
```

### Logging Levels

- **DEBUG**: Detailed information for diagnosing problems
- **INFO**: General informational messages
- **WARNING**: Warning messages for potentially harmful situations
- **ERROR**: Error messages for serious problems
- **CRITICAL**: Critical messages for very serious errors

### Get Module Logger

```python
from src.logging_config import get_logger

logger = get_logger('MyModule')
logger.debug("Debug message")
logger.info("Info message")
logger.warning("Warning message")
logger.error("Error message")
logger.critical("Critical message")
```

### Colored Console Output

The logging system automatically adds colors to console output when running in a TTY:
- DEBUG: Cyan
- INFO: Green
- WARNING: Yellow
- ERROR: Red
- CRITICAL: Magenta

## Error Handling Patterns

### Pattern 1: Try-Except with Specific Exceptions

```python
from src.client_factory import ClientBuilder
from src.exceptions import (
    ConnectionError as ClientConnectionError,
    QueryExecutionError,
    ConfigurationError
)

try:
    client = ClientBuilder().with_config_file('config.yaml', 'my_db').build()
    with client:
        results = client.execute_query("SELECT * FROM users")
except ConfigurationError as e:
    logger.error(f"Configuration error: {e}")
    # Handle configuration error
except ClientConnectionError as e:
    logger.error(f"Connection error: {e}")
    # Handle connection error
except QueryExecutionError as e:
    logger.error(f"Query error: {e}")
    # Handle query error
except Exception as e:
    logger.error(f"Unexpected error: {e}", exc_info=True)
    # Handle unexpected error
```

### Pattern 2: Context Manager with Automatic Cleanup

```python
from src.client_factory import ClientBuilder

try:
    client = ClientBuilder().with_source_type('postgres').with_config(config).build()
    
    with client:  # Automatic connect/disconnect
        results = client.execute_query("SELECT * FROM users")
        # Process results
except Exception as e:
    logger.error(f"Error: {e}", exc_info=True)
    # Connection automatically closed even on error
```

### Pattern 3: Graceful Degradation

```python
from src.exceptions import ConnectionError as ClientConnectionError

def get_data_with_fallback(primary_source, fallback_source):
    try:
        client = ClientFactory.create_from_config_file('config.yaml', primary_source)
        with client:
            return client.execute_query("SELECT * FROM data")
    except ClientConnectionError as e:
        logger.warning(f"Primary source failed: {e}, trying fallback")
        try:
            client = ClientFactory.create_from_config_file('config.yaml', fallback_source)
            with client:
                return client.execute_query("SELECT * FROM data")
        except Exception as e:
            logger.error(f"Fallback also failed: {e}")
            raise
```

### Pattern 4: Retry Logic

```python
import time
from src.exceptions import ConnectionError as ClientConnectionError

def connect_with_retry(client, max_retries=3, delay=2):
    for attempt in range(max_retries):
        try:
            client.connect()
            logger.info(f"Connected successfully on attempt {attempt + 1}")
            return
        except ClientConnectionError as e:
            logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
            else:
                logger.error(f"Failed to connect after {max_retries} attempts")
                raise
```

## Usage Examples

### Example 1: CLI with Logging

```bash
# Info level logging (default)
python main.py --config config/sources.yaml --source my_postgres --query "SELECT 1"

# Debug level logging
python main.py --config config/sources.yaml --source my_postgres --query "SELECT 1" --log-level DEBUG

# Log to file
python main.py --config config/sources.yaml --source my_postgres --query "SELECT 1" --log-file logs/app.log

# Error level only
python main.py --config config/sources.yaml --source my_postgres --query "SELECT 1" --log-level ERROR
```

### Example 2: Programmatic Logging Setup

```python
from src.logging_config import setup_logging
from src.client_factory import ClientBuilder

# Setup logging at application start
setup_logging(
    level='DEBUG',
    log_file='logs/application.log',
    log_to_console=True
)

# Use throughout application
client = ClientBuilder().with_config_file('config.yaml', 'my_db').build()
# Logging happens automatically in all operations
```

### Example 3: Custom Error Handling

```python
from src.client_factory import ClientFactory
from src.exceptions import FactoryIngestionError
from src.logging_config import get_logger

logger = get_logger('CustomApp')

def safe_query(source_name, query):
    """Execute query with comprehensive error handling."""
    try:
        logger.info(f"Executing query on {source_name}")
        client = ClientFactory.create_from_config_file('config.yaml', source_name)
        
        with client:
            results = client.execute_query(query)
            logger.info(f"Query returned {len(results)} rows")
            return results
            
    except FactoryIngestionError as e:
        logger.error(f"Factory ingestion error: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.critical(f"Unexpected error: {e}", exc_info=True)
        raise

# Usage
results = safe_query('my_postgres', 'SELECT * FROM users')
if results:
    print(f"Got {len(results)} results")
else:
    print("Query failed, check logs")
```

### Example 4: Logging in Custom Clients

```python
from src.clients.base_client import BaseClient
from src.logging_config import get_logger
from src.exceptions import ConnectionError, QueryExecutionError

class CustomClient(BaseClient):
    def __init__(self, config):
        super().__init__(config)
        # Logger automatically created by BaseClient
        self.logger.info("CustomClient initialized")
    
    def connect(self):
        try:
            self.logger.debug("Attempting connection...")
            # Connection logic
            self.connection = create_connection()
            self.logger.info("Connected successfully")
        except Exception as e:
            self.logger.error(f"Connection failed: {e}", exc_info=True)
            raise ConnectionError(f"Failed to connect: {e}")
    
    def execute_query(self, query, params=None):
        try:
            self.logger.debug(f"Executing query: {query[:100]}")
            # Query logic
            results = self.connection.execute(query, params)
            self.logger.info(f"Query returned {len(results)} rows")
            return results
        except Exception as e:
            self.logger.error(f"Query failed: {e}", exc_info=True)
            raise QueryExecutionError(f"Query execution failed: {e}")
```

## Best Practices

### 1. Use Specific Exceptions

✅ **Good:**
```python
from src.exceptions import ConnectionError as ClientConnectionError

try:
    client.connect()
except ClientConnectionError as e:
    # Handle connection error specifically
    pass
```

❌ **Bad:**
```python
try:
    client.connect()
except Exception as e:
    # Too broad, catches everything
    pass
```

### 2. Log at Appropriate Levels

```python
logger.debug("Detailed diagnostic information")  # Development
logger.info("Normal operation information")      # Production
logger.warning("Warning about potential issues") # Attention needed
logger.error("Error occurred, operation failed") # Requires action
logger.critical("Critical system failure")       # Immediate action
```

### 3. Include Context in Log Messages

✅ **Good:**
```python
logger.error(f"Failed to connect to {host}:{port} - {error}", exc_info=True)
```

❌ **Bad:**
```python
logger.error("Connection failed")
```

### 4. Use exc_info for Stack Traces

```python
try:
    risky_operation()
except Exception as e:
    logger.error("Operation failed", exc_info=True)  # Includes stack trace
```

### 5. Clean Up Resources

```python
# Use context managers for automatic cleanup
with client:
    results = client.execute_query(query)
# Connection automatically closed, even on error
```

### 6. Don't Log Sensitive Information

❌ **Bad:**
```python
logger.info(f"Connecting with password: {password}")
```

✅ **Good:**
```python
logger.info(f"Connecting to {host} as {username}")
```

### 7. Catch and Re-raise with Context

```python
try:
    client.connect()
except Exception as e:
    logger.error(f"Connection failed: {e}", exc_info=True)
    raise ConnectionError(f"Failed to connect to database: {e}") from e
```

## Log File Locations

Default log file locations (when specified):

```
logs/
├── application.log      # Main application log
├── errors.log          # Error-level logs only
└── debug.log           # Debug-level logs
```

## Environment Variables

You can control logging via environment variables:

```bash
export FACTORY_INGESTION_LOG_LEVEL=DEBUG
export FACTORY_INGESTION_LOG_FILE=logs/app.log
```

## Troubleshooting

### Issue: No logs appearing

**Solution:** Check logging level and ensure logger is configured:
```python
from src.logging_config import setup_logging
setup_logging(level='DEBUG', log_to_console=True)
```

### Issue: Too many logs

**Solution:** Increase log level:
```python
setup_logging(level='WARNING')  # Only warnings and errors
```

### Issue: Need logs in file

**Solution:** Specify log file:
```python
setup_logging(level='INFO', log_file='logs/app.log')
```

### Issue: Want to see stack traces

**Solution:** Use DEBUG level and exc_info:
```python
setup_logging(level='DEBUG')
logger.error("Error occurred", exc_info=True)
```

## Summary

The Factory Ingestion library provides:
- ✅ **10 custom exception types** for specific error scenarios
- ✅ **Structured logging** with 5 levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- ✅ **Colored console output** for better readability
- ✅ **File logging** support with rotation
- ✅ **Context managers** for automatic resource cleanup
- ✅ **Comprehensive error messages** with stack traces
- ✅ **Module-level loggers** for organized logging

All errors are logged automatically, and you can control verbosity via command-line arguments or programmatic configuration.
