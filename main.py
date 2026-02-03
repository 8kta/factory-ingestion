import argparse
import sys
from pathlib import Path

from src.client_factory import ClientFactory, ClientBuilder
from src.logging_config import setup_logging, get_logger
from src.exceptions import (
    FactoryIngestionError,
    ConnectionError as ClientConnectionError,
    ConfigurationError,
    QueryExecutionError
)

# Setup logging
logger = get_logger('main')


def example_usage_with_builder():
    print("\n=== Example 1: Using ClientBuilder with direct configuration ===")
    
    try:
        logger.info("Starting example with ClientBuilder")
        client = (ClientBuilder()
                  .with_source_type('postgres')
                  .add_config_param('host', 'localhost')
                  .add_config_param('port', 5432)
                  .add_config_param('database', 'my_database')
                  .add_config_param('username', 'postgres')
                  .add_config_param('password', 'password')
                  .build())
        
        with client:
            print(f"Connected to PostgreSQL: {client.is_connected()}")
            results = client.execute_query("SELECT version();")
            print(f"Query results: {results}")
            logger.info("Example completed successfully")
    except ClientConnectionError as e:
        logger.error(f"Connection error: {e}")
        print(f"❌ Connection failed: {e}")
    except QueryExecutionError as e:
        logger.error(f"Query execution error: {e}")
        print(f"❌ Query failed: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"❌ Error: {e}")


def example_usage_with_config_file():
    print("\n=== Example 2: Using ClientBuilder with configuration file ===")
    
    try:
        logger.info("Starting example with config file")
        client = (ClientBuilder()
                  .with_config_file('config/sources.yaml', 'my_postgres')
                  .build())
        
        with client:
            print(f"Connected to PostgreSQL: {client.is_connected()}")
            results = client.execute_query("SELECT current_database();")
            print(f"Query results: {results}")
            logger.info("Example completed successfully")
    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        print(f"❌ Configuration error: {e}")
    except ClientConnectionError as e:
        logger.error(f"Connection error: {e}")
        print(f"❌ Connection failed: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"❌ Error: {e}")


def example_usage_with_factory():
    print("\n=== Example 3: Using ClientFactory directly ===")
    
    try:
        logger.info("Starting example with ClientFactory")
        config = {
            'host': 'localhost',
            'port': 3306,
            'database': 'my_database',
            'username': 'root',
            'password': 'password'
        }
        
        client = ClientFactory.create_client('mysql', config)
        
        with client:
            print(f"Connected to MySQL: {client.is_connected()}")
            results = client.execute_query("SELECT DATABASE();")
            print(f"Query results: {results}")
            logger.info("Example completed successfully")
    except ClientConnectionError as e:
        logger.error(f"Connection error: {e}")
        print(f"❌ Connection failed: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"❌ Error: {e}")


def example_multiple_sources():
    print("\n=== Example 4: Working with multiple sources ===")
    
    config_path = 'config/sources.yaml'
    
    sources_to_query = ['my_postgres', 'my_mysql', 'my_sqlite']
    
    for source_name in sources_to_query:
        try:
            print(f"\n--- Connecting to {source_name} ---")
            client = ClientFactory.create_from_config_file(config_path, source_name)
            
            with client:
                print(f"Connected: {client.is_connected()}")
                
        except Exception as e:
            print(f"Error with {source_name}: {e}")


def example_messaging_systems():
    print("\n=== Example 5: Working with messaging systems ===")
    
    print("\n--- Kafka Example ---")
    kafka_client = (ClientBuilder()
                    .with_config_file('config/sources.yaml', 'my_kafka')
                    .build())
    
    with kafka_client:
        messages = kafka_client.execute_query('', {'max_messages': 5, 'timeout_ms': 1000})
        print(f"Received {len(messages)} messages from Kafka")
    
    print("\n--- RabbitMQ Example ---")
    rabbitmq_client = (ClientBuilder()
                       .with_config_file('config/sources.yaml', 'my_rabbitmq')
                       .build())
    
    with rabbitmq_client:
        rabbitmq_client.publish_message({'event': 'test', 'data': 'example'})
        print("Message published to RabbitMQ")
        
        messages = rabbitmq_client.execute_query('', {'max_messages': 5})
        print(f"Received {len(messages)} messages from RabbitMQ")


def example_aws_services():
    print("\n=== Example 6: Working with AWS services ===")
    
    print("\n--- SQS Example ---")
    sqs_client = (ClientBuilder()
                  .with_config_file('config/sources.yaml', 'my_sqs')
                  .build())
    
    with sqs_client:
        sqs_client.send_message('Test message body')
        print("Message sent to SQS")
        
        messages = sqs_client.execute_query('', {'max_messages': 10, 'wait_time': 5})
        print(f"Received {len(messages)} messages from SQS")
    
    print("\n--- SNS Example ---")
    sns_client = (ClientBuilder()
                  .with_config_file('config/sources.yaml', 'my_sns')
                  .build())
    
    with sns_client:
        response = sns_client.publish_message('Test notification', subject='Test Subject')
        print(f"Message published to SNS: {response['MessageId']}")


def example_search_engines():
    print("\n=== Example 7: Working with search engines ===")
    
    print("\n--- OpenSearch Example ---")
    opensearch_client = (ClientBuilder()
                         .with_config_file('config/sources.yaml', 'my_opensearch')
                         .build())
    
    with opensearch_client:
        results = opensearch_client.execute_query(
            'status:active',
            {'index': 'my_index', 'body': {'size': 10}}
        )
        print(f"Found {len(results)} documents in OpenSearch")
    
    print("\n--- Solr Example ---")
    solr_client = (ClientBuilder()
                   .with_config_file('config/sources.yaml', 'my_solr')
                   .build())
    
    with solr_client:
        results = solr_client.execute_query('*:*', {'rows': 10})
        print(f"Found {len(results)} documents in Solr")


def main():
    parser = argparse.ArgumentParser(
        description='Multi-source data client factory',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Query a specific source from config file
  python main.py --config config/sources.yaml --source my_postgres --query "SELECT * FROM users LIMIT 10"
  
  # Query using single source config
  python main.py --config config/single_source.json --query "SELECT * FROM users LIMIT 10"
  
  # List available sources in config
  python main.py --config config/sources.yaml --list-sources
  
  # Run example demonstrations
  python main.py --examples
  
  # Enable debug logging
  python main.py --config config/sources.yaml --source my_postgres --query "SELECT 1" --log-level DEBUG
        """
    )
    
    parser.add_argument('--config', '-c', help='Path to configuration file (YAML or JSON)')
    parser.add_argument('--source', '-s', help='Source name (required for multi-source configs)')
    parser.add_argument('--query', '-q', help='Query to execute')
    parser.add_argument('--list-sources', action='store_true', help='List available sources in config')
    parser.add_argument('--examples', action='store_true', help='Run example demonstrations')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Set logging level (default: INFO)')
    parser.add_argument('--log-file', help='Path to log file (optional)')
    
    args = parser.parse_args()
    
    # Setup logging based on arguments
    setup_logging(
        level=args.log_level,
        log_file=args.log_file,
        log_to_console=True
    )
    
    logger.info(f"Starting Factory Ingestion CLI with log level: {args.log_level}")
    
    if args.examples:
        logger.info("Running example demonstrations")
        print("Running example demonstrations...")
        print("\nNote: These examples will fail if the actual services are not running.")
        print("They are provided to demonstrate the API usage.\n")
        
        examples = [
            example_usage_with_builder,
            example_usage_with_config_file,
            example_usage_with_factory,
            example_multiple_sources,
            example_messaging_systems,
            example_aws_services,
            example_search_engines
        ]
        
        for example in examples:
            try:
                example()
            except Exception as e:
                logger.warning(f"Example failed: {e}")
                print(f"Example failed (expected if services not running): {e}")
        
        return
    
    if not args.config:
        logger.warning("No configuration file specified")
        parser.print_help()
        return
    
    if args.list_sources:
        try:
            logger.info(f"Listing sources from config: {args.config}")
            config_data = ClientFactory.load_config(args.config)
            if 'sources' in config_data:
                print("Available sources:")
                for source_name, source_config in config_data['sources'].items():
                    source_type = source_config.get('type', 'unknown')
                    print(f"  - {source_name} ({source_type})")
            else:
                print("Single source configuration:")
                print(f"  Type: {config_data.get('type', 'unknown')}")
        except ConfigurationError as e:
            logger.error(f"Configuration error: {e}")
            print(f"❌ Configuration error: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            print(f"❌ Error: {e}", file=sys.stderr)
            sys.exit(1)
        return
    
    if not args.query:
        logger.warning("No query specified")
        print("Error: --query is required")
        parser.print_help()
        return
    
    try:
        logger.info(f"Creating client from config: {args.config}, source: {args.source}")
        client = (ClientBuilder()
                  .with_config_file(args.config, args.source)
                  .build())
        
        print(f"Connecting to {args.source or 'source'}...")
        logger.info(f"Connecting to {args.source or 'source'}")
        
        with client:
            print(f"✓ Connected: {client.is_connected()}")
            print(f"\nExecuting query: {args.query}")
            logger.info(f"Executing query: {args.query}")
            
            results = client.execute_query(args.query)
            
            print(f"\n✓ Results ({len(results)} rows):")
            for i, row in enumerate(results, 1):
                print(f"{i}. {row}")
            
            logger.info(f"Query completed successfully, returned {len(results)} rows")
    
    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        print(f"❌ Configuration error: {e}", file=sys.stderr)
        sys.exit(1)
    except ClientConnectionError as e:
        logger.error(f"Connection error: {e}")
        print(f"❌ Connection failed: {e}", file=sys.stderr)
        sys.exit(1)
    except QueryExecutionError as e:
        logger.error(f"Query execution error: {e}")
        print(f"❌ Query failed: {e}", file=sys.stderr)
        sys.exit(1)
    except FactoryIngestionError as e:
        logger.error(f"Factory ingestion error: {e}")
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"❌ Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
