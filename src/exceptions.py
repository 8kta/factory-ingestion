"""
Custom exceptions for the Factory Ingestion client library.
"""


class FactoryIngestionError(Exception):
    """Base exception for all factory ingestion errors."""
    pass


class ConnectionError(FactoryIngestionError):
    """Raised when a connection to a data source fails."""
    pass


class QueryExecutionError(FactoryIngestionError):
    """Raised when a query execution fails."""
    pass


class ConfigurationError(FactoryIngestionError):
    """Raised when there's an error in configuration."""
    pass


class ClientNotFoundError(FactoryIngestionError):
    """Raised when a requested client type is not found."""
    pass


class InvalidSourceTypeError(FactoryIngestionError):
    """Raised when an invalid source type is specified."""
    pass


class MissingDependencyError(FactoryIngestionError):
    """Raised when a required dependency is not installed."""
    pass


class AuthenticationError(FactoryIngestionError):
    """Raised when authentication fails."""
    pass


class TimeoutError(FactoryIngestionError):
    """Raised when an operation times out."""
    pass


class ValidationError(FactoryIngestionError):
    """Raised when data validation fails."""
    pass
