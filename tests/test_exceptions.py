import pytest
from src.exceptions import (
    FactoryIngestionError,
    ConnectionError as ClientConnectionError,
    QueryExecutionError,
    ConfigurationError,
    ClientNotFoundError,
    InvalidSourceTypeError,
    MissingDependencyError,
    AuthenticationError,
    TimeoutError,
    ValidationError
)


class TestExceptions:
    """Test custom exception classes."""
    
    def test_base_exception(self):
        """Test base FactoryIngestionError."""
        error = FactoryIngestionError("Base error")
        assert str(error) == "Base error"
        assert isinstance(error, Exception)
    
    def test_connection_error(self):
        """Test ConnectionError exception."""
        error = ClientConnectionError("Connection failed")
        assert str(error) == "Connection failed"
        assert isinstance(error, FactoryIngestionError)
        assert isinstance(error, Exception)
    
    def test_query_execution_error(self):
        """Test QueryExecutionError exception."""
        error = QueryExecutionError("Query failed")
        assert str(error) == "Query failed"
        assert isinstance(error, FactoryIngestionError)
    
    def test_configuration_error(self):
        """Test ConfigurationError exception."""
        error = ConfigurationError("Invalid config")
        assert str(error) == "Invalid config"
        assert isinstance(error, FactoryIngestionError)
    
    def test_client_not_found_error(self):
        """Test ClientNotFoundError exception."""
        error = ClientNotFoundError("Client not found")
        assert str(error) == "Client not found"
        assert isinstance(error, FactoryIngestionError)
    
    def test_invalid_source_type_error(self):
        """Test InvalidSourceTypeError exception."""
        error = InvalidSourceTypeError("Invalid source type")
        assert str(error) == "Invalid source type"
        assert isinstance(error, FactoryIngestionError)
    
    def test_missing_dependency_error(self):
        """Test MissingDependencyError exception."""
        error = MissingDependencyError("Dependency missing")
        assert str(error) == "Dependency missing"
        assert isinstance(error, FactoryIngestionError)
    
    def test_authentication_error(self):
        """Test AuthenticationError exception."""
        error = AuthenticationError("Auth failed")
        assert str(error) == "Auth failed"
        assert isinstance(error, FactoryIngestionError)
    
    def test_timeout_error(self):
        """Test TimeoutError exception."""
        error = TimeoutError("Operation timed out")
        assert str(error) == "Operation timed out"
        assert isinstance(error, FactoryIngestionError)
    
    def test_validation_error(self):
        """Test ValidationError exception."""
        error = ValidationError("Validation failed")
        assert str(error) == "Validation failed"
        assert isinstance(error, FactoryIngestionError)
    
    def test_exception_inheritance_chain(self):
        """Test that all exceptions inherit properly."""
        exceptions = [
            ClientConnectionError,
            QueryExecutionError,
            ConfigurationError,
            ClientNotFoundError,
            InvalidSourceTypeError,
            MissingDependencyError,
            AuthenticationError,
            TimeoutError,
            ValidationError
        ]
        
        for exc_class in exceptions:
            error = exc_class("Test message")
            assert isinstance(error, FactoryIngestionError)
            assert isinstance(error, Exception)
    
    def test_exception_with_cause(self):
        """Test exception chaining."""
        original_error = ValueError("Original error")
        
        try:
            raise ClientConnectionError("Connection failed") from original_error
        except ClientConnectionError as e:
            assert str(e) == "Connection failed"
            assert e.__cause__ == original_error
    
    def test_exception_can_be_caught_as_base(self):
        """Test that specific exceptions can be caught as base exception."""
        try:
            raise QueryExecutionError("Query failed")
        except FactoryIngestionError as e:
            assert str(e) == "Query failed"
    
    def test_exception_can_be_caught_specifically(self):
        """Test that exceptions can be caught by their specific type."""
        with pytest.raises(ConfigurationError) as exc_info:
            raise ConfigurationError("Config error")
        
        assert str(exc_info.value) == "Config error"
