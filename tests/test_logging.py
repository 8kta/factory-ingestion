import pytest
import logging
import tempfile
from pathlib import Path
from src.logging_config import setup_logging, get_logger, ColoredFormatter


class TestLoggingConfig:
    """Test logging configuration functionality."""
    
    def test_setup_logging_default(self):
        """Test default logging setup."""
        logger = setup_logging()
        assert logger is not None
        assert logger.name == 'factory_ingestion'
        assert logger.level == logging.INFO
    
    def test_setup_logging_with_level(self):
        """Test logging setup with custom level."""
        logger = setup_logging(level='DEBUG')
        assert logger.level == logging.DEBUG
        
        logger = setup_logging(level='ERROR')
        assert logger.level == logging.ERROR
    
    def test_setup_logging_with_file(self, tmp_path):
        """Test logging setup with file output."""
        log_file = tmp_path / "test.log"
        logger = setup_logging(level='INFO', log_file=str(log_file))
        
        # Log a message
        logger.info("Test message")
        
        # Check file was created
        assert log_file.exists()
        
        # Check message was written
        content = log_file.read_text()
        assert "Test message" in content
    
    def test_setup_logging_console_only(self):
        """Test logging setup with console only."""
        logger = setup_logging(level='INFO', log_to_console=True)
        
        # Should have at least one handler (console)
        assert len(logger.handlers) > 0
    
    def test_setup_logging_custom_format(self):
        """Test logging setup with custom format."""
        custom_format = '%(levelname)s - %(message)s'
        logger = setup_logging(level='INFO', format_string=custom_format)
        
        assert logger is not None
    
    def test_get_logger(self):
        """Test getting module-specific logger."""
        logger = get_logger('TestModule')
        
        assert logger.name == 'factory_ingestion.TestModule'
        assert isinstance(logger, logging.Logger)
    
    def test_get_logger_multiple_modules(self):
        """Test getting loggers for different modules."""
        logger1 = get_logger('Module1')
        logger2 = get_logger('Module2')
        
        assert logger1.name == 'factory_ingestion.Module1'
        assert logger2.name == 'factory_ingestion.Module2'
        assert logger1 is not logger2
    
    def test_colored_formatter(self):
        """Test ColoredFormatter."""
        formatter = ColoredFormatter('%(levelname)s - %(message)s')
        
        record = logging.LogRecord(
            name='test',
            level=logging.INFO,
            pathname='test.py',
            lineno=1,
            msg='Test message',
            args=(),
            exc_info=None
        )
        
        formatted = formatter.format(record)
        assert 'Test message' in formatted
    
    def test_logging_levels(self, tmp_path):
        """Test different logging levels."""
        log_file = tmp_path / "levels.log"
        logger = setup_logging(level='DEBUG', log_file=str(log_file))
        
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
        logger.critical("Critical message")
        
        content = log_file.read_text()
        assert "Debug message" in content
        assert "Info message" in content
        assert "Warning message" in content
        assert "Error message" in content
        assert "Critical message" in content
    
    def test_logging_with_exception(self, tmp_path):
        """Test logging with exception info."""
        log_file = tmp_path / "exception.log"
        logger = setup_logging(level='ERROR', log_file=str(log_file))
        
        try:
            raise ValueError("Test exception")
        except ValueError:
            logger.error("An error occurred", exc_info=True)
        
        content = log_file.read_text()
        assert "An error occurred" in content
        assert "ValueError" in content
        assert "Test exception" in content
    
    def test_logger_hierarchy(self):
        """Test logger hierarchy."""
        parent_logger = get_logger('Parent')
        child_logger = get_logger('Parent.Child')
        
        assert 'Parent' in parent_logger.name
        assert 'Parent.Child' in child_logger.name
    
    def test_setup_logging_creates_log_directory(self, tmp_path):
        """Test that log directory is created if it doesn't exist."""
        log_dir = tmp_path / "logs" / "subdir"
        log_file = log_dir / "test.log"
        
        logger = setup_logging(level='INFO', log_file=str(log_file))
        logger.info("Test message")
        
        assert log_dir.exists()
        assert log_file.exists()
    
    def test_multiple_handlers(self, tmp_path):
        """Test logging with both console and file handlers."""
        log_file = tmp_path / "multi.log"
        logger = setup_logging(
            level='INFO',
            log_file=str(log_file),
            log_to_console=True
        )
        
        # Should have 2 handlers (console + file)
        assert len(logger.handlers) == 2
    
    def test_logger_clears_existing_handlers(self):
        """Test that setup_logging clears existing handlers."""
        # Setup first time
        logger1 = setup_logging(level='INFO')
        handler_count1 = len(logger1.handlers)
        
        # Setup second time
        logger2 = setup_logging(level='DEBUG')
        handler_count2 = len(logger2.handlers)
        
        # Should have same number of handlers (old ones cleared)
        assert handler_count1 == handler_count2
