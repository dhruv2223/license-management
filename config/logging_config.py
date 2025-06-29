import logging
import sys
from datetime import datetime
import os
from typing import Optional

class LoggingConfig:
    """
    Centralized logging configuration for the license management system
    """
    
    # Define log level hierarchy for validation
    VALID_LOG_LEVELS = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'WARN': logging.WARNING,  # Alias for WARNING
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    
    @staticmethod
    def setup_logging(
        log_level: str = "INFO",
        log_file: Optional[str] = None,
        enable_console: bool = True
    ) -> None:
        """
        Sets up comprehensive logging for the application
        
        Args:
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_file: Optional log file path
            enable_console: Whether to enable console logging
        """
        
        # Validate and normalize log level
        log_level = log_level.upper()
        if log_level not in LoggingConfig.VALID_LOG_LEVELS:
            print(f"Warning: Invalid log level '{log_level}'. Defaulting to INFO.")
            log_level = "INFO"
        
        numeric_level = LoggingConfig.VALID_LOG_LEVELS[log_level]
        
        # Create logs directory if it doesn't exist
        if log_file:
            log_dir = os.path.dirname(log_file)
            if log_dir:
                os.makedirs(log_dir, exist_ok=True)
        
        # Configure root logger
        logger = logging.getLogger()
        logger.setLevel(numeric_level)
        logger.handlers.clear()  # Clear existing handlers
        
        # Console handler with level-specific formatting
        if enable_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(numeric_level)
            
            # Different formatting based on log level
            if numeric_level <= logging.DEBUG:
                console_format = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            else:
                console_format = '%(asctime)s - %(levelname)s - %(message)s'
            
            console_formatter = logging.Formatter(console_format)
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
        
        # File handler with detailed formatting
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(numeric_level)
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        
        # Configure third-party library logging levels
        LoggingConfig._configure_third_party_loggers(log_level)
        
        # Log the configuration setup
        logger.info("="*50)
        logger.info("Logging configuration completed")
        logger.info(f"Log level set to: {log_level} ({numeric_level})")
        if log_file:
            logger.info(f"Logging to file: {log_file}")
        logger.info("="*50)
    
    @staticmethod
    def _configure_third_party_loggers(log_level: str) -> None:
        """Configure logging levels for third-party libraries"""
        
        # Suppress noisy third-party loggers unless DEBUG level
        if log_level != "DEBUG":
            logging.getLogger("pyspark").setLevel(logging.WARNING)
            logging.getLogger("py4j").setLevel(logging.WARNING)
            logging.getLogger("urllib3").setLevel(logging.WARNING)
            logging.getLogger("requests").setLevel(logging.WARNING)
            logging.getLogger("boto3").setLevel(logging.WARNING)
            logging.getLogger("botocore").setLevel(logging.WARNING)
        else:
            # In DEBUG mode, allow more verbose third-party logging
            logging.getLogger("pyspark").setLevel(logging.INFO)
            logging.getLogger("py4j").setLevel(logging.INFO)
    
    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """
        Get a logger instance for a specific module/class
        
        Args:
            name: Logger name (typically __name__)
            
        Returns:
            Configured logger instance
        """
        return logging.getLogger(name)
