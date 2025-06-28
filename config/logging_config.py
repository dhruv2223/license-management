import logging
import sys
from datetime import datetime
import os

class LoggingConfig:
    """
    Centralized logging configuration for the license management system
    """
    
    @staticmethod
    def setup_logging(
        log_level: str = "INFO",
        log_file: str = None,
        enable_console: bool = True
    ) -> None:
        """
        Sets up comprehensive logging for the application
        
        Args:
            log_level: Logging level (DEBUG, INFO, WARN, ERROR)
            log_file: Optional log file path
            enable_console: Whether to enable console logging
        """
        
        # Create logs directory if it doesn't exist
        if log_file:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # Configure root logger
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[]
        )
        
        logger = logging.getLogger()
        logger.handlers.clear()  # Clear existing handlers
        
        # Console handler
        if enable_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
        
        # File handler
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        
        # Application-specific loggers
        logging.getLogger("pyspark").setLevel(logging.WARN)
        logging.getLogger("py4j").setLevel(logging.WARN)
        
        logging.info("Logging configuration completed")
        logging.info(f"Log level set to: {log_level}")
        if log_file:
            logging.info(f"Logging to file: {log_file}")
