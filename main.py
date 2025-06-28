from config.logging_config import LoggingConfig
from src.pyspark_app.license_processor import LicenseProcessor
import os
import logging
from datetime import datetime

def main():
    """
    Main entry point for the License Management System
    """
    
    # Setup logging
    log_file = os.getenv("LOG_FILE", f"logs/license_processor_{datetime.now().strftime('%Y%m%d')}.log")
    LoggingConfig.setup_logging(
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        log_file=log_file,
        enable_console=True
    )
    
    processor = None
    
    try:
        # Initialize License Processor
        processor = LicenseProcessor()
        
        if not processor.initialize():
            logging.error("Failed to initialize License Processor")
            return False
        
        # Test connections
        if not processor.test_connections():
            logging.error("Database connection tests failed")
            return False
        
        logging.info("Step 1.1 Environment Setup completed successfully!")
        
        # Placeholder for future steps
        logging.info("Ready for Step 1.2 - Database Schema Implementation")
        
        return True
        
    except Exception as e:
        logging.error(f"Application error: {str(e)}")
        return False
        
    finally:
        if processor:
            processor.cleanup()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
