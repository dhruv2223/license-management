from config.logging_config import LoggingConfig
from src.pyspark_app.license_processor import LicenseProcessor
import os
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
    
    # Get logger for this module
    logger = LoggingConfig.get_logger(__name__)
    
    processor = None
    
    try:
        # Initialize License Processor
        processor = LicenseProcessor(logger=logger)
        
        if not processor.initialize():

            return False
        
        # Test connections
        if not processor.test_connections():
            logger.error("Database connection tests failed")
            return False
        
        logger.info("Step 1.1 Environment Setup completed successfully!")
        
        # Placeholder for future steps
    
        processor.run_pending_license_request_job() 

        logger.info("Step 1.2 run pending license request completed successfully!")
        processor.run_expired_license_job() 

        logger.info("Step 1.3 run expired license job completed successfully!")
        processor.count_user_license_by_type()

        logger.info("count user by license type completed successfully!")
        return True 
        
        
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        return False
        
    finally:
        if processor:
            processor.cleanup()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
