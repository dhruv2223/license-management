from pyspark.sql import SparkSession
import logging
from typing import Optional
from config.spark_config import SparkConfigManager
from config.database_config import DatabaseConfig
from config.logging_config import LoggingConfig

class LicenseProcessor:
    """
    Main class for License Management processing
    Handles Spark session lifecycle and database connections
    """
    
    def __init__(self, app_name: str = "LicenseManagementPipeline"):
        """
        Initialize License Processor
        
        Args:
            app_name: Spark application name
        """
        self.app_name = app_name
        self.spark: Optional[SparkSession] = None
        self.client_db_props = None
        self.kaksha_db_props = None
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def initialize(self) -> bool:
        """
        Initialize Spark session and database connections
        
            bool: True if initialization successful, False otherwise
        Returns:
        """
        try:
            self.logger.info("Initializing License Processor...")
            
            # Validate database configurations
            if not DatabaseConfig.validate_connections():
                self.logger.error("Database configuration validation failed")
                return False
            
            # Get database properties
            self.client_db_props = DatabaseConfig.get_client_db_properties()
            self.kaksha_db_props = DatabaseConfig.get_kaksha_db_properties()
            
            # Create Spark session
            self.spark = SparkConfigManager.create_spark_session(self.app_name)
            
            self.logger.info("License Processor initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize License Processor: {str(e)}")
            return False
    
    def cleanup(self) -> None:
        """
        Clean up resources and stop Spark session
        """
        try:
            if self.spark:
                self.spark.stop()
                self.logger.info("Spark session stopped successfully")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
    
    def test_connections(self) -> bool:
        """
        Test database connections
        
        Returns:
            bool: True if all connections successful
        """
        try:
            self.logger.info("Testing database connections...")
            
            # Test Client DB connection
            client_test = self.spark.read \
                .format("jdbc") \
                .options(**self.client_db_props) \
                .option("query", "SELECT 1 as test") \
                .load()
            
            client_count = client_test.count()
            self.logger.info(f"Client DB connection successful: {client_count} record")
            
            # Test Kaksha DB connection  
            kaksha_test = self.spark.read \
                .format("jdbc") \
                .options(**self.kaksha_db_props) \
                .option("query", "SELECT 1 as test") \
                .load()
            
            kaksha_count = kaksha_test.count()
            self.logger.info(f"Kaksha DB connection successful: {kaksha_count} record")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Database connection test failed: {str(e)}")
            return False
