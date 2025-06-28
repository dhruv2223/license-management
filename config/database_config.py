import os
from typing import Dict, Any
from dotenv import load_dotenv
import logging
load_dotenv()

class DatabaseConfig:
    """
    Database configuration management for Client and Kaksha databases
    """
    
    @staticmethod
    def get_client_db_properties() -> Dict[str, str]:
        """
        Returns Client DB connection properties
        """
        return {
            "url": os.getenv("CLIENT_DB_URL", "jdbc:postgresql://localhost:5432/client_db"),
            "user": os.getenv("CLIENT_DB_USER", "postgres"),
            "password": os.getenv("CLIENT_DB_PASSWORD", "password"),
            "driver": "org.postgresql.Driver",
            "ssl": "false",
            "sslmode": "prefer",
            "fetchsize": "10000",
            "batchsize": "10000"
        }
    
    @staticmethod
    def get_kaksha_db_properties() -> Dict[str, str]:
        """
        Returns Kaksha DB connection properties
        """
        return {
            "url": os.getenv("KAKSHA_DB_URL", "jdbc:postgresql://localhost:5433/kaksha_db"),
            "user": os.getenv("KAKSHA_DB_USER", "postgres"), 
            "password": os.getenv("KAKSHA_DB_PASSWORD", "password"),
            "driver": "org.postgresql.Driver",
            "ssl": "false",
            "sslmode": "prefer",
            "fetchsize": "10000",
            "batchsize": "10000"
        }
    
    @staticmethod
    def validate_connections() -> bool:
        """
        Validates database connections are properly configured
        """
        try:
            client_props = DatabaseConfig.get_client_db_properties()
            kaksha_props = DatabaseConfig.get_kaksha_db_properties()
            
            # Basic validation
            required_keys = ["url", "user", "password", "driver"]
            
            for props, db_name in [(client_props, "Client"), (kaksha_props, "Kaksha")]:
                for key in required_keys:
                    if not props.get(key):
                        logging.error(f"Missing {key} for {db_name} database")
                        return False
            
            logging.info("Database configurations validated successfully")
            return True
            
        except Exception as e:
            logging.error(f"Database configuration validation failed: {str(e)}")
            return False
