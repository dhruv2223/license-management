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
            "url": os.getenv("CLIENT_DB_URL", "jdbc:postgresql://client-db:5432/clientdb"),
            "dbname": "clientdb",  # ✅ ADD THIS
            "host": os.getenv("CLIENT_DB_HOST", "client-db"),
            "port": "5432",
            "user": os.getenv("CLIENT_DB_USER", "client_user"),
            "password": os.getenv("CLIENT_DB_PASSWORD", "client_pass"),
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
            "url": os.getenv("KAKSHA_DB_URL", "jdbc:postgresql://kaksha-db:5432/kakshadb"),
            "user": os.getenv("KAKSHA_DB_USER", "postgres"), 
            "password": os.getenv("KAKSHA_DB_PASSWORD", "kaksha_pass"),
            "driver": "org.postgresql.Driver",
            "dbname": "kakshadb",
            "host": os.getenv("KAKSHA_DB_HOST", "kaksha-db"),
            "port": "5432",
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
