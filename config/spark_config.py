from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging
import os
from typing import Dict, Any

class SparkConfigManager:
    """
    Centralized Spark configuration management for License Management System
    """
    
    @staticmethod
    def get_optimized_spark_config() -> Dict[str, str]:
        """
        Returns optimized Spark configuration for license processing
        Tuned for handling 10M+ records efficiently
        """
        return {
            # Adaptive Query Execution
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true", 
            "spark.sql.adaptive.skewJoin.enabled": "true",
            
            # Performance Optimizations
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
            
            # Memory Management
            "spark.executor.memory": "4g",
            "spark.executor.cores": "4",
            "spark.executor.instances": "3",
            "spark.driver.memory": "2g",
            "spark.driver.maxResultSize": "1g",
            
            # Shuffle Optimization
            "spark.sql.shuffle.partitions": "200",
            "spark.shuffle.compress": "true",
            "spark.shuffle.spill.compress": "true",
            
            # Catalyst Optimizer
            "spark.sql.cbo.enabled": "true",
            "spark.sql.statistics.histogram.enabled": "true",
            
            # Connection Pool
            "spark.sql.execution.arrow.maxRecordsPerBatch": "10000"
        }
    
    @staticmethod
    def create_spark_session(app_name: str = "LicenseManagementPipeline") -> SparkSession:
        """
        Creates optimized Spark session for license management
        
        Args:
            app_name: Application name for Spark UI
            
        Returns:
            Configured SparkSession instance
        """
        config = SparkConfigManager.get_optimized_spark_config()
        
        builder = SparkSession.builder.appName(app_name)
        
        # Apply all configurations
        for key, value in config.items():
            builder = builder.config(key, value)
        
        # Add PostgreSQL driver
        builder = builder.config(
            "spark.jars", 
            "https://jdbc.postgresql.org/download/postgresql-42.6.0.jar"
        )
        
        spark = builder.getOrCreate()
        
        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")
        
        logging.info(f"Spark Session created: {app_name}")
        logging.info(f"Spark Version: {spark.version}")
        logging.info(f"Available cores: {spark.sparkContext.defaultParallelism}")
        
        return spark
