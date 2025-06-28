"""
License Management System - Step 1.3c: User Count Aggregation
Advanced PySpark implementation with comprehensive aggregations and analytics
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import uuid

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    when, isnan, isnull, coalesce, lit, current_timestamp, date_format,
    datediff, months_between, to_date, regexp_replace, upper, trim,
    row_number, rank, dense_rank, lag, lead, first, last,
    collect_list, collect_set, size, array_contains, explode,
    countDistinct, approx_count_distinct, stddev, variance
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType, TimestampType


class UserCountAggregator:
    """
    Advanced user count aggregation with comprehensive analytics and reporting.
    Demonstrates multiple PySpark concepts including window functions, complex aggregations,
    and advanced DataFrame operations.
    """
    
    def __init__(self, spark: SparkSession, kaksha_db_props: Dict):
        self.spark = spark
        self.kaksha_db_props = kaksha_db_props
        self.logger = logging.getLogger(__name__)
        
        # Performance optimization settings
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
    def generate_user_count_reports(self) -> Dict[str, DataFrame]:
        """
        Generate comprehensive user count aggregation reports.
        
        Returns:
            Dict[str, DataFrame]: Dictionary containing various aggregation reports
        """
        try:
            self.logger.info("Starting user count aggregation process")
            
            # Load base data
            licenses_df = self._load_licenses_data()
            users_df = self._load_users_data()
            
            # Perform data quality checks
            self._validate_data_quality(licenses_df, users_df)
            
            # Generate various aggregation reports
            reports = {
                "basic_license_counts": self._basic_license_type_aggregation(licenses_df),
                "detailed_user_analytics": self._detailed_user_analytics(licenses_df, users_df),
                "temporal_trends": self._temporal_trend_analysis(licenses_df),
                "advanced_window_analytics": self._advanced_window_analytics(licenses_df),
                "user_behavior_segments": self._user_behavior_segmentation(licenses_df, users_df),
                "license_utilization_metrics": self._license_utilization_metrics(licenses_df, users_df),
                "executive_summary": self._executive_summary_report(licenses_df, users_df)
            }
            
            # Cache frequently used reports for performance
            reports["basic_license_counts"].cache()
            reports["executive_summary"].cache()
            
            self.logger.info("User count aggregation completed successfully")
            return reports
            
        except Exception as e:
            self.logger.error(f"Error in user count aggregation: {str(e)}")
            raise
    
    def _load_licenses_data(self) -> DataFrame:
        """Load licenses data with proper schema and validation."""
        try:
            licenses_df = self.spark.read \
                .format("jdbc") \
                .options(**self.kaksha_db_props) \
                .option("dbtable", "kaksha_licenses") \
                .load()
            
            # Add derived columns for better analytics
            licenses_df = licenses_df.withColumn(
                "is_active", 
                when(col("expiry_date") >= current_timestamp().cast("date"), True).otherwise(False)
            ).withColumn(
                "days_to_expiry",
                datediff(col("expiry_date"), current_timestamp().cast("date"))
            ).withColumn(
                "license_duration_months",
                months_between(col("expiry_date"), col("start_date"))
            ).withColumn(
                "license_age_days",
                datediff(current_timestamp().cast("date"), col("start_date"))
            )
            
            self.logger.info(f"Loaded {licenses_df.count()} license records")
            return licenses_df
            
        except Exception as e:
            self.logger.error(f"Error loading licenses data: {str(e)}")
            raise
    
    def _load_users_data(self) -> DataFrame:
        """Load users data with proper schema and validation."""
        try:
            users_df = self.spark.read \
                .format("jdbc") \
                .options(**self.kaksha_db_props) \
                .option("dbtable", "kaksha_users") \
                .load()
            
            # Add derived columns for user analytics
            users_df = users_df.withColumn(
                "days_since_last_login",
                when(col("lastlogin").isNotNull(),
                     datediff(current_timestamp(), col("lastlogin")))
                .otherwise(lit(None))
            ).withColumn(
                "user_activity_status",
                when(col("days_since_last_login") <= 7, "Active")
                .when(col("days_since_last_login") <= 30, "Moderate")
                .when(col("days_since_last_login") <= 90, "Inactive")
                .otherwise("Dormant")
            )
            
            self.logger.info(f"Loaded {users_df.count()} user records")
            return users_df
            
        except Exception as e:
            self.logger.error(f"Error loading users data: {str(e)}")
            raise
    
    def _validate_data_quality(self, licenses_df: DataFrame, users_df: DataFrame):
        """Perform comprehensive data quality validation."""
        try:
            # License data validation
            total_licenses = licenses_df.count()
            null_userids = licenses_df.filter(col("userid").isNull()).count()
            invalid_license_types = licenses_df.filter(
                ~col("license_type").isin(["Type 1", "Type 2", "Type 3"])
            ).count()
            invalid_dates = licenses_df.filter(
                col("expiry_date") <= col("start_date")
            ).count()
            
            # User data validation
            total_users = users_df.count()
            null_emails = users_df.filter(col("email").isNull()).count()
            
            # Log validation results
            self.logger.info(f"Data Quality Report:")
            self.logger.info(f"  Total licenses: {total_licenses}")
            self.logger.info(f"  Null user IDs: {null_userids}")
            self.logger.info(f"  Invalid license types: {invalid_license_types}")
            self.logger.info(f"  Invalid date ranges: {invalid_dates}")
            self.logger.info(f"  Total users: {total_users}")
            self.logger.info(f"  Null emails: {null_emails}")
            
            # Raise warning if data quality issues found
            if null_userids > 0 or invalid_license_types > 0 or invalid_dates > 0:
                self.logger.warning("Data quality issues detected - proceeding with caution")
                
        except Exception as e:
            self.logger.error(f"Error in data quality validation: {str(e)}")
            raise
    
    def _basic_license_type_aggregation(self, licenses_df: DataFrame) -> DataFrame:
        """
        Basic license type aggregation - Core requirement 1.3
        Group by license_type and count users
        """
        try:
            basic_counts = licenses_df.groupBy("license_type") \
                .agg(
                    countDistinct("userid").alias("unique_users"),
                    count("license_id").alias("total_licenses"),
                    spark_sum(when(col("is_active"), 1).otherwise(0)).alias("active_licenses"),
                    spark_sum(when(~col("is_active"), 1).otherwise(0)).alias("expired_licenses")
                ) \
                .withColumn("active_percentage", 
                           (col("active_licenses") / col("total_licenses") * 100).cast("decimal(5,2)")) \
                .orderBy("license_type")
            
            self.logger.info("Basic license type aggregation completed")
            return basic_counts
            
        except Exception as e:
            self.logger.error(f"Error in basic license type aggregation: {str(e)}")
            raise
    
    def _detailed_user_analytics(self, licenses_df: DataFrame, users_df: DataFrame) -> DataFrame:
        """
        Detailed user analytics with comprehensive metrics.
        Demonstrates complex joins and aggregations.
        """
        try:
            # Join licenses with users for comprehensive analytics
            user_license_df = licenses_df.join(users_df, "userid", "inner")
            
            # Complex aggregations per user
            user_analytics = user_license_df.groupBy("userid", "name", "email", "user_activity_status") \
                .agg(
                    count("license_id").alias("total_licenses"),
                    countDistinct("license_type").alias("unique_license_types"),
                    collect_set("license_type").alias("license_types_array"),
                    spark_sum(when(col("is_active"), 1).otherwise(0)).alias("active_licenses"),
                    avg("license_duration_months").alias("avg_license_duration"),
                    spark_max("expiry_date").alias("latest_expiry"),
                    spark_min("start_date").alias("earliest_start"),
                    spark_sum("days_to_expiry").alias("total_days_to_expiry"),
                    first("islicenseactive").alias("current_license_status")
                ) \
                .withColumn("license_portfolio_score",
                           (col("active_licenses") * 2 + col("unique_license_types") * 3).cast("integer")) \
                .withColumn("user_value_segment",
                           when(col("license_portfolio_score") >= 10, "Premium")
                           .when(col("license_portfolio_score") >= 5, "Standard")
                           .otherwise("Basic"))
            
            self.logger.info("Detailed user analytics completed")
            return user_analytics
            
        except Exception as e:
            self.logger.error(f"Error in detailed user analytics: {str(e)}")
            raise
    
    def _temporal_trend_analysis(self, licenses_df: DataFrame) -> DataFrame:
        """
        Temporal trend analysis using window functions.
        Demonstrates advanced time-based analytics.
        """
        try:
            # Add temporal columns
            temporal_df = licenses_df.withColumn("start_year", date_format(col("start_date"), "yyyy")) \
                .withColumn("start_month", date_format(col("start_date"), "yyyy-MM")) \
                .withColumn("expiry_year", date_format(col("expiry_date"), "yyyy"))
            
            # Monthly aggregations with window functions
            monthly_trends = temporal_df.groupBy("start_month", "license_type") \
                .agg(
                    count("license_id").alias("licenses_issued"),
                    countDistinct("userid").alias("unique_users")
                )
            
            # Add window functions for trend analysis
            window_spec = Window.partitionBy("license_type").orderBy("start_month")
            
            trend_analysis = monthly_trends.withColumn(
                "cumulative_licenses",
                spark_sum("licenses_issued").over(window_spec)
            ).withColumn(
                "previous_month_licenses",
                lag("licenses_issued").over(window_spec)
            ).withColumn(
                "license_growth_rate",
                when(col("previous_month_licenses").isNotNull(),
                     ((col("licenses_issued") - col("previous_month_licenses")) / 
                      col("previous_month_licenses") * 100).cast("decimal(5,2)"))
                .otherwise(lit(0))
            ).withColumn(
                "running_avg_licenses",
                avg("licenses_issued").over(window_spec.rowsBetween(-2, 0))
            )
            
            self.logger.info("Temporal trend analysis completed")
            return trend_analysis
            
        except Exception as e:
            self.logger.error(f"Error in temporal trend analysis: {str(e)}")
            raise
    
    def _advanced_window_analytics(self, licenses_df: DataFrame) -> DataFrame:
        """
        Advanced window analytics demonstrating complex ranking and analytical functions.
        """
        try:
            # Multiple window specifications
            user_window = Window.partitionBy("userid").orderBy(col("start_date").desc())
            type_window = Window.partitionBy("license_type").orderBy(col("license_duration_months").desc())
            global_window = Window.orderBy(col("start_date").desc())
            
            advanced_analytics = licenses_df.withColumn(
                "user_license_rank",
                row_number().over(user_window)
            ).withColumn(
                "is_latest_license",
                when(col("user_license_rank") == 1, True).otherwise(False)
            ).withColumn(
                "license_duration_rank",
                rank().over(type_window)
            ).withColumn(
                "license_duration_dense_rank",
                dense_rank().over(type_window)
            ).withColumn(
                "next_license_start",
                lead("start_date").over(user_window)
            ).withColumn(
                "prev_license_expiry",
                lag("expiry_date").over(user_window)
            ).withColumn(
                "license_gap_days",
                when(col("prev_license_expiry").isNotNull(),
                     datediff(col("start_date"), col("prev_license_expiry")))
                .otherwise(lit(None))
            ).withColumn(
                "global_license_percentile",
                (row_number().over(global_window) / count("*").over(Window.partitionBy()) * 100)
                .cast("decimal(5,2)")
            )
            
            # Filter to get meaningful analytics
            result = advanced_analytics.filter(col("user_license_rank") <= 3) \
                .select(
                    "userid", "license_id", "license_type", "start_date", "expiry_date",
                    "user_license_rank", "is_latest_license", "license_duration_rank",
                    "license_gap_days", "global_license_percentile"
                )
            
            self.logger.info("Advanced window analytics completed")
            return result
            
        except Exception as e:
            self.logger.error(f"Error in advanced window analytics: {str(e)}")
            raise
    
    def _user_behavior_segmentation(self, licenses_df: DataFrame, users_df: DataFrame) -> DataFrame:
        """
        User behavior segmentation using advanced analytics.
        """
        try:
            # Join and create behavior metrics
            behavior_df = licenses_df.join(users_df, "userid", "inner")
            
            # Segmentation logic
            segments = behavior_df.groupBy("userid") \
                .agg(
                    count("license_id").alias("total_licenses"),
                    countDistinct("license_type").alias("license_diversity"),
                    avg("license_duration_months").alias("avg_duration"),
                    spark_max("days_since_last_login").alias("days_since_login"),
                    spark_sum(when(col("is_active"), 1).otherwise(0)).alias("active_licenses")
                ) \
                .withColumn("user_segment",
                           when((col("total_licenses") >= 5) & (col("license_diversity") >= 2), "Power User")
                           .when((col("total_licenses") >= 3) & (col("active_licenses") >= 1), "Regular User")
                           .when(col("total_licenses") >= 1, "Casual User")
                           .otherwise("Inactive User")) \
                .withColumn("engagement_score",
                           (col("total_licenses") * 0.3 + 
                            col("license_diversity") * 0.4 + 
                            col("active_licenses") * 0.3).cast("decimal(5,2)"))
            
            # Segment summary
            segment_summary = segments.groupBy("user_segment") \
                .agg(
                    count("userid").alias("user_count"),
                    avg("engagement_score").alias("avg_engagement_score"),
                    avg("total_licenses").alias("avg_licenses_per_user")
                ) \
                .orderBy(col("avg_engagement_score").desc())
            
            self.logger.info("User behavior segmentation completed")
            return segment_summary
            
        except Exception as e:
            self.logger.error(f"Error in user behavior segmentation: {str(e)}")
            raise
    
    def _license_utilization_metrics(self, licenses_df: DataFrame, users_df: DataFrame) -> DataFrame:
        """
        License utilization metrics and efficiency analysis.
        """
        try:
            # Calculate utilization metrics
            utilization_df = licenses_df.join(users_df, "userid", "inner")
            
            utilization_metrics = utilization_df.groupBy("license_type") \
                .agg(
                    count("license_id").alias("total_issued"),
                    countDistinct("userid").alias("unique_users"),
                    spark_sum(when(col("is_active"), 1).otherwise(0)).alias("currently_active"),
                    avg("license_duration_months").alias("avg_duration_months"),
                    stddev("license_duration_months").alias("duration_stddev"),
                    avg("days_to_expiry").alias("avg_days_to_expiry"),
                    spark_sum(when(col("days_to_expiry") <= 30, 1).otherwise(0)).alias("expiring_soon"),
                    countDistinct(when(col("user_activity_status") == "Active", col("userid"))).alias("active_users_with_license")
                ) \
                .withColumn("utilization_rate",
                           (col("currently_active") / col("total_issued") * 100).cast("decimal(5,2)")) \
                .withColumn("user_efficiency_ratio",
                           (col("active_users_with_license") / col("unique_users") * 100).cast("decimal(5,2)")) \
                .withColumn("renewal_urgency_score",
                           (col("expiring_soon") / col("currently_active") * 100).cast("decimal(5,2)"))
            
            self.logger.info("License utilization metrics completed")
            return utilization_metrics
            
        except Exception as e:
            self.logger.error(f"Error in license utilization metrics: {str(e)}")
            raise
    
    def _executive_summary_report(self, licenses_df: DataFrame, users_df: DataFrame) -> DataFrame:
        """
        Executive summary report with key business metrics.
        """
        try:
            # High-level metrics
            total_licenses = licenses_df.count()
            total_users = users_df.count()
            active_licenses = licenses_df.filter(col("is_active")).count()
            unique_licensed_users = licenses_df.select("userid").distinct().count()
            
            # Create summary as single row DataFrame
            summary_data = [
                (
                    total_licenses,
                    total_users,
                    active_licenses,
                    unique_licensed_users,
                    round((active_licenses / total_licenses * 100), 2) if total_licenses > 0 else 0,
                    round((unique_licensed_users / total_users * 100), 2) if total_users > 0 else 0,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )
            ]
            
            summary_schema = StructType([
                StructField("total_licenses", IntegerType(), True),
                StructField("total_users", IntegerType(), True),
                StructField("active_licenses", IntegerType(), True),
                StructField("unique_licensed_users", IntegerType(), True),
                StructField("license_activation_rate", StringType(), True),
                StructField("user_licensing_penetration", StringType(), True),
                StructField("report_generated_at", StringType(), True)
            ])
            
            summary_df = self.spark.createDataFrame(summary_data, summary_schema)
            
            self.logger.info("Executive summary report completed")
            return summary_df
            
        except Exception as e:
            self.logger.error(f"Error in executive summary report: {str(e)}")
            raise
    
    def export_reports_to_database(self, reports: Dict[str, DataFrame], output_table_prefix: str = "report_"):
        """
        Export generated reports back to database for dashboards and reporting.
        """
        try:
            for report_name, report_df in reports.items():
                table_name = f"{output_table_prefix}{report_name}"
                
                # Add metadata columns
                report_with_metadata = report_df.withColumn("generated_at", current_timestamp()) \
                    .withColumn("report_type", lit(report_name))
                
                # Write to database
                report_with_metadata.write \
                    .format("jdbc") \
                    .options(**self.kaksha_db_props) \
                    .option("dbtable", table_name) \
                    .mode("overwrite") \
                    .save()
                
                self.logger.info(f"Exported report '{report_name}' to table '{table_name}'")
            
            self.logger.info("All reports exported successfully")
            
        except Exception as e:
            self.logger.error(f"Error exporting reports: {str(e)}")
            raise
    
    def print_report_summaries(self, reports: Dict[str, DataFrame]):
        """
        Print summary statistics for all generated reports.
        """
        try:
            self.logger.info("=== REPORT SUMMARIES ===")
            
            for report_name, report_df in reports.items():
                row_count = report_df.count()
                column_count = len(report_df.columns)
                
                self.logger.info(f"\n{report_name.upper()}:")
                self.logger.info(f"  Rows: {row_count}")
                self.logger.info(f"  Columns: {column_count}")
                self.logger.info(f"  Schema: {', '.join(report_df.columns)}")
                
                # Show sample data for smaller reports
                if row_count <= 20:
                    self.logger.info("  Sample Data:")
                    report_df.show(5, truncate=False)
                
        except Exception as e:
            self.logger.error(f"Error printing report summaries: {str(e)}")


# Example usage and testing
def main():
    """
    Main function demonstrating the user count aggregation implementation.
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("UserCountAggregation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    # Database connection properties
    kaksha_db_props = {
        "url": "jdbc:postgresql://localhost:5432/kaksha_db",
        "user": "postgres",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    
    # Initialize aggregator
    aggregator = UserCountAggregator(spark, kaksha_db_props)
    
    try:
        # Generate all reports
        reports = aggregator.generate_user_count_reports()
        
        # Print summaries
        aggregator.print_report_summaries(reports)
        
        # Export to database (optional)
        # aggregator.export_reports_to_database(reports)
        
        print("User count aggregation completed successfully!")
        
    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    main()
