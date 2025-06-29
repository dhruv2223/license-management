from pyspark.sql.functions import col, lit, current_date, current_timestamp, expr, udf, row_number # <-- Added row_number import
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DateType, TimestampType
from uuid import uuid4
import random
import psycopg2
from datetime import datetime

class PendingLicenseRequestProcessor:
    def __init__(self, spark: SparkSession, client_db_config: dict, kaksha_db_config: dict):
        self.spark = spark
        self.client_db_config = client_db_config
        self.kaksha_db_config = kaksha_db_config
        # It's good practice to define the number of partitions for operations
        self.num_partitions = spark.sparkContext.defaultParallelism * 2 

    def read_table(self, db_config, table_name, partition_column=None, lower_bound=None, upper_bound=None, num_partitions=10):
        """
        Refactored read_table to support JDBC partitioning for parallel reads.
        - partition_column: A numeric column to partition the read by (e.g., 'id').
        - lower_bound/upper_bound: Min and max values of the partition_column to read.
        - num_partitions: The number of parallel connections to the database.
        """
        reader = self.spark.read \
            .format("jdbc") \
            .option("url", db_config["url"]) \
            .option("dbtable", table_name) \
            .option("user", db_config["user"]) \
            .option("password", db_config["password"]) \
            .option("driver", db_config["driver"])

        # *** PERFORMANCE IMPROVEMENT: Parallel JDBC Read ***
        if all([partition_column, lower_bound is not None, upper_bound is not None]):
            print(f"Reading {table_name} in parallel on column '{partition_column}'")
            reader = reader.option("partitionColumn", partition_column) \
                           .option("lowerBound", lower_bound) \
                           .option("upperBound", upper_bound) \
                           .option("numPartitions", num_partitions)
        
        return reader.load()

    def read_pending_requests(self):
        # For this to be effective, you'd need min/max IDs of PENDING requests.
        # A subquery or a separate query can get these bounds. For simplicity, we assume they are known or we skip partitioning here.
        # For example: SELECT min(id), max(id) FROM user_licence_requests WHERE request_status = 'PENDING'
        df = self.read_table(self.client_db_config, "user_licence_requests") # Partitioning could be added here
        df.createOrReplaceTempView("user_licence_requests")
        
        return self.spark.sql("""
            SELECT id, userid, license_type, product_name, requested_at
            FROM user_licence_requests
            WHERE request_status = 'PENDING'
        """)

    def read_kaksha_users(self):
        # Assuming 'userid' is a numeric primary key we can partition on.
        # In a real scenario, you'd query for min/max user IDs first.
        # For demonstration, let's assume we read without partitioning if bounds are unknown.
        df = self.read_table(self.kaksha_db_config, "kaksha_users")
        df.createOrReplaceTempView("kaksha_users")
        
        return self.spark.sql("""
            SELECT userid
            FROM kaksha_users
        """)

    def read_existing_active_licenses(self):
        df = self.read_table(self.kaksha_db_config, "kaksha_licenses") # Partitioning could be added here
        df.createOrReplaceTempView("kaksha_licenses")
        
        return self.spark.sql("""
            SELECT userid, license_type, product_name
            FROM kaksha_licenses
            WHERE status = 'ACTIVE' 
            AND expiry_date >= current_date()
        """)

    def generate_uuid(self):
        return str(uuid4())

    def generate_new_licenses(self, valid_requests_df):
        self.spark.udf.register("generate_uuid", lambda: str(uuid4()), StringType())
        
        valid_requests_df.createOrReplaceTempView("valid_requests")
        
        return self.spark.sql("""
            SELECT 
                generate_uuid() as license_id,
                userid,
                license_type,
                current_date() as start_date,
                CASE 
                    WHEN license_type = 'Type 1' THEN DATE_ADD(current_date(), 30)
                    WHEN license_type = 'Type 2' THEN DATE_ADD(current_date(), 90)
                    WHEN license_type = 'Type 3' THEN DATE_ADD(current_date(), 365)
                    ELSE DATE_ADD(current_date(), 30)
                END as expiry_date,
                product_name,
                'ACTIVE' as status,
                current_timestamp() as created_at
            FROM valid_requests
        """)

    def write_licenses_to_kaksha(self, licenses_df):
        """
        *** PERFORMANCE IMPROVEMENT: Parallel JDBC Write ***
        Replaced .collect() and psycopg2 loop with Spark's native JDBC writer.
        This writes in parallel from executors directly to the database.
        """
        print(f"Writing {licenses_df.count()} new licenses to kaksha_licenses table...")
        
        # Coalesce to control the number of connections to the database
        licenses_df.coalesce(self.num_partitions // 2).write \
            .format("jdbc") \
            .option("url", self.kaksha_db_config["url"]) \
            .option("dbtable", "kaksha_licenses") \
            .option("user", self.kaksha_db_config["user"]) \
            .option("password", self.kaksha_db_config["password"]) \
            .option("driver", self.kaksha_db_config["driver"]) \
            .mode("append") \
            .save()
        print(f"✅ Successfully inserted licenses")


    def update_request_statuses(self, approved_df, rejected_df):
        """
        *** PERFORMANCE IMPROVEMENT: Switched to a more scalable update pattern ***
        Instead of collecting IDs to the driver, this approach writes IDs to a temp table
        and then executes a single UPDATE FROM statement. This is much more scalable.
        """
        approved_ids_df = approved_df.select("id").withColumn("status", lit("APPROVED"))
        rejected_ids_df = rejected_df.select("id").withColumn("status", lit("REJECTED"))

        updates_df = approved_ids_df.unionByName(rejected_ids_df)
        
        if updates_df.rdd.isEmpty():
            return
            
        updates_df.createOrReplaceTempView("status_updates")
        
        # Write updates to a temporary table in the target database
        temp_update_table = f"temp_updates_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        updates_df.write \
            .format("jdbc") \
            .option("url", self.client_db_config["url"]) \
            .option("dbtable", temp_update_table) \
            .option("user", self.client_db_config["user"]) \
            .option("password", self.client_db_config["password"]) \
            .mode("overwrite") \
            .save()
            
        # Execute a single UPDATE command using the temp table
        conn = psycopg2.connect(**self.client_db_config)
        cursor = conn.cursor()
        try:
            update_query = f"""
                UPDATE user_licence_requests
                SET 
                    request_status = source.status,
                    processed_at = CURRENT_TIMESTAMP
                FROM {temp_update_table} AS source
                WHERE user_licence_requests.id = source.id;
            """
            cursor.execute(update_query)
            
            # Drop the temporary table
            cursor.execute(f"DROP TABLE {temp_update_table};")
            
            conn.commit()
            print("✅ Successfully updated request statuses.")
        except Exception as e:
            conn.rollback()
            print(f"❌ Error updating statuses: {e}")
            raise e
        finally:
            cursor.close()
            conn.close()

    def process_license_requests(self, pending_requests_df, users_df, active_licenses_df):
        """Process license requests using a combination of DataFrame API and SQL"""

        # *** PERFORMANCE IMPROVEMENT: Proactive Repartitioning ***
        # Repartition data on the join key ('userid') to avoid shuffles in subsequent steps.
        pending_requests_df = pending_requests_df.repartition(self.num_partitions, "userid")
        users_df = users_df.repartition(self.num_partitions, "userid")
        active_licenses_df = active_licenses_df.repartition(self.num_partitions, "userid", "license_type", "product_name")

        # Create temp views for all DataFrames
        pending_requests_df.createOrReplaceTempView("pending_requests")
        users_df.createOrReplaceTempView("kaksha_users")
        active_licenses_df.createOrReplaceTempView("active_licenses")

        # 1. Find valid requests (users exist in kaksha_users)
        valid_requests_df = self.spark.sql("""
            SELECT p.id, p.userid, p.license_type, p.product_name, p.requested_at
            FROM pending_requests p
            INNER JOIN kaksha_users k ON p.userid = k.userid
        """)

        # 2. Use a Window Function to select the most recent request
        window_spec = Window.partitionBy("userid", "license_type", "product_name").orderBy(col("requested_at").desc())

        latest_valid_requests_df = valid_requests_df.withColumn("row_num", row_number().over(window_spec)) \
                                                    .filter(col("row_num") == 1) \
                                                    .drop("row_num")

        # Create a temp view for these latest valid requests
        latest_valid_requests_df.createOrReplaceTempView("latest_valid_requests")

        # 3. Filter out requests where user already has an active license
        deduped_requests_df = self.spark.sql("""
            SELECT v.id, v.userid, v.license_type, v.product_name, v.requested_at
            FROM latest_valid_requests v
            LEFT ANTI JOIN active_licenses a
              ON v.userid = a.userid
              AND v.license_type = a.license_type
              AND v.product_name = a.product_name
        """) # Using LEFT ANTI JOIN is often more performant than LEFT JOIN + WHERE IS NULL

        # 4. Find rejected requests (all pending requests that are not in the final approved list)
        deduped_requests_df.createOrReplaceTempView("deduped_requests")
        rejected_requests_df = self.spark.sql("""
            SELECT p.id, p.userid, p.license_type, p.product_name, p.requested_at
            FROM pending_requests p
            LEFT ANTI JOIN deduped_requests d ON p.id = d.id
        """)

        return deduped_requests_df, rejected_requests_df
    
    # ... (get_license_counts_by_type and run methods remain largely the same, but will benefit from the partitioned reads/writes)
    
    def get_license_counts_by_type(self):
        """Get count of users by license_type using SQL"""
        df = self.read_table(self.kaksha_db_config, "kaksha_licenses")
        df.createOrReplaceTempView("all_licenses")
        
        return self.spark.sql("""
            SELECT 
                license_type,
                COUNT(DISTINCT userid) as user_count,
                COUNT(*) as total_licenses
            FROM all_licenses
            WHERE status = 'ACTIVE'
            GROUP BY license_type
            ORDER BY license_type
        """)

    def run(self):
        # Read all required data
        pending_requests_df = self.read_pending_requests()
        
        if pending_requests_df.rdd.isEmpty():
            print("✅ No pending requests found.")
            return

        # Cache the pending requests DataFrame as it's used to calculate both approved and rejected requests
        pending_requests_df.cache()
        print(f"Found {pending_requests_df.count()} pending requests to process.")

        users_df = self.read_kaksha_users()
        active_licenses_df = self.read_existing_active_licenses()

        # Process requests using SQL-based logic
        approved_requests_df, rejected_requests_df = self.process_license_requests(
            pending_requests_df, users_df, active_licenses_df
        )

        # Cache the results to avoid re-computation
        approved_requests_df.cache()
        rejected_requests_df.cache()
        
        approved_count = approved_requests_df.count()
        rejected_count = rejected_requests_df.count()

        # Generate new licenses for approved requests
        if approved_count > 0:
            new_licenses_df = self.generate_new_licenses(approved_requests_df)
            self.write_licenses_to_kaksha(new_licenses_df)
        
        # Update request statuses
        self.update_request_statuses(approved_requests_df, rejected_requests_df)

        print(f"✅ Processed {approved_count} license requests")
        print(f"❌ Rejected {rejected_count} license requests")
        
        # Show license counts by type
        print("\n📊 Current License Distribution:")
        license_counts_df = self.get_license_counts_by_type()
        license_counts_df.show()

        # Unpersist cached DataFrames
        pending_requests_df.unpersist()
        approved_requests_df.unpersist()
        rejected_requests_df.unpersist()
