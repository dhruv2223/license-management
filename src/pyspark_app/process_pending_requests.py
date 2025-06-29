from pyspark.sql.functions import col, lit, current_date, current_timestamp, expr, udf
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

    def read_table(self, db_config, table_name):
        return self.spark.read \
            .format("jdbc") \
            .option("url", db_config["url"]) \
            .option("dbtable", table_name) \
            .option("user", db_config["user"]) \
            .option("password", db_config["password"]) \
            .option("driver", db_config["driver"]) \
            .load()

    def read_pending_requests(self):
        df = self.read_table(self.client_db_config, "user_licence_requests")
        df.createOrReplaceTempView("user_licence_requests")
        
        return self.spark.sql("""
            SELECT id, userid, license_type, product_name, requested_at
            FROM user_licence_requests
            WHERE request_status = 'PENDING'
        """)

    def read_kaksha_users(self):
        df = self.read_table(self.kaksha_db_config, "kaksha_users")
        df.createOrReplaceTempView("kaksha_users")
        
        return self.spark.sql("""
            SELECT userid
            FROM kaksha_users
        """)

    def read_existing_active_licenses(self):
        df = self.read_table(self.kaksha_db_config, "kaksha_licenses")
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
        # Register UDF for UUID generation
        self.spark.udf.register("generate_uuid", lambda: str(uuid4()), StringType())
        
        # Create temp view for the valid requests
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
        # Convert DataFrame to list of tuples for direct SQL insert with UUID casting
        license_data = licenses_df.collect()
        
        # Extract database connection details from URL if needed
        url = self.kaksha_db_config["url"]
        if "dbname" not in self.kaksha_db_config:
            # Extract dbname from URL like jdbc:postgresql://host:port/dbname
            dbname = url.split("/")[-1].split("?")[0]
        else:
            dbname = self.kaksha_db_config["dbname"]
            
        if "host" not in self.kaksha_db_config:
            # Extract host from URL
            host = url.split("//")[1].split(":")[0]
        else:
            host = self.kaksha_db_config["host"]
            
        if "port" not in self.kaksha_db_config:
            # Extract port from URL or default to 5432
            try:
                port = int(url.split("//")[1].split(":")[1].split("/")[0])
            except:
                port = 5432
        else:
            port = self.kaksha_db_config["port"]
        
        conn = psycopg2.connect(
            dbname=dbname,
            user=self.kaksha_db_config["user"],
            password=self.kaksha_db_config["password"],
            host=host,
            port=port
        )
        cursor = conn.cursor()
        
        try:
            # Insert each record with explicit UUID casting
            for row in license_data:
                cursor.execute("""
                    INSERT INTO kaksha_licenses (license_id, userid, license_type, start_date, expiry_date, product_name, status, created_at)
                    VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row.license_id, 
                    row.userid, 
                    row.license_type, 
                    row.start_date, 
                    row.expiry_date, 
                    row.product_name, 
                    row.status, 
                    row.created_at
                ))
            
            conn.commit()
            print(f"✅ Successfully inserted {len(license_data)} licenses")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error inserting licenses: {e}")
            raise e
        finally:
            cursor.close()
            conn.close()

    def update_request_statuses(self, approved_df, rejected_df):
        # Use SQL to get the IDs instead of DataFrame operations
        approved_df.createOrReplaceTempView("approved_requests")
        rejected_df.createOrReplaceTempView("rejected_requests")
        
        approved_ids_df = self.spark.sql("SELECT id FROM approved_requests")
        rejected_ids_df = self.spark.sql("SELECT id FROM rejected_requests")
        
        approved_ids = [row["id"] for row in approved_ids_df.collect()]
        rejected_ids = [row["id"] for row in rejected_ids_df.collect()]

        conn = psycopg2.connect(
            dbname=self.client_db_config["dbname"],
            user=self.client_db_config["user"],
            password=self.client_db_config["password"],
            host=self.client_db_config["host"],
            port=self.client_db_config["port"]
        )
        cursor = conn.cursor()

        if approved_ids:
            cursor.execute(
                "UPDATE user_licence_requests SET request_status = 'APPROVED', processed_at = CURRENT_TIMESTAMP WHERE id IN %s",
                (tuple(approved_ids),)
            )

        if rejected_ids:
            cursor.execute(
                "UPDATE user_licence_requests SET request_status = 'REJECTED', processed_at = CURRENT_TIMESTAMP WHERE id IN %s",
                (tuple(rejected_ids),)
            )

        conn.commit()
        cursor.close()
        conn.close()

    def process_license_requests(self, pending_requests_df, users_df, active_licenses_df):
        """Process license requests using SQL joins and filtering"""
        
        # Create temp views for all DataFrames
        pending_requests_df.createOrReplaceTempView("pending_requests")
        users_df.createOrReplaceTempView("kaksha_users") 
        active_licenses_df.createOrReplaceTempView("active_licenses")
        
        # Find valid requests (users exist in kaksha_users)
        valid_requests_df = self.spark.sql("""
            SELECT p.id, p.userid, p.license_type, p.product_name, p.requested_at
            FROM pending_requests p
            INNER JOIN kaksha_users k ON p.userid = k.userid
        """)
        
        # Create temp view for valid requests
        valid_requests_df.createOrReplaceTempView("valid_requests")
        
        # Filter out requests where user already has active license for same type and product
        deduped_requests_df = self.spark.sql("""
            SELECT v.id, v.userid, v.license_type, v.product_name, v.requested_at
            FROM valid_requests v
            LEFT JOIN active_licenses a 
                ON v.userid = a.userid 
                AND v.license_type = a.license_type 
                AND v.product_name = a.product_name
            WHERE a.userid IS NULL
        """)
        
        # Find rejected requests (those not in deduped requests)
        deduped_requests_df.createOrReplaceTempView("deduped_requests")
        rejected_requests_df = self.spark.sql("""
            SELECT p.id, p.userid, p.license_type, p.product_name, p.requested_at
            FROM pending_requests p
            LEFT JOIN deduped_requests d ON p.id = d.id
            WHERE d.id IS NULL
        """)
        
        return deduped_requests_df, rejected_requests_df

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

        users_df = self.read_kaksha_users()
        active_licenses_df = self.read_existing_active_licenses()

        # Process requests using SQL-based logic
        approved_requests_df, rejected_requests_df = self.process_license_requests(
            pending_requests_df, users_df, active_licenses_df
        )

        # Generate new licenses for approved requests
        if not approved_requests_df.rdd.isEmpty():
            new_licenses_df = self.generate_new_licenses(approved_requests_df)
            self.write_licenses_to_kaksha(new_licenses_df)
        
        # Update request statuses
        self.update_request_statuses(approved_requests_df, rejected_requests_df)

        # Print summary with SQL counts
        approved_count = self.spark.sql("SELECT COUNT(*) as count FROM deduped_requests").collect()[0]["count"]
        rejected_count = self.spark.sql("SELECT COUNT(*) as count FROM rejected_requests").collect()[0]["count"] if not rejected_requests_df.rdd.isEmpty() else 0

        print(f"✅ Processed {approved_count} license requests")
        print(f"❌ Rejected {rejected_count} license requests")
        
        # Show license counts by type
        print("\n📊 Current License Distribution:")
        license_counts_df = self.get_license_counts_by_type()
        license_counts_df.show()
