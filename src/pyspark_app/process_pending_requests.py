import sys, os
import psycopg2.extras
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import psycopg2
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType, DateType
from uuid import uuid4
from datetime import datetime, timedelta
from pyspark.sql.functions import expr
from config.spark_config import SparkConfigManager
import random

def mark_requests_processed(user_ids):
    conn = psycopg2.connect(
        dbname="client_db",
        user="postgres",
        password="Cdhruv@1234", 
        host="localhost",
        port=5432
    )
    cursor = conn.cursor()
    query = f"""
    UPDATE user_licence_requests
    SET request_status = 'processed'
    WHERE userid IN %s
    """
    cursor.execute(query, (tuple(user_ids),))


    conn.commit()
    cursor.close()
    conn.close()


spark = SparkConfigManager.create_spark_session("ProcessPendingRequests")
kaksha_db_props = {
    "url": "jdbc:postgresql://localhost:5432/kaksha_db",
    "user": "postgres",
    "password": "Cdhruv@1234",  
    "driver": "org.postgresql.Driver"
}
# Load requests
requests_df = spark.read.parquet("data/fake_user_license_requests").filter(col("request_status") == "pending")

# Assign fake license info
def generate_uuid():
    return str(uuid4())

def random_license_type():
    return random.choice(["Type 1", "Type 2", "Type 3"])

def random_product():
    return random.choice(["Math101", "CodeCamp", "SciMaster"])

def today():
    return datetime.now().date()

def expiry():
    return (datetime.now() + timedelta(days=random.randint(100, 365))).date()

uuid_udf = udf(generate_uuid, StringType())
type_udf = udf(random_license_type, StringType())
product_udf = udf(random_product, StringType())
start_udf = udf(today, DateType())
expiry_udf = udf(expiry, DateType())



licenses_df = requests_df.select(
    uuid_udf().alias("license_id"),
    col("userid"),
    type_udf().alias("license_type"),
    start_udf().alias("start_date"),
    expiry_udf().alias("expiry_date"),
    product_udf().alias("product_name"),
    lit("active").alias("status")
)



# Preview
licenses_df.show(5)

# Save to Parquet (later you’ll write to DB)
licenses_df.write.mode("overwrite").parquet("data/processed_kaksha_licenses")

licenses_df.write \
    .jdbc(
        url=kaksha_db_props["url"],
        table="kaksha_licenses",
        mode="append",
        properties=kaksha_db_props
    )

# Optional: Mark requests as processed
processed_requests_df = requests_df.withColumn("request_status", lit("processed"))
processed_requests_df.write.mode("overwrite").parquet("data/processed_user_requests")
user_ids = [row["userid"] for row in processed_requests_df.select("userid").distinct().collect()]
mark_requests_processed(user_ids)