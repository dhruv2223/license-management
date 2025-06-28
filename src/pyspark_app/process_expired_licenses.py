import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType, DateType
from uuid import uuid4
from datetime import datetime, timedelta
from config.spark_config import SparkConfigManager
import psycopg2
import random

# Spark session
spark = SparkConfigManager.create_spark_session("UpdateExpiredLicenses")

# PostgreSQL connection properties
kaksha_db_props = {
    "url": "jdbc:postgresql://localhost:5432/kaksha_db",
    "user": "postgres",
    "password": "your_password",
    "driver": "org.postgresql.Driver"
}

# Load current licenses
licenses_df = spark.read \
    .format("jdbc") \
    .option("url", kaksha_db_props["url"]) \
    .option("dbtable", "kaksha_licenses") \
    .option("user", kaksha_db_props["user"]) \
    .option("password", kaksha_db_props["password"]) \
    .option("driver", kaksha_db_props["driver"]) \
    .load()

# Filter expired licenses
expired_df = licenses_df.filter(col("expiry_date") < lit(datetime.now().date()))

# Delete expired licenses from DB
def delete_expired_from_db(license_ids):
    if not license_ids:
        return
    conn = psycopg2.connect(
        dbname="kaksha_db",
        user="postgres",
        password="your_password",
        host="localhost",
        port=5432
    )
    cursor = conn.cursor()
    delete_query = "DELETE FROM kaksha_licenses WHERE license_id = ANY(%s)"
    cursor.execute(delete_query, (license_ids,))
    conn.commit()
    cursor.close()
    conn.close()

license_ids = [row["license_id"] for row in expired_df.select("license_id").distinct().collect()]
delete_expired_from_db(license_ids)

# Generate refreshed license rows
def random_license_type():
    return random.choice(["Type 1", "Type 2", "Type 3"])

def random_product():
    return random.choice(["Math101", "CodeCamp", "SciMaster"])

def today():
    return datetime.now().date()

def expiry():
    return (datetime.now() + timedelta(days=random.randint(90, 365))).date()

uuid_udf = udf(lambda: str(uuid4()), StringType())
type_udf = udf(random_license_type, StringType())
product_udf = udf(random_product, StringType())
start_udf = udf(today, DateType())
expiry_udf = udf(expiry, DateType())

refreshed_df = expired_df.select(
    uuid_udf().alias("license_id"),
    col("userid"),
    type_udf().alias("license_type"),
    start_udf().alias("start_date"),
    expiry_udf().alias("expiry_date"),
    product_udf().alias("product_name"),
    lit("active").alias("status")
)

# Append refreshed licenses to DB
refreshed_df.write \
    .jdbc(
        url=kaksha_db_props["url"],
        table="kaksha_licenses",
        mode="append",
        properties=kaksha_db_props
    )

print(f"Replaced {len(license_ids)} expired licenses successfully.")

