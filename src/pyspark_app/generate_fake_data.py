from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, rand, udf
from pyspark.sql.types import *
from faker import Faker
import random
import uuid
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

fake = Faker()

# -----------------------------
# 1. Spark session
# -----------------------------
from config.spark_config import SparkConfigManager
spark = SparkConfigManager.create_spark_session("FakeDataGenerator")

# -----------------------------
# 2. Generate fake kaksha_users
# -----------------------------
def generate_users(num_users: int):
    data = [
        (
            i,
            fake.name(),
            fake.email(),
            fake.date_time_this_year(),
            random.choice([True, False])
        )
        for i in range(1, num_users + 1)
    ]
    
    schema = StructType([
        StructField("userid", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("email", StringType(), True),
        StructField("lastlogin", TimestampType(), True),
        StructField("islicenseactive", BooleanType(), True)
    ])
    
    return spark.createDataFrame(data, schema)

users_df = generate_users(100_000)
users_df.write.mode("overwrite").parquet("data/fake_kaksha_users")

# -----------------------------
# 3. Generate user_licence_requests
# -----------------------------
license_requests_df = users_df.select(
    col("userid"),
    expr("'pending' as request_status")
).withColumn("id", expr("monotonically_increasing_id()"))

license_requests_df.write.mode("overwrite").parquet("data/fake_user_license_requests")

# -----------------------------
# 4. Generate kaksha_licenses
# -----------------------------

license_types = ['Type 1', 'Type 2', 'Type 3']
products = ['Math101', 'SciMaster', 'CodeCamp']

def generate_license(row):
    return (
        str(uuid.uuid4()),
        row['userid'],
        random.choice(license_types),
        fake.date_between(start_date='-1y', end_date='today'),
        fake.date_between(start_date='today', end_date='+1y'),
        random.choice(products),
        random.choice(['active', 'expired'])
    )

licenses_data = users_df.rdd.map(generate_license)

license_schema = StructType([
    StructField("license_id", StringType(), False),
    StructField("userid", IntegerType(), False),
    StructField("license_type", StringType(), False),
    StructField("start_date", DateType(), False),
    StructField("expiry_date", DateType(), False),
    StructField("product_name", StringType(), False),
    StructField("status", StringType(), False),
])

licenses_df = spark.createDataFrame(licenses_data, schema=license_schema)
licenses_df.write.mode("overwrite").parquet("data/fake_kaksha_licenses")
