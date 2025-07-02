import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date
import logging


class LicenseStatusUpdater:
    """
    Handles license expiration and user status updates using Spark SQL.
    
    Implements the complete flow:
    kaksha_licenses (check expiry_date) → Update status to 'EXPIRED' → Update kaksha_users.islicenseactive
    """
    
    def __init__(self, spark: SparkSession, kaksha_db_config: dict):
        self.spark = spark
        self.db_config = kaksha_db_config
        self.logger = logging.getLogger(__name__)
        
        # JDBC URL for Spark operations
        self.jdbc_url = f"jdbc:postgresql://{kaksha_db_config['host']}:{kaksha_db_config['port']}/{kaksha_db_config['dbname']}"
        self.connection_properties = {
            "user": kaksha_db_config["user"],
            "password": kaksha_db_config["password"],
            "driver": "org.postgresql.Driver"
        }
    
    def _get_db_connection(self):
        """Create and return a database connection."""
        try:
            return psycopg2.connect(
                dbname=self.db_config["dbname"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                host=self.db_config["host"],
                port=self.db_config["port"]
            )
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            raise
    
    def _load_tables_to_temp_views(self):
        """Load database tables into Spark temp views for SQL operations"""
        try:
            # Load kaksha_licenses table
            licenses_df = self.spark.read.jdbc(
                self.jdbc_url, 
                "kaksha_licenses", 
                properties=self.connection_properties
            )
            licenses_df.createOrReplaceTempView("kaksha_licenses")
            
            # Load kaksha_users table
            users_df = self.spark.read.jdbc(
                self.jdbc_url, 
                "kaksha_users", 
                properties=self.connection_properties
            )
            users_df.createOrReplaceTempView("kaksha_users")
            
            self.logger.info("Tables loaded into Spark temp views")
            
        except Exception as e:
            self.logger.error(f"Error loading tables to temp views: {e}")
            raise
    
    def identify_expired_licenses(self):
        """Step 1: Identify licenses that need to be expired"""
        self.logger.info("Step 1: Identifying expired licenses...")
        
        try:
            self._load_tables_to_temp_views()
            
            # Find expired licenses
            expired_licenses_df = self.spark.sql("""
                SELECT license_id
                FROM kaksha_licenses 
                WHERE status = 'ACTIVE' 
                AND expiry_date < current_date()
            """)
            
            license_ids = [str(row['license_id']) for row in expired_licenses_df.collect()]
            
            self.logger.info(f"Found {len(license_ids)} expired licenses")
            return license_ids
            
        except Exception as e:
            self.logger.error(f"Error identifying expired licenses: {e}")
            raise
    
    def expire_licenses(self, license_ids):
        """Step 2: Update license status from 'ACTIVE' to 'EXPIRED'"""
        self.logger.info("Step 2: Updating license status to EXPIRED...")
        
        if not license_ids:
            self.logger.info("No licenses to expire")
            return 0
        
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            try:
                cursor.execute("""
                    UPDATE kaksha_licenses 
                    SET status = 'EXPIRED', 
                        updated_at = CURRENT_TIMESTAMP
                    WHERE license_id = ANY(%s::uuid[])
                """, (license_ids,))
                
                updated_count = cursor.rowcount
                conn.commit()
                
                self.logger.info(f"Expired {updated_count} licenses")
                return updated_count
                
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Error in bulk update: {e}")
                raise
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            self.logger.error(f"Error expiring licenses: {e}")
            raise
    
    def update_user_status(self):
        """Step 3: Update kaksha_users.islicenseactive based on remaining active licenses"""
        self.logger.info("Step 3: Updating user license active status...")
        
        try:
            # Refresh temp views to get updated data
            self._load_tables_to_temp_views()
            
            # Get users whose status needs to be updated
            users_to_update_df = self.spark.sql("""
                SELECT 
                    u.userid,
                    CASE 
                        WHEN COUNT(CASE WHEN l.status = 'ACTIVE' AND l.expiry_date >= current_date() THEN 1 END) > 0 THEN true
                        ELSE false
                    END as should_be_active
                FROM kaksha_users u
                LEFT JOIN kaksha_licenses l ON u.userid = l.userid
                GROUP BY u.userid, u.islicenseactive
                HAVING u.islicenseactive != CASE 
                    WHEN COUNT(CASE WHEN l.status = 'ACTIVE' AND l.expiry_date >= current_date() THEN 1 END) > 0 THEN true
                    ELSE false
                END
            """)
            
            users_to_update = users_to_update_df.collect()
            
            if not users_to_update:
                self.logger.info("No user status updates needed")
                return 0
            
            # Prepare bulk update data
            user_updates = [(row['should_be_active'], row['userid']) for row in users_to_update]
            
            # Perform bulk update
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            try:
                cursor.executemany("""
                    UPDATE kaksha_users 
                    SET islicenseactive = %s, 
                        updated_at = CURRENT_TIMESTAMP
                    WHERE userid = %s
                """, user_updates)
                
                updated_count = cursor.rowcount
                conn.commit()
                
                self.logger.info(f"Updated islicenseactive status for {updated_count} users")
                return updated_count
                
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Error updating user status: {e}")
                raise
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            self.logger.error(f"Error in user status update: {e}")
            raise
    
    def run(self):
        """Main execution method"""
        self.logger.info("Starting license expiration process...")
        
        try:
            # Step 1: Find expired licenses
            license_ids = self.identify_expired_licenses()
            
            # Step 2: Expire licenses
            licenses_expired = self.expire_licenses(license_ids)
            
            # Step 3: Update user status
            users_updated = self.update_user_status()
            
            self.logger.info(f"Process completed: {licenses_expired} licenses expired, {users_updated} users updated")
            
            return {
                'licenses_expired': licenses_expired,
                'users_updated': users_updated,
                'status': 'SUCCESS'
            }
            
        except Exception as e:
            self.logger.error(f"License expiration process failed: {e}")
            return {
                'status': 'FAILED',
                'error': str(e)
            }
