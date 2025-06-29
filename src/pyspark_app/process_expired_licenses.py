import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, current_timestamp
from typing import Dict, Tuple, List
from datetime import datetime
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
            
            self.logger.info("✅ Tables loaded into Spark temp views")
            
        except Exception as e:
            self.logger.error(f"Error loading tables to temp views: {e}")
            raise
    
    def identify_expired_licenses_sql(self) -> Tuple[List[Dict], int]:
        """
        Step 1: Identify licenses that need to be expired using Spark SQL.
        
        Returns:
            Tuple of (license_details_list, count)
        """
        self.logger.info("Step 1: Identifying expired licenses using Spark SQL...")
        
        try:
            self._load_tables_to_temp_views()
            
            # Use Spark SQL to find expired licenses
            expired_licenses_df = self.spark.sql("""
                SELECT 
                    license_id, 
                    userid, 
                    license_type, 
                    product_name, 
                    expiry_date,
                    DATEDIFF(current_date(), expiry_date) as days_overdue
                FROM kaksha_licenses 
                WHERE status = 'ACTIVE' 
                AND expiry_date < current_date()
                ORDER BY expiry_date ASC
            """)
            
            # Collect results
            expired_licenses = expired_licenses_df.collect()
            
            # Convert to list of dictionaries
            license_details = []
            for row in expired_licenses:
                license_details.append({
                    'license_id': row['license_id'],
                    'userid': row['userid'],
                    'license_type': row['license_type'],
                    'product_name': row['product_name'],
                    'expiry_date': row['expiry_date'],
                    'days_overdue': row['days_overdue']
                })
            
            self.logger.info(f"Found {len(expired_licenses)} expired licenses")
            
            # Show summary by license type
            if expired_licenses:
                expired_summary_df = self.spark.sql("""
                    SELECT 
                        license_type,
                        COUNT(*) as expired_count,
                        MIN(expiry_date) as earliest_expiry,
                        MAX(expiry_date) as latest_expiry,
                        AVG(DATEDIFF(current_date(), expiry_date)) as avg_days_overdue
                    FROM kaksha_licenses 
                    WHERE status = 'ACTIVE' 
                    AND expiry_date < current_date()
                    GROUP BY license_type
                    ORDER BY expired_count DESC
                """)
                
                self.logger.info("Expired licenses summary by type:")
                expired_summary_df.show(truncate=False)
            
            return license_details, len(expired_licenses)
            
        except Exception as e:
            self.logger.error(f"Error identifying expired licenses: {e}")
            raise
    
    def get_users_affected_by_expiry_sql(self) -> List[Dict]:
        """
        Get detailed information about users affected by license expiry using SQL.
        
        Returns:
            List of user details with their license status
        """
        self.logger.info("Analyzing users affected by license expiry...")
        
        try:
            affected_users_df = self.spark.sql("""
                SELECT 
                    u.userid,
                    u.name,
                    u.email,
                    u.islicenseactive as current_status,
                    COUNT(CASE WHEN l.status = 'ACTIVE' AND l.expiry_date >= current_date() THEN 1 END) as active_licenses,
                    COUNT(CASE WHEN l.status = 'ACTIVE' AND l.expiry_date < current_date() THEN 1 END) as expiring_licenses,
                    COUNT(CASE WHEN l.status = 'EXPIRED' THEN 1 END) as already_expired_licenses,
                    CASE 
                        WHEN COUNT(CASE WHEN l.status = 'ACTIVE' AND l.expiry_date >= current_date() THEN 1 END) > 0 THEN true
                        ELSE false
                    END as should_be_active
                FROM kaksha_users u
                LEFT JOIN kaksha_licenses l ON u.userid = l.userid
                WHERE u.userid IN (
                    SELECT DISTINCT userid 
                    FROM kaksha_licenses 
                    WHERE status = 'ACTIVE' AND expiry_date < current_date()
                )
                GROUP BY u.userid, u.name, u.email, u.islicenseactive
                ORDER BY expiring_licenses DESC, active_licenses DESC
            """)
            
            affected_users = affected_users_df.collect()
            
            user_details = []
            for row in affected_users:
                user_details.append({
                    'userid': row['userid'],
                    'name': row['name'],
                    'email': row['email'],
                    'current_status': row['current_status'],
                    'active_licenses': row['active_licenses'],
                    'expiring_licenses': row['expiring_licenses'],
                    'already_expired_licenses': row['already_expired_licenses'],
                    'should_be_active': row['should_be_active']
                })
            
            self.logger.info(f"Found {len(user_details)} users affected by license expiry")
            
            # Show summary
            affected_users_df.show(20, truncate=False)
            
            return user_details
            
        except Exception as e:
            self.logger.error(f"Error analyzing affected users: {e}")
            raise
    
    def expire_licenses_bulk_sql(self) -> Dict[str, int]:
        """
        Step 2: Update license status from 'ACTIVE' to 'EXPIRED' using bulk operations.
        
        Returns:
            Dictionary with count of expired licenses by type
        """
        self.logger.info("Step 2: Updating license status to EXPIRED using bulk operations...")
        
        try:
            # First, get the count by type before updating
            type_counts_df = self.spark.sql("""
                SELECT 
                    license_type,
                    COUNT(*) as count
                FROM kaksha_licenses 
                WHERE status = 'ACTIVE' 
                AND expiry_date < current_date()
                GROUP BY license_type
                ORDER BY license_type
            """)
            
            # Convert to dictionary
            type_counts = {row['license_type']: row['count'] for row in type_counts_df.collect()}
            
            # Get the license IDs to update
            licenses_to_expire_df = self.spark.sql("""
                SELECT license_id
                FROM kaksha_licenses 
                WHERE status = 'ACTIVE' 
                AND expiry_date < current_date()
            """)
            
            license_ids = [str(row['license_id']) for row in licenses_to_expire_df.collect()]
            
            if not license_ids:
                self.logger.info("No licenses to expire")
                return {}
            
            # Perform bulk update using psycopg2 for better performance
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            try:
                # Option 1: Use PostgreSQL's efficient bulk update with proper UUID casting
                cursor.execute("""
                    UPDATE kaksha_licenses 
                    SET status = 'EXPIRED', 
                        updated_at = CURRENT_TIMESTAMP
                    WHERE license_id = ANY(%s::uuid[])
                """, (license_ids,))
                
                # Alternative Option 2: If the above doesn't work, use this approach
                # cursor.execute("""
                #     UPDATE kaksha_licenses 
                #     SET status = 'EXPIRED', 
                #         updated_at = CURRENT_TIMESTAMP
                #     WHERE license_id::text = ANY(%s)
                # """, (license_ids,))
                
                # Alternative Option 3: Use executemany for individual updates (slower but more reliable)
                # cursor.executemany("""
                #     UPDATE kaksha_licenses 
                #     SET status = 'EXPIRED', 
                #         updated_at = CURRENT_TIMESTAMP
                #     WHERE license_id = %s::uuid
                # """, [(license_id,) for license_id in license_ids])
                
                updated_count = cursor.rowcount
                conn.commit()
                
                self.logger.info(f"✅ Expired {updated_count} licenses")
                
                for license_type, count in type_counts.items():
                    self.logger.info(f"  - {license_type}: {count}")
                
                return type_counts
                
            except Exception as e:
                conn.rollback()
                self.logger.error(f"❌ Error in bulk update: {e}")
                raise
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            self.logger.error(f"❌ Error expiring licenses: {e}")
            raise
    
    def update_user_license_status_sql(self) -> int:
        """
        Step 3: Update kaksha_users.islicenseactive using SQL analysis.
        
        Returns:
            Number of users whose status was updated
        """
        self.logger.info("Step 3: Updating user license active status using SQL...")
        
        try:
            # Refresh the temp views to get updated data
            self._load_tables_to_temp_views()
            
            # Get users whose status needs to be updated
            users_to_update_df = self.spark.sql("""
                SELECT 
                    u.userid,
                    u.islicenseactive as current_status,
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
            user_updates = []
            for row in users_to_update:
                user_updates.append((row['should_be_active'], row['userid']))
            
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
                
                self.logger.info(f"🔄 Updated islicenseactive status for {updated_count} users")
                
                # Show summary of changes
                changes_summary_df = self.spark.sql("""
                    SELECT 
                        should_be_active as new_status,
                        COUNT(*) as user_count
                    FROM (
                        SELECT 
                            u.userid,
                            CASE 
                                WHEN COUNT(CASE WHEN l.status = 'ACTIVE' AND l.expiry_date >= current_date() THEN 1 END) > 0 THEN true
                                ELSE false
                            END as should_be_active
                        FROM kaksha_users u
                        LEFT JOIN kaksha_licenses l ON u.userid = l.userid
                        WHERE u.userid IN ({})
                        GROUP BY u.userid
                    ) changes
                    GROUP BY should_be_active
                    ORDER BY new_status DESC
                """.format(','.join([f"'{update[1]}'" for update in user_updates])))
                
                self.logger.info("User status changes summary:")
                changes_summary_df.show()
                
                return updated_count
                
            except Exception as e:
                conn.rollback()
                self.logger.error(f"❌ Error updating user status: {e}")
                raise
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            self.logger.error(f"❌ Error in user status update: {e}")
            raise
    
    def generate_comprehensive_report_sql(self, type_counts: Dict[str, int], users_updated: int, 
                                        start_time: datetime) -> Dict:
        """
        Generate comprehensive report using SQL analytics.
        
        Args:
            type_counts: Dictionary of license types and their expired counts
            users_updated: Number of users whose status was updated
            start_time: When the process started
            
        Returns:
            Comprehensive summary report dictionary
        """
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        try:
            # Refresh temp views for latest data
            self._load_tables_to_temp_views()
            
            # Get current license distribution
            current_distribution_df = self.spark.sql("""
                SELECT 
                    status,
                    license_type,
                    COUNT(*) as count,
                    COUNT(DISTINCT userid) as unique_users
                FROM kaksha_licenses
                GROUP BY status, license_type
                ORDER BY status, license_type
            """)
            
            # Get user status distribution
            user_status_df = self.spark.sql("""
                SELECT 
                    islicenseactive,
                    COUNT(*) as user_count
                FROM kaksha_users
                GROUP BY islicenseactive
                ORDER BY islicenseactive DESC
            """)
            
            # Get recently expired licenses (last 7 days)
            recent_expiry_trends_df = self.spark.sql("""
                SELECT 
                    DATE(updated_at) as expiry_date,
                    license_type,
                    COUNT(*) as expired_count
                FROM kaksha_licenses
                WHERE status = 'EXPIRED' 
                AND updated_at >= current_date() - INTERVAL '7 days'
                GROUP BY DATE(updated_at), license_type
                ORDER BY expiry_date DESC, license_type
            """)
            
            # Get upcoming expirations (next 30 days)
            upcoming_expirations_df = self.spark.sql("""
                SELECT 
                    license_type,
                    COUNT(*) as expiring_soon_count,
                    MIN(expiry_date) as earliest_expiry,
                    MAX(expiry_date) as latest_expiry
                FROM kaksha_licenses
                WHERE status = 'ACTIVE'
                AND expiry_date BETWEEN current_date() AND current_date() + INTERVAL '30 days'
                GROUP BY license_type
                ORDER BY expiring_soon_count DESC
            """)
            
            total_licenses_expired = sum(type_counts.values())
            
            # Collect analytics data
            current_distribution = [row.asDict() for row in current_distribution_df.collect()]
            user_status_dist = [row.asDict() for row in user_status_df.collect()]
            recent_trends = [row.asDict() for row in recent_expiry_trends_df.collect()]
            upcoming_expirations = [row.asDict() for row in upcoming_expirations_df.collect()]
            
            summary = {
                'timestamp': end_time.isoformat(),
                'processing_time_seconds': processing_time,
                'execution_summary': {
                    'total_licenses_expired': total_licenses_expired,
                    'licenses_expired_by_type': type_counts,
                    'users_status_updated': users_updated,
                },
                'current_license_distribution': current_distribution,
                'user_status_distribution': user_status_dist,
                'recent_expiry_trends': recent_trends,
                'upcoming_expirations_30_days': upcoming_expirations,
                'status': 'SUCCESS'
            }
            
            # Log comprehensive summary
            self.logger.info("=== COMPREHENSIVE LICENSE EXPIRATION REPORT ===")
            self.logger.info(f"Timestamp: {summary['timestamp']}")
            self.logger.info(f"Processing time: {processing_time:.2f} seconds")
            self.logger.info(f"Total licenses expired: {total_licenses_expired}")
            
            if type_counts:
                self.logger.info("Licenses expired by type:")
                for license_type, count in type_counts.items():
                    self.logger.info(f"  - {license_type}: {count}")
            
            self.logger.info(f"Users affected (islicenseactive updated): {users_updated}")
            
            # Show current distribution
            self.logger.info("\nCurrent License Distribution:")
            current_distribution_df.show()
            
            # Show user status distribution
            self.logger.info("User Status Distribution:")
            user_status_df.show()
            
            # Show upcoming expirations
            if upcoming_expirations:
                self.logger.info("Upcoming Expirations (Next 30 Days):")
                upcoming_expirations_df.show()
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error generating comprehensive report: {e}")
            # Return basic summary if detailed analytics fail
            return {
                'timestamp': end_time.isoformat(),
                'processing_time_seconds': processing_time,
                'total_licenses_expired': sum(type_counts.values()),
                'licenses_expired_by_type': type_counts,
                'users_status_updated': users_updated,
                'status': 'SUCCESS_WITH_REPORT_WARNINGS',
                'error': str(e)
            }
    
    def run_comprehensive_sql_analysis(self) -> Dict:
        """
        Run comprehensive license analysis using only Spark SQL operations.
        
        Returns:
            Complete analysis report
        """
        start_time = datetime.now()
        self.logger.info("⏳ Starting comprehensive license analysis using Spark SQL...")
        
        try:
            self._load_tables_to_temp_views()
            
            # Step 1: Analyze current state
            self.logger.info("📊 Analyzing current license state...")
            
            overall_stats_df = self.spark.sql("""
                SELECT 
                    COUNT(*) as total_licenses,
                    COUNT(DISTINCT userid) as total_users,
                    COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) as active_licenses,
                    COUNT(CASE WHEN status = 'EXPIRED' THEN 1 END) as expired_licenses,
                    COUNT(CASE WHEN status = 'ACTIVE' AND expiry_date < current_date() THEN 1 END) as need_expiry,
                    COUNT(CASE WHEN status = 'ACTIVE' AND expiry_date BETWEEN current_date() AND current_date() + INTERVAL '30 days' THEN 1 END) as expiring_soon
                FROM kaksha_licenses
            """)
            
            self.logger.info("Overall License Statistics:")
            overall_stats_df.show()
            
            # Step 2: Identify and analyze expired licenses
            expired_details, expired_count = self.identify_expired_licenses_sql()
            
            if expired_count == 0:
                self.logger.info("✅ No licenses need expiration")
                return {
                    'timestamp': datetime.now().isoformat(),
                    'total_licenses_expired': 0,
                    'users_status_updated': 0,
                    'overall_statistics': [row.asDict() for row in overall_stats_df.collect()][0],
                    'status': 'NO_ACTION_NEEDED'
                }
            
            # Step 3: Analyze affected users
            affected_users = self.get_users_affected_by_expiry_sql()
            
            # Step 4: Expire licenses
            type_counts = self.expire_licenses_bulk_sql()
            
            # Step 5: Update user status
            users_updated = self.update_user_license_status_sql()
            
            # Step 6: Generate comprehensive report
            summary = self.generate_comprehensive_report_sql(type_counts, users_updated, start_time)
            
            # Add additional analytics
            summary['affected_users_analysis'] = affected_users[:10]  # Top 10 affected users
            summary['overall_statistics'] = [row.asDict() for row in overall_stats_df.collect()][0]
            
            self.logger.info("✅ Comprehensive license analysis completed successfully")
            return summary
            
        except Exception as e:
            self.logger.error(f"❌ Comprehensive license analysis failed: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'status': 'FAILED',
                'error': str(e),
                'processing_time_seconds': (datetime.now() - start_time).total_seconds()
            }
    
    def use_database_function(self) -> int:
        """
        Alternative method: Use the existing expire_licenses() database function.
        
        Returns:
            Number of licenses expired
        """
        self.logger.info("Using database function expire_licenses()...")
        
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT expire_licenses() as expired_count")
            result = cursor.fetchone()
            expired_count = result[0] if result else 0
            
            self.logger.info(f"Database function expired {expired_count} licenses")
            return expired_count
            
        except Exception as e:
            self.logger.error(f"Error using database function: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def run(self, method: str = 'comprehensive_sql') -> Dict:
        """
        Main execution method with multiple processing options.
        
        Args:
            method: Processing method - 'comprehensive_sql', 'basic_sql', or 'database_function'
            
        Returns:
            Summary report dictionary
        """
        start_time = datetime.now()
        self.logger.info(f"⏳ Starting license expiration process using method: {method}")
        
        try:
            if method == 'database_function':
                # Method 1: Use existing database function
                expired_count = self.use_database_function()
                return {
                    'timestamp': datetime.now().isoformat(),
                    'processing_time_seconds': (datetime.now() - start_time).total_seconds(),
                    'total_licenses_expired': expired_count,
                    'method': 'database_function',
                    'status': 'SUCCESS'
                }
            
            elif method == 'comprehensive_sql':
                # Method 2: Comprehensive SQL analysis
                return self.run_comprehensive_sql_analysis()
            
            elif method == 'basic_sql':
                # Method 3: Basic SQL process
                expired_details, expired_count = self.identify_expired_licenses_sql()
                
                if expired_count == 0:
                    return {
                        'timestamp': datetime.now().isoformat(),
                        'total_licenses_expired': 0,
                        'users_status_updated': 0,
                        'status': 'NO_ACTION_NEEDED'
                    }
                
                type_counts = self.expire_licenses_bulk_sql()
                users_updated = self.update_user_license_status_sql()
                
                return self.generate_comprehensive_report_sql(type_counts, users_updated, start_time)
            
            else:
                raise ValueError(f"Unknown method: {method}")
                
        except Exception as e:
            self.logger.error(f"❌ License expiration process failed: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'status': 'FAILED',
                'error': str(e),
                'method': method,
                'processing_time_seconds': (datetime.now() - start_time).total_seconds()
            }
