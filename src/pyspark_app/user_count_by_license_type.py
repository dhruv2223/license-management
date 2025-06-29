from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct

class LicenseUserCounter:
    def __init__(self, spark: SparkSession, kaksha_db_config: dict):
        self.spark = spark
        self.kaksha_db_config = kaksha_db_config

    def read_table(self, db_config, table_name):
        """Read table from database using JDBC"""
        return self.spark.read \
            .format("jdbc") \
            .option("url", db_config["url"]) \
            .option("dbtable", table_name) \
            .option("user", db_config["user"]) \
            .option("password", db_config["password"]) \
            .option("driver", db_config["driver"]) \
            .load()

    def get_user_count_by_license_type(self, include_expired=False):
        """
        Get count of users by license_type
        
        Args:
            include_expired (bool): Whether to include expired licenses. Default is False (active only)
        
        Returns:
            DataFrame with license_type, unique_users, and total_licenses columns
        """
        # Read the kaksha_licenses table
        licenses_df = self.read_table(self.kaksha_db_config, "kaksha_licenses")
        
        # Create temp view for SQL queries
        licenses_df.createOrReplaceTempView("kaksha_licenses")
        
        # Build the WHERE clause based on whether to include expired licenses
        where_clause = "WHERE status = 'ACTIVE'"
        if not include_expired:
            where_clause += " AND expiry_date >= current_date()"
        
        # SQL query to get counts by license type
        query = f"""
            SELECT 
                license_type,
                COUNT(DISTINCT userid) as unique_users,
                COUNT(*) as total_licenses
            FROM kaksha_licenses
            {where_clause}
            GROUP BY license_type
            ORDER BY license_type
        """
        
        return self.spark.sql(query)

    def get_user_count_by_license_type_and_product(self, include_expired=False):
        """
        Get count of users by license_type and product_name
        
        Args:
            include_expired (bool): Whether to include expired licenses. Default is False (active only)
        
        Returns:
            DataFrame with license_type, product_name, unique_users, and total_licenses columns
        """
        licenses_df = self.read_table(self.kaksha_db_config, "kaksha_licenses")
        licenses_df.createOrReplaceTempView("kaksha_licenses")
        
        where_clause = "WHERE status = 'ACTIVE'"
        if not include_expired:
            where_clause += " AND expiry_date >= current_date()"
        
        query = f"""
            SELECT 
                license_type,
                product_name,
                COUNT(DISTINCT userid) as unique_users,
                COUNT(*) as total_licenses
            FROM kaksha_licenses
            {where_clause}
            GROUP BY license_type, product_name
            ORDER BY license_type, product_name
        """
        
        return self.spark.sql(query)

    def get_detailed_license_summary(self):
        """
        Get a comprehensive summary of license distribution
        
        Returns:
            Dictionary containing multiple DataFrames with different views of the data
        """
        licenses_df = self.read_table(self.kaksha_db_config, "kaksha_licenses")
        licenses_df.createOrReplaceTempView("kaksha_licenses")
        
        # Active licenses by type
        active_by_type = self.spark.sql("""
            SELECT 
                license_type,
                COUNT(DISTINCT userid) as unique_users,
                COUNT(*) as total_licenses
            FROM kaksha_licenses
            WHERE status = 'ACTIVE' AND expiry_date >= current_date()
            GROUP BY license_type
            ORDER BY license_type
        """)
        
        # All licenses by status
        by_status = self.spark.sql("""
            SELECT 
                status,
                COUNT(DISTINCT userid) as unique_users,
                COUNT(*) as total_licenses
            FROM kaksha_licenses
            GROUP BY status
            ORDER BY status
        """)
        
        # Expired vs Active breakdown
        expiry_status = self.spark.sql("""
            SELECT 
                license_type,
                CASE 
                    WHEN status = 'ACTIVE' AND expiry_date >= current_date() THEN 'Active'
                    WHEN status = 'ACTIVE' AND expiry_date < current_date() THEN 'Expired'
                    ELSE status
                END as license_status,
                COUNT(DISTINCT userid) as unique_users,
                COUNT(*) as total_licenses
            FROM kaksha_licenses
            GROUP BY license_type, 
                CASE 
                    WHEN status = 'ACTIVE' AND expiry_date >= current_date() THEN 'Active'
                    WHEN status = 'ACTIVE' AND expiry_date < current_date() THEN 'Expired'
                    ELSE status
                END
            ORDER BY license_type, license_status
        """)
        
        return {
            'active_by_type': active_by_type,
            'by_status': by_status,
            'expiry_breakdown': expiry_status
        }

    def print_license_summary(self, include_expired=False):
        """
        Print a formatted summary of license counts
        
        Args:
            include_expired (bool): Whether to include expired licenses in the summary
        """
        print("📊 License User Count Summary")
        print("=" * 50)
        
        # Get counts by license type
        counts_df = self.get_user_count_by_license_type(include_expired)
        
        if counts_df.rdd.isEmpty():
            print("No licenses found.")
            return
        
        # Show the main summary
        print(f"\n🎫 Users by License Type ({'Including Expired' if include_expired else 'Active Only'}):")
        counts_df.show(truncate=False)
        
        # Get total counts
        total_query = """
            SELECT 
                COUNT(DISTINCT userid) as total_unique_users,
                COUNT(*) as total_licenses
            FROM kaksha_licenses
            WHERE status = 'ACTIVE'
        """
        
        if not include_expired:
            total_query += " AND expiry_date >= current_date()"
            
        total_df = self.spark.sql(total_query)
        totals = total_df.collect()[0]
        
        print(f"\n📈 Overall Totals:")
        print(f"   • Total Unique Users: {totals['total_unique_users']}")
        print(f"   • Total Licenses: {totals['total_licenses']}")

    def run(self, include_expired=False, detailed_analysis=True):
        """
        Main method to run the license counting job
        
        Args:
            include_expired (bool): Whether to include expired licenses in counts. Default is False
            detailed_analysis (bool): Whether to show detailed breakdown. Default is True
        """
        print("🚀 Starting License User Count Job...")
        print("=" * 60)
        
        try:
            # Get basic counts by license type
            print(f"\n📊 Getting license user counts ({'including expired' if include_expired else 'active only'})...")
            counts_df = self.get_user_count_by_license_type(include_expired=include_expired)
            
            if counts_df.rdd.isEmpty():
                print("❌ No licenses found in the database.")
                return
            
            # Show main results
            print(f"\n🎫 Users by License Type:")
            counts_df.show(truncate=False)
            
            # Calculate and show totals
            total_query = """
                SELECT 
                    COUNT(DISTINCT userid) as total_unique_users,
                    COUNT(*) as total_licenses
                FROM kaksha_licenses
                WHERE status = 'ACTIVE'
            """
            
            if not include_expired:
                total_query += " AND expiry_date >= current_date()"
                
            total_df = self.spark.sql(total_query)
            totals = total_df.collect()[0]
            
            print(f"\n📈 Summary Totals:")
            print(f"   • Total Unique Users: {totals['total_unique_users']}")
            print(f"   • Total Active Licenses: {totals['total_licenses']}")
            
            if detailed_analysis:
                print(f"\n🔍 Detailed Analysis:")
                print("-" * 40)
                
                # Show breakdown by product
                print(f"\n📦 Breakdown by License Type and Product:")
                product_counts = self.get_user_count_by_license_type_and_product(include_expired=include_expired)
                product_counts.show(truncate=False)
                
                # Show comprehensive summary
                detailed_summary = self.get_detailed_license_summary()
                
                print(f"\n📋 License Status Overview:")
                detailed_summary['by_status'].show(truncate=False)
                
                print(f"\n⏰ Active vs Expired Breakdown:")
                detailed_summary['expiry_breakdown'].show(truncate=False)
            
            print(f"\n✅ License counting job completed successfully!")
            
        except Exception as e:
            print(f"❌ Error during license counting job: {str(e)}")
            raise e
