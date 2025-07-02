from prefect import flow, task
from config.logging_config import LoggingConfig
from src.pyspark_app.license_processor import LicenseProcessor 
from prefect.tasks import NO_CACHE
import os
from datetime import datetime

# ---------- Setup Logging (reuses your existing LoggingConfig) ----------
log_file = os.getenv("LOG_FILE", f"logs/license_processor_{datetime.now().strftime('%Y%m%d')}.log")
LoggingConfig.setup_logging(
    log_level=os.getenv("LOG_LEVEL", "INFO"),
    log_file=log_file,
    enable_console=True
)
logger = LoggingConfig.get_logger(__name__)

# ---------- Prefect Tasks ----------
@task(name="Step 1.1: Initialize Processor", retries=2, retry_delay_seconds=10)
def init_and_test_connections():
    processor = LicenseProcessor(logger=logger)
    if not processor.initialize():
        logger.error("❌ Failed to initialize License Processor")
        raise RuntimeError("Initialization failed")
    if not processor.test_connections():
        logger.error("❌ Database connection tests failed")
        raise ConnectionError("DB connection failed")
    logger.info("✅ Step 1.1 Environment Setup completed successfully!")
    return processor

@task(name="Step 1.2: Process Pending Requests", retries=3, retry_delay_seconds=20)
def run_pending_requests(processor: LicenseProcessor):
    processor.run_pending_license_request_job()
    logger.info("✅ Step 1.2 run pending license request completed successfully!")

@task(name="Step 1.3: Process Expired Licenses", retries=2, retry_delay_seconds=15)
def run_expired_requests(processor: LicenseProcessor):
    processor.run_expired_license_job()
    logger.info("✅ Step 1.3 run expired license job completed successfully!")

@task(name="Step 1.4: Count Users by License Type", retries=1)
def count_users_by_license_type(processor: LicenseProcessor):
    processor.count_user_license_by_type()
    logger.info("✅ Count user by license type completed successfully!")

@task(name="Cleanup Processor")
def cleanup_processor(processor: LicenseProcessor):
    if processor:
        processor.cleanup()
        logger.info("🧹 Processor cleanup complete")

# ---------- Flow Definition ----------
# ---------- Alternative Approach (Recommended) ----------
@flow(name="License Management Pipeline v2")
def license_flow():
    """
    Alternative approach: Handle cleanup within the flow logic
    """
    try:
        # Initialize processor
        processor = init_and_test_connections()
        
        # Run processing tasks
        run_pending_requests(processor)
        run_expired_requests(processor)
        count_users_by_license_type(processor)
        
        # Cleanup as part of normal flow
        cleanup_processor(processor)
        
    except Exception as e:
        logger.error(f"❌ Flow failed with error: {e}")
        # Note: In this approach, cleanup only happens on success
        # You might want to add error-specific cleanup logic here
        raise

# ---------- For Local Testing ----------
if __name__ == "__main__":
    license_flow_v2()
