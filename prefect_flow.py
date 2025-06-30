from prefect import task, flow, get_run_logger
from config.logging_config import LoggingConfig
from src.pyspark_app.license_processor import LicenseProcessor
import os
from datetime import datetime

# Setup logging once
log_file = os.getenv("LOG_FILE", f"logs/license_processor_{datetime.now().strftime('%Y%m%d')}.log")
LoggingConfig.setup_logging(
    log_level=os.getenv("LOG_LEVEL", "INFO"),
    log_file=log_file,
    enable_console=True
)
logger = LoggingConfig.get_logger(__name__)


@task(retries=3, retry_delay_seconds=60)
def initialize_processor() -> LicenseProcessor:
    processor = LicenseProcessor(logger=logger)
    if not processor.initialize():
        raise Exception("❌ Failed to initialize License Processor")
    if not processor.test_connections():
        raise Exception("❌ Database connection tests failed")
    logger.info("✅ Initialization complete")
    return processor


@task(retries=3, retry_delay_seconds=60)
def run_pending_requests(processor: LicenseProcessor):
    logger.info("🚀 Running pending license requests job...")
    processor.run_pending_license_request_job()
    logger.info("✅ Completed pending license requests job.")


@task(retries=3, retry_delay_seconds=60)
def run_expired_licenses(processor: LicenseProcessor):
    logger.info("🚀 Running expired license cleanup job...")
    processor.run_expired_license_job()
    logger.info("✅ Completed expired license job.")


@task(retries=2, retry_delay_seconds=30)
def count_user_licenses(processor: LicenseProcessor):
    logger.info("📊 Counting user licenses by type...")
    processor.count_user_license_by_type()
    logger.info("✅ Completed license count job.")


@flow(name="License Management Pipeline")
def license_management_flow():
    logger.info("🌅 Starting License Management Prefect Flow...")

    processor = initialize_processor()

    run_pending_requests(processor)
    run_expired_licenses(processor)
    count_user_licenses(processor)

    logger.info("🏁 License Management Flow completed.")


if __name__ == "__main__":
    license_management_flow()

