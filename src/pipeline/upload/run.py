"""Upload module for data transfer to S3.

This module handles the upload of processed data to S3 storage
for the United Nations OSAA MVP project.
"""

import duckdb

import pipeline.config as config
from pipeline.utils import s3_init, setup_logger

# Setup
logger = setup_logger(__name__)


class Upload:
    """Manage the upload of processed data tables to S3 storage.

    This class handles the process of uploading DuckDB tables to S3,
    including setting up S3 credentials and managing the upload process
    for different environment configurations.
    """

    def __init__(self) -> None:
        """Initialize the Upload process with S3 session and DuckDB connection.

        Sets up an S3 client, DuckDB connection, and retrieves the current
        environment target.
        """
        logger.info("🚀 Initializing Upload Process")
        logger.info(f"   Environment Target: {config.TARGET}")
        logger.info(f"   Database Path: {config.DB_PATH}")

        self.s3_client, self.session = s3_init(return_session=True)
        logger.info("   ✅ S3 Client Initialized")

        self.con = duckdb.connect(config.DB_PATH)
        self.env = config.TARGET

    def setup_s3_secret(self) -> None:
        """Set up the S3 secret in DuckDB for S3 access.

        Creates a DuckDB secret with AWS credentials, enabling S3 interactions.
        Logs the setup process and handles any potential errors.
        """
        try:
            logger.info("🔐 Setting up S3 Secret in DuckDB")

            region = self.session.region_name
            credentials = self.session.get_credentials().get_frozen_credentials()

            logger.info(f"   AWS Region: {region}")
            logger.info("   Creating S3 secret with AWS credentials")

            self.con.sql(
                f"""
            CREATE SECRET my_s3_secret (
                TYPE S3,
                KEY_ID '{credentials.access_key}',
                SECRET '{credentials.secret_key}',
                REGION '{region}'
            );
            """
            )
            logger.info("✅ S3 secret successfully created in DuckDB")

        except Exception as e:
            logger.error(f"❌ Error setting up S3 secret: {e}")
            raise

    def upload(self, schema_name: str, table_name: str, s3_file_path: str) -> None:
        """Upload a DuckDB table to S3.

        Args:
            schema_name: Name of the schema containing the table.
            table_name: Name of the table to upload.
            s3_file_path: Destination S3 file path for the uploaded table.
        """
        # Format the fully qualified table name with environment
        if self.env == "prod":
            fully_qualified_name = f"{schema_name}.{table_name}"
        else:
            fully_qualified_name = f"{schema_name}__{self.env}.{table_name}"

        # Use parameterized query to prevent SQL injection
        copy_table_query = """
            COPY (SELECT * FROM ?)
            TO ?
            (FORMAT PARQUET)
        """

        self.con.execute(copy_table_query, [fully_qualified_name, s3_file_path])

        logger.info(f"Uploaded {fully_qualified_name} to S3: {s3_file_path}")

    def run(self) -> None:
        """Run the entire upload process.

        Sets up S3 secret, uploads specified tables, and ensures
        the database connection is closed after processing.
        """
        try:
            logger.info("🔄 Starting Upload Process")
            logger.info(f"   S3 Bucket: {config.S3_BUCKET_NAME}")
            logger.info(f"   Transformed Area Folder: {config.TRANSFORMED_AREA_FOLDER}")

            self.setup_s3_secret()

            upload_path = (
                f"s3://{config.S3_BUCKET_NAME}/"
                f"{config.TRANSFORMED_AREA_FOLDER}/wdi/wdi_transformed.parquet"
            )

            logger.info("📤 Uploading WDI Transformed Data")
            logger.info("   Source: intermediate.wdi")
            logger.info("   Destination: " + upload_path)

            self.upload("intermediate", "wdi", upload_path)

            logger.info("✅ Upload process completed successfully!")

        except Exception as e:
            logger.error(f"❌ Error during upload process: {e}")
            raise

        finally:
            logger.info("🔒 Closing database connection")
            self.con.close()


if __name__ == "__main__":
    upload_process = Upload()
    upload_process.run()
