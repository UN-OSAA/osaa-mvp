"""Ingest module for data processing and S3 upload.

This module handles the ingestion of CSV files, converting them to Parquet,
and optionally uploading them to S3 for the United Nations OSAA MVP project.
"""

import os
import re
from typing import Dict, Optional

import duckdb

import pipeline.config as config
from pipeline.utils import s3_init, setup_logger

# Setup
logger = setup_logger(__name__)


class Ingest:
    """Manage the data ingestion process for the United Nations OSAA MVP project.

    This class handles the conversion of CSV files to Parquet format,
    manages database connections, and optionally uploads processed files
    to S3 storage. It provides methods for file processing, table creation,
    and data transformation.

    Key features:
    - Convert CSV files to Parquet
    - Create and manage database tables
    - Optionally upload processed files to S3
    - Handle environment-specific configurations
    """

    def __init__(self) -> None:
        """Initialize the IngestProcess with DuckDB connection and S3 session.

        Sets up a DuckDB connection and optionally initializes an S3 session
        based on the configuration settings.
        """
        logger.info("🚀 Initializing Ingest Process")
        logger.info(f"   Database Path: {config.DB_PATH}")
        logger.info(f"   S3 Upload Enabled: {config.ENABLE_S3_UPLOAD}")

        self.con = duckdb.connect(config.DB_PATH)
        if config.ENABLE_S3_UPLOAD:
            logger.info("   Initializing S3 client...")
            self.s3_client, self.session = s3_init(return_session=True)
            logger.info("   ✅ S3 Client Initialized")
        else:
            logger.warning("   ⚠️ S3 upload is disabled")
            self.s3_client = None
            self.session = None

    def setup_s3_secret(self) -> None:
        """Set up the S3 secret in DuckDB for S3 access.

        Creates a DuckDB secret with AWS credentials if S3 upload is enabled.
        Logs the setup process and any potential errors.
        """
        if not config.ENABLE_S3_UPLOAD:
            logger.info("S3 upload disabled, skipping S3 secret setup")
            return

        try:
            region = self.session.region_name
            credentials = self.session.get_credentials().get_frozen_credentials()

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
            logger.info("S3 secret setup in DuckDB.")

        except Exception as e:
            logger.error(f"Error setting up S3 secret: {e}")
            raise

    def convert_csv_to_parquet_and_upload(
        self, local_file_path: str, s3_file_path: Optional[str] = None
    ) -> None:
        """Convert a CSV file to Parquet and optionally upload it to S3.

        Args:
            local_file_path: Path to the local CSV file.
            s3_file_path: Optional S3 file path for the output Parquet file.
        """
        try:
            table_name_match = re.search(r"[^/]+(?=\.)", local_file_path)
            table_name = (
                table_name_match.group(0).replace("-", "_") if table_name_match else "UNNAMED"
            )
            fully_qualified_name = "source." + table_name

            self.con.sql("CREATE SCHEMA IF NOT EXISTS source")

            # Use parameterized query to prevent SQL injection
            drop_table_query = "DROP TABLE IF EXISTS ?"
            create_table_query = """
                CREATE TABLE ? AS
                SELECT *
                FROM read_csv(?, header = true)
            """
            copy_table_query = """
                COPY (SELECT * FROM ?)
                TO ?
                (FORMAT PARQUET)
            """

            self.con.execute(drop_table_query, [fully_qualified_name])
            self.con.execute(create_table_query, [fully_qualified_name, local_file_path])

            logger.info(f"Successfully created table {fully_qualified_name}")

            if s3_file_path:
                self.con.execute(copy_table_query, [fully_qualified_name, s3_file_path])
                logger.info(
                    f"Successfully converted and uploaded {local_file_path} to {s3_file_path}"
                )
            else:
                logger.info(f"Successfully converted {local_file_path}")

        except Exception as e:
            logger.error(
                f"Error converting and uploading {local_file_path} to S3: {e}",
                exc_info=True,
            )
            raise

    def generate_file_to_s3_folder_mapping(self, raw_data_dir: str) -> Dict[str, str]:
        """Generate mapping of local files to their respective S3 folders.

        Excludes any folder and file that starts with symbols.

        Args:
            raw_data_dir: The base directory containing raw data subfolders.

        Returns:
            A dictionary where the key is the filename and the value is the subfolder name.
        """
        file_to_s3_folder_mapping: Dict[str, str] = {}

        # Pattern to exclude any folder/file name with symbols (like hidden files)
        symbols = r"^[~!@#$%^&*()_\-+={[}}|:;\"'<,>.?/]+"

        # Traverse the raw_data directory
        for subdir, _, files in os.walk(raw_data_dir):
            logger.info(f"Walking directory: {subdir}, found files: {files}")
            # Get the subfolder name (relative to raw_data_dir)
            sub_folder = os.path.relpath(subdir, raw_data_dir)

            # Exclude folders that start with any symbol in the excluded set
            if re.match(symbols, sub_folder):
                logger.info(f"Skipping folder: {sub_folder} due to symbols.")
                continue

            # Map each file to its corresponding subfolder, but exclude files starting with symbols
            for file_name in files:
                if not re.match(symbols, file_name):
                    logger.info(f"Mapping file: {file_name} in subfolder {sub_folder}")
                    file_to_s3_folder_mapping[file_name] = sub_folder

        logger.info(f"Generated file mapping: {file_to_s3_folder_mapping}")
        return file_to_s3_folder_mapping

    def convert_and_upload_files(self) -> None:
        """Convert CSV files to Parquet and optionally upload them to S3.

        Generates a file mapping, processes each file, and converts it to Parquet.
        Uploads to S3 if enabled in the configuration.
        """
        try:
            logger.info("🔄 Starting File Conversion and Upload Process")
            logger.info(f"   Raw Data Directory: {config.RAW_DATA_DIR}")
            logger.info(f"   S3 Bucket: {config.S3_BUCKET_NAME}")
            logger.info(f"   Landing Area Folder: {config.LANDING_AREA_FOLDER}")

            file_mapping = self.generate_file_to_s3_folder_mapping(config.RAW_DATA_DIR)

            logger.info(f"📊 Found {len(file_mapping)} files to process")

            for file_name_csv, s3_sub_folder in file_mapping.items():
                local_file_path = os.path.join(config.RAW_DATA_DIR, s3_sub_folder, file_name_csv)

                # Only set up S3 path if uploads are enabled
                s3_file_path = None
                if config.ENABLE_S3_UPLOAD:
                    file_name_pq = f"{os.path.splitext(file_name_csv)[0]}.parquet"

                    s3_file_path = (
                        f"s3://{config.S3_BUCKET_NAME}/"
                        f"{config.LANDING_AREA_FOLDER}/{s3_sub_folder}/{file_name_pq}"
                    )
                    logger.info(f"📤 Preparing S3 upload for: {s3_file_path}")
                else:
                    logger.warning("   ⚠️ S3 upload disabled, skipping S3 path generation")

                logger.info(f"🔍 Processing local file: {local_file_path}")

                if os.path.isfile(local_file_path):
                    self.convert_csv_to_parquet_and_upload(local_file_path, s3_file_path)
                else:
                    logger.warning(f"❌ File not found: {local_file_path}")

            logger.info("✅ Ingestion process completed successfully!")

        except Exception as e:
            logger.error(f"❌ Error during file ingestion: {e}")
            raise

    def run(self) -> None:
        """Run the entire ingestion process.

        Sets up S3 secret and converts/uploads files, ensuring the database
        connection is closed after processing.
        """
        try:
            self.setup_s3_secret()
            self.convert_and_upload_files()
        finally:
            self.con.close()


if __name__ == "__main__":
    ingest_process = Ingest()
    ingest_process.run()
