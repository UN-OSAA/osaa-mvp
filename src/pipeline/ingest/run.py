import os
import re
import duckdb
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from pipeline.utils import setup_logger, s3_init
import pipeline.config as config

# Setup
logger = setup_logger(__name__)

# Custom Exceptions
class IngestError(Exception):
    """Base exception for ingestion process errors."""
    pass

class S3ConfigurationError(IngestError):
    """Exception raised for S3 configuration or connection issues."""
    pass

class FileConversionError(IngestError):
    """Exception raised for errors during file conversion."""
    pass

class Ingest:
    def __init__(self):
        """
        Initialize the IngestProcess with DuckDB connection and optionally S3 session.
        
        :raises IngestError: If there are issues initializing the ingestion process
        """
        try:
            # Validate DB path
            if not os.path.exists(os.path.dirname(config.DB_PATH)):
                os.makedirs(os.path.dirname(config.DB_PATH), exist_ok=True)
            
            self.con = duckdb.connect(config.DB_PATH)
            
            if config.ENABLE_S3_UPLOAD:
                try:
                    self.s3_client, self.session = s3_init(return_session=True)
                except Exception as e:
                    logger.error(f"Failed to initialize S3 client: {e}")
                    raise S3ConfigurationError(f"S3 initialization failed: {e}")
            else:
                logger.info("S3 upload disabled, skipping S3 initialization")
                self.s3_client = None
                self.session = None
        
        except Exception as e:
            logger.error(f"Initialization error: {e}")
            raise IngestError(f"Failed to initialize Ingest process: {e}")

    def setup_s3_secret(self):
        """
        Set up the S3 secret in DuckDB for S3 access.
        
        :raises S3ConfigurationError: If there are issues setting up S3 secret
        """
        if not config.ENABLE_S3_UPLOAD:
            logger.info("S3 upload disabled, skipping S3 secret setup")
            return

        try:
            # Validate session and credentials
            if not self.session:
                raise S3ConfigurationError("No S3 session available")

            region = self.session.region_name
            credentials = self.session.get_credentials().get_frozen_credentials()

            # Validate credentials
            if not all([credentials.access_key, credentials.secret_key, region]):
                raise S3ConfigurationError("Incomplete S3 credentials")

            self.con.sql(f"""
            CREATE SECRET IF NOT EXISTS my_s3_secret (
                TYPE S3,
                KEY_ID '{credentials.access_key}',
                SECRET '{credentials.secret_key}',
                REGION '{region}'
            );
            """)
            logger.info("S3 secret setup in DuckDB successfully.")

        except (NoCredentialsError, ClientError) as e:
            error_msg = f"AWS Credentials Error: {e}"
            logger.error(error_msg)
            raise S3ConfigurationError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error setting up S3 secret: {e}"
            logger.error(error_msg)
            raise S3ConfigurationError(error_msg)

    def convert_csv_to_parquet_and_upload(self, local_file_path: str, s3_file_path: str):
        """
        Convert a CSV file to Parquet and upload it to S3.
        
        :param local_file_path: Path to the local CSV file
        :param s3_file_path: Destination S3 path for the Parquet file
        :raises FileConversionError: If there are issues converting or uploading the file
        """
        try:
            # Input validation
            if not os.path.exists(local_file_path):
                raise FileNotFoundError(f"Local file not found: {local_file_path}")
            
            if not local_file_path.lower().endswith('.csv'):
                raise ValueError(f"Expected CSV file, got: {local_file_path}")

            table_name = re.search(r'[^/]+(?=\.)', local_file_path)
            table_name = table_name.group(0).replace('-','_') if table_name else "UNNAMED"
            fully_qualified_name = 'source.' + table_name

            self.con.sql("CREATE SCHEMA IF NOT EXISTS source")

            self.con.sql(f"drop table if exists {fully_qualified_name}")
            self.con.sql(f"""
                CREATE TABLE {fully_qualified_name} AS
                SELECT * 
                FROM read_csv('{local_file_path}', header = true)
                """           
            )

            logger.info(f"Successfully created table {fully_qualified_name}")
            
            self.con.sql(f"""
                COPY (SELECT * FROM {fully_qualified_name})
                TO '{s3_file_path}'
                (FORMAT PARQUET)
                """
            )

            logger.info(f"Successfully converted and uploaded {local_file_path} to {s3_file_path}")

        except FileNotFoundError as e:
            logger.error(f"File not found error: {e}")
            raise FileConversionError(str(e))
        except ValueError as e:
            logger.error(f"Invalid file type: {e}")
            raise FileConversionError(str(e))
        except Exception as e:
            logger.error(f"Unexpected error in file conversion: {e}")
            raise FileConversionError(f"Conversion failed: {e}")

    def generate_file_to_s3_folder_mapping(self, raw_data_dir: str) -> dict:
        """
        Generate mapping of local files to their respective S3 folders. Excludes any folder and file that starts with symbols.

        :param raw_data_dir: The base directory containing raw data subfolders.
        :return: A dictionary where the key is the filename and the value is the subfolder name.
        """
        file_to_s3_folder_mapping = {}

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


    def convert_and_upload_files(self):
        """
        Convert CSV files to Parquet and optionally upload them to S3.
        """
        try:
            file_mapping = self.generate_file_to_s3_folder_mapping(config.RAW_DATA_DIR)
            for file_name_csv, s3_sub_folder in file_mapping.items():

                local_file_path = os.path.join(config.RAW_DATA_DIR, s3_sub_folder, file_name_csv)

                # Only set up S3 path if uploads are enabled
                s3_file_path = None
                if config.ENABLE_S3_UPLOAD:
                    file_name_pq = f'{os.path.splitext(file_name_csv)[0]}.parquet'

                    s3_file_path = f's3://{config.S3_BUCKET_NAME}/{config.LANDING_AREA_FOLDER}/{s3_sub_folder}/{file_name_pq}'
                    logger.info(f"Uploading to S3: {s3_file_path}")
                else:
                    logger.info("S3 upload disabled, skipping S3 path generation")

                logger.info(f"Processing local file: {local_file_path}")

                if os.path.isfile(local_file_path):
                    self.convert_csv_to_parquet_and_upload(local_file_path, s3_file_path)
                else:
                    logger.warning(f'File not found: {local_file_path}')
            logger.info("Ingestion process completed successfully.")
            
        except Exception as e:
            logger.error(f"Error during file ingestion: {e}")
            raise

    def run(self):
        """
        Main method to run the ingestion process.
        
        :raises IngestError: If the entire ingestion process fails
        """
        try:
            # Setup S3 secret if enabled
            if config.ENABLE_S3_UPLOAD:
                self.setup_s3_secret()
            
            # Add your specific ingestion logic here
            logger.info("Ingestion process started")
            
            # Example: Process files
            # You would replace this with your actual file processing logic
            self.convert_and_upload_files()
            
            logger.info("Ingestion process completed successfully")
        
        except IngestError as e:
            logger.error(f"Ingestion process failed: {e}")
            raise
        except Exception as e:
            error_msg = f"Unexpected error in ingestion process: {e}"
            logger.error(error_msg)
            raise IngestError(error_msg)

if __name__ == '__main__':
    try:
        ingest_process = Ingest()
        ingest_process.run()
    except IngestError as e:
        logger.error(f"Ingestion process failed: {e}")
        # Optionally, you could add more sophisticated error handling here
        # such as sending an alert, retrying, or taking corrective action
        exit(1)