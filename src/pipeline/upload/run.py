
import duckdb
from pipeline.utils import setup_logger, s3_init
import pipeline.config as config

# Setup
logger = setup_logger(__name__)

class Upload:
    def __init__(self):
        """
        Initialize the IngestProcess with S3 session and DuckDB connection.
        """
        self.s3_client, self.session = s3_init(return_session=True)
        self.con = duckdb.connect(config.DB_PATH)
        self.env = config.TARGET

    def setup_s3_secret(self):
        """
        Set up the S3 secret in DuckDB for S3 access.
        """
        try:
            region = self.session.region_name
            credentials = self.session.get_credentials().get_frozen_credentials()

            self.con.sql(f"""
            CREATE SECRET my_s3_secret (
                TYPE S3,
                KEY_ID '{credentials.access_key}',
                SECRET '{credentials.secret_key}',
                REGION '{region}'
            );
            """)
            logger.info("S3 secret setup in DuckDB.")

        except Exception as e:
            logger.error(f"Error setting up S3 secret: {e}")
            raise

    def upload(self, schema_name: str, table_name: str, s3_file_path: str):
        """
        Upload a Duckdb table to s3, given the schema and table name and path.
        """    
        # Format the fully qualified table name with environment
        if self.env == 'prod':
            fully_qualified_name = f"{schema_name}.{table_name}"
        else:
            fully_qualified_name = f"{schema_name}__{self.env}.{table_name}"
        
        self.con.sql(f"""
            COPY (SELECT * FROM {fully_qualified_name})
            TO '{s3_file_path}'
            (FORMAT PARQUET)
        """)

        logger.info(f"Uploaded {fully_qualified_name} to S3: {s3_file_path}")
        
    def run(self):
        """
        Run the entire upload process.
        """
        try:
            self.setup_s3_secret()
            self.upload(
                "intermediate",
                "wdi",
                f's3://{config.S3_BUCKET_NAME}/{config.TRANSFORMED_AREA_FOLDER}/wdi/wdi_transformed.parquet'
            )
        finally:
            self.con.close()

if __name__ == '__main__':
    upload_process = Upload()
    upload_process.run()