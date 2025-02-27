# OSAA Data Pipeline MVP

## 1. Purpose

This project implements a **Minimum Viable Product** (MVP) Data Pipeline for the United Nations Office of the Special Adviser on Africa (OSAA), leveraging modern data engineering tools to create an efficient and scalable data processing system.

## 2. Quickstart

Here's how to get started with the OSAA Data Pipeline:

1. **Setup Environment**
   ```bash
   # Clone the repository
   git clone https://github.com/UN-OSAA/osaa-mvp.git
   cd osaa-mvp

   # Copy and configure environment variables (get from your team lead)
   cp .env.example .env
   ```

2. **Build the Container** (Required before first run and after code changes)
   ```bash
   # Build the Docker container - this may take a few minutes
   docker build -t osaa-mvp .
   ```

3. **Run the Pipeline**
   ```bash
   # Run the complete pipeline
   docker compose run --rm pipeline etl
   ```

4. **Common Commands**
   ```bash
   # Run only data ingestion
   docker compose run --rm pipeline ingest

   # Run only transformations
   docker compose run --rm pipeline transform
   
   # Run a configuration test
   docker compose run --rm pipeline config_test
   
   # Promote data (from dev to prod)
   docker compose run --rm pipeline promote

   # Run in development mode with your username
   docker compose run --rm -e USERNAME=your_name pipeline etl
   ```

5. **View Results**
   - Processed data will be available in the S3 bucket
   - Source files: `s3://unosaa-data-pipeline/dev/landing/...`
   - Your development data: `s3://unosaa-data-pipeline/dev/dev_{USERNAME}/...`
   - Production data: `s3://unosaa-data-pipeline/prod/...`

For detailed instructions and advanced usage, see the sections below.

## 3. Getting Started

### 3.1 Required Software

- [Docker Desktop](https://www.docker.com/products/docker-desktop/): Available for Windows, Mac, and Linux
- [Git](https://git-scm.com/downloads): Choose the version for your operating system

After installing Docker Desktop, you'll need to start the application before running any pipeline commands.

### 3.2 Basic Setup

1. **Clone the Repository**
   ```bash
   git clone https://github.com/UN-OSAA/osaa-mvp.git
   cd osaa-mvp
   ```

2. **Configure Environment**
   Copy the example configuration file:
   ```bash
   cp .env.example .env
   ```
   Get the required credentials from your team lead and update `.env`

3. **Build and Run**
   ```bash
   # IMPORTANT: Build the Docker container first
   docker build -t osaa-mvp .

   # Then run the pipeline
   docker compose run --rm pipeline etl
   ```

   Note: You must rebuild the container whenever you make changes to the code or when pulling updates from GitHub

### 3.3 Troubleshooting Common Issues

#### Docker Issues

1. **Docker Not Running**
   - Make sure Docker Desktop is running
   - Restart Docker Desktop if needed
   - Check system resources

2. **Network Issues**
   - Ensure system is connected to the internet
   - Check VPN status if required

#### Pipeline Issues

1. **Access Denied**
   - Verify your credentials in `.env`
   - Contact your team lead for valid credentials

2. **Data Not Found**
   - Check that your source data is in the correct location
   - Verify file names and formats

If issues persist, contact your team lead with detailed error information.

## 4. Working with Data

### 4.1 Running the Pipeline

The pipeline processes data in three main steps:
1. **Ingest**: Converts source data (CSV) to optimized format
2. **Transform**: Applies data transformations and cleaning
3. **Upload**: Stores results in the cloud

#### Basic Commands
```bash
# Run the complete pipeline
docker compose run --rm pipeline ingest
docker compose run --rm pipeline etl

# Run individual steps
docker compose run --rm pipeline ingest    # Only ingest new data
docker compose run --rm pipeline transform # Only run transformations
```

### 4.2 Adding New Data

To add a new dataset:

1. **Stage Your Data in the local upload folder**
   - Save your CSV file in `data/raw/<source_name>/`
   - Ensure the data follows the required format

2. **Intake the data into SQLMesh with a source model**
   - Create a source model for your new source in SQLMesh: `sqlMesh/models/sources/<source_name>/<source_model_name>.sql`

   Example:
   ```sql
   MODEL (
      name sdg.data_national,
      kind FULL,
      cron '@daily',
      columns (
         INDICATOR_ID TEXT,
         COUNTRY_ID TEXT,
         YEAR INTEGER,
         VALUE DECIMAL,
         MAGNITUDE TEXT,
         QUALIFIER TEXT
      )
   );

   SELECT
      *
   FROM
      read_parquet(
         @s3_read('who/who_life_expectancy.csv')
      );
   ```
   Note:
   - Indicate the kind of model you want to create (FULL, INCREMENTAL, etc.). Use incremental style for large datasets that will be run frequently.
   - Define the column schema for the new source.

3. **Add a transformation model**
   - Create a transformation model (if needed) in SQLMesh using Ibis. Add the model to the same folder as the source model above.

4. **Run the Pipeline**
   ```bash
   # Process your new data
   docker build -t osaa-mvp .
   docker compose run --rm pipeline ingest
   docker compose run --rm pipeline etl
   ```

5. **Verify Results**
   - Check the S3 bucket for your processed data
   - Review any error messages if the process fails

### 4.4 Using SQLMesh UI to Verify Data
After running the pipeline, you can use the SQLMesh UI to verify the data.

1. **Start the UI**
   ```bash
   docker compose --profile ui up ui
   ```

2. **Access the Interface**
   - Open `http://localhost:8080` in your browser
   - Use the Editor tab to inspect individual models and their data
   - Use the Data Catalog to visualize data lineage and model's documentation

3. **Stop the UI**
   ```bash
   # Use Ctrl+C to stop when finished
   ```

### 4.5 Development vs Production

The pipeline has two main modes:
- **Development**: Your personal workspace for testing
- **Production**: Official data processing (restricted access)

Always work in development mode unless instructed otherwise:
```bash
# Run in development mode with your username
docker compose run --rm -e USERNAME=your_name pipeline etl
```

## 5. Getting Help

If you encounter issues or need assistance:
1. Check the troubleshooting section above
2. Review any error messages carefully
3. Contact your team lead or technical support

## 6. Project Structure

### 6.1 Repository Overview

The project repo consists of several key components:
1. The SQLMesh project containing all transformations
2. Docker container configuration files
3. Local development environment files

### 6.2 Directory Structure

```
osaa-mvp/
├── data/                      # Local representation of the datalake
│   ├── raw/                   # Source data files (CSV)
│   │   ├── edu/               # Contains educational datasets
│   │   └── wdi/               # World Development Indicators datasets
│   └── staging/               # Staging area for processed Parquet files
├── scratchpad/                # Temporary space for working code or notes
├── sqlMesh/                   # SQLMesh configuration and models
│   ├── models/                # SQLMesh model definitions
│   └── unosaa_data_pipeline.db            # DuckDB database for SQLMesh transformations
├── src/
│   └── pipeline/             # Core pipeline code
│       ├── ingest/           # Handles data ingestion from local raw csv to S3 parquet
│       ├── upload/           # Handles DuckDB transformed data upload to S3
│       ├── s3_sync/          # Handles SQLMesh database files sync with S3
│       ├── s3_promote/       # Handles data promotion between environments
│       ├── utils/            # Utility functions and modules
│       ├── catalog.py        # Defines data catalog interactions
│       ├── config.py         # Stores configuration details
│       ├── utils.py          # Utility functions (to be migrated to utils/)
├── .env_example              # Environment variables template
├── dockerfile                # Docker container definition
├── docker-compose.yml        # Docker services configuration
├── entrypoint.sh             # Docker container entry point script
└── requirements.txt          # Python package dependencies
```

### 6.3 Cloud Storage Structure

```
s3://osaa-mvp/                 # Base bucket
│
├── dev/                     # Development environment
│   ├── landing/             # Landing zone for raw data
│   └── dev_{username}/      
|       └── staging/         # Development staging area
|           ├── _metadata/   # Metadata models
|           └── master/      # Final unified models
│
├── qa/                      # QA environment
│   ├── landing/             # QA landing zone
│   └── staging/             # QA staging area
│
└── prod/                    # Production environment
    ├── landing/             # Production landing zone
    └── staging/             # Production staging area
```

### 6.4 Source Code Structure

The `src/pipeline` directory contains the core pipeline commands:

```
src/pipeline/
├── ingest/                # Handles 'ingest' command
│   └── run.py             # Converts CSVs to Parquet
├── upload/                # Handles 'upload' command
│   └── run.py             # Uploads transformed data
├── s3_sync/               # Handles 's3_sync' command
│   └── run.py             # sync SQLMesh database files with S3
├── s3_promote/            # Handles 's3_promote' command
│   └── run.py             # Promotes data between environments
├── catalog.py             # Manages data locations
├── config.py              # Handles configuration
└── utils.py               # Shared utilities
```

## 7. CI/CD Workflows

### 7.1 Deploy to GHCR

[`.github/workflows/deploy_to_ghcr.yml`](.github/workflows/deploy_to_ghcr.yml)

Triggered when PRs are merged to main:
- Builds the container
- Runs QA process
- Pushes container to GitHub Container Registry

### 7.2 Run from GHCR

[`.github/workflows/run_from_ghcr.yml`](.github/workflows/run_from_ghcr.yml)

Triggered on every push:
- Builds the container
- Runs transform process
- Validates container execution

### 7.3 Daily Transform

[`.github/workflows/daily_transform.yml`](.github/workflows/daily_transform.yml)

Automated daily data processing:
- Runs at scheduled times
- Processes new data in production
- Updates analytics outputs

## 8. Security Notes

- Never commit `.env` files containing sensitive credentials
- Store all sensitive information as GitHub Secrets for CI/CD

## 9. Next Steps

### 9.1 Data Processing Improvements

- Add support for more data sources and formats
- Enhance data validation and quality checks
- Optimize transformation performance
- Expand the data catalog

### 9.2 User Interface

- Add web-based data exploration tools
- Create interactive dashboards
- Develop automated reporting capabilities
- Improve documentation and user guides

## Contact

- Mirian Lima (Project Sponsor) - mirian.lima@un.org
- Stephen Sciortino (Principal Engineer) - stephen.sciortino@un.org
- Project Link: [https://github.com/UN-OSAA/osaa-mvp.git](https://github.com/UN-OSAA/osaa-mvp.git)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgement

This project was **heavily inspired by** the work of [Cody Peterson](https://github.com/lostmygithubaccount), specifically the [ibis-analytics](https://github.com/ibis-project/ibis-analytics) repository. While the initial direction and structure of the project were derived from Cody's original work, significant modifications and expansions have been made to fit the needs and goals of this project.
