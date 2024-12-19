# OSAA Data Pipeline MVP

## Table of Contents
- [Purpose](#purpose)
- [How It Works](#how-it-works)
- [Getting Started](#getting-started)
- [Dependencies](#dependencies)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Purpose

This project implements a **Minimum Viable Product** (MVP) Data Pipeline for the United Nations Office of the Special Adviser on Africa (OSAA), leveraging modern data engineering tools to create an efficient and scalable data processing system.

### Key Technologies
- **Ibis**: Provides a unified interface for data manipulation
- **DuckDB**: In-memory analytical database for fast data processing
- **Parquet**: Columnar storage format for efficient data storage
- **S3**: Cloud storage for data lake architecture

## How It Works

### Pipeline Process
The data pipeline consists of three main stages:

#### Ingestion Process (`ingest/run.py`)
- Reads raw CSV files from `data/raw/<source>` directories
- Converts them to Parquet format using DuckDB
- Uploads the Parquet files to S3 under `<env>/landing/<source>/` folders
- Creates folders for each data source:
  - `edu`: Education-related datasets
  - `wdi`: World Development Indicators

#### Transformation Process (SQLMesh)
- Reads Parquet files from the S3 landing zone
- Performs data transformations using SQLMesh models
- Stores transformed data in local DuckDB database (`osaa_mvp.db`)
- Outputs transformed data to S3 under `<env>/transformed/<schema>/` folders

#### Upload Process (`upload/run.py`)
- Takes transformed data from the DuckDB database
- Uploads final transformed datasets to S3 under `<env>/transformed/` directory
- Currently focuses on uploading WDI (World Development Indicators) transformed data

### Environment Configuration
The pipeline supports different execution environments controlled through environment variables:
- `TARGET`: Controls S3 paths and SQLMesh environments (`dev`, `int`, `prod`). Default is `dev`
- `USERNAME`: Used for S3 paths in `dev` environment. Default is `default`

S3 path examples:
- Production: `prod/`
- Integration: `int/`
- Development: `dev_<username>/`

## Getting Started

### User Profiles
1. **Data Analyst/Policy Analyst** - You are a user of the data, and you need to run the data pipeline in your own development environment. Link to Instructions
2. **Data Engineer** - You are a project maintainer and you need to make edits to the pipeline process. You will interact directly with the code and cli tools, in addition to the compiling and running the container. Link to Instructions.


### Process for Data Analysts
Please follow this section if your role is Data Analysts. Please see the definition of [Data Analyst](#user-profiles) above for more details.

#### Prerequisites
- AWS account with S3 access
- Docker Desktop - for building and running the pipeline in a containerized environment
- Git cli

--- To be completed ---

### Process for Data Engineers
Please follow this section if your role is Data Engineer. Please see the definition of [Data Engineer](#user-profiles) above for more details.

#### Prerequisites
- Python 3.9-3.11 (3.12 not supported)
- AWS account with S3 access
- Just (command runner) - for running predefined commands
- Docker Desktop (optional) - for running the pipeline in a containerized environment
- Git cli

#### Setup
1. Clone the repository:
   ```
   git clone https://github.com/UN-OSAA/osaa-mvp.git
   cd osaa-mvp
   ```
2. Set up your environment variables:
   ```bash
   # Copy the example environment file
   cp .env.example .env
   ```
   Edit .env with your AWS credentials

   Required variables:
   ```bash
   # AWS Credentials
   AWS_ACCESS_KEY_ID=<your-aws-access-key>
   AWS_SECRET_ACCESS_KEY=<your-aws-secret-key>
   AWS_DEFAULT_REGION=<your-aws-region>

   # S3 Configuration
   S3_BUCKET_NAME=osaa-mvp
   TARGET=dev
   USERNAME=<your-name>

   # SQLMesh Configuration
   # Postgres for the shared state db
   POSTGRES_HOST=your_host
   POSTGRES_PORT=your_port
   POSTGRES_USER=your_user
   POSTGRES_PASSWORD=your_pass
   POSTGRES_DATABASE=your_database
   # Specify Gateway
   GATEWAY=your_gateway
   ```

   These credentials are used for:
   - S3 access for data storage
   - DuckDB S3 integration
   - Local development and Docker execution

#### Running the Pipeline

##### Invoke Using Docker
The simplest method to run the pipeline commands is with Docker. Once built, users can use the container to run any of the "just" workflows configured with the project.

See the steps below to build and run the container

1. Build the Docker image:
   ```
   docker build -t osaa-mvp .
   ```

### Environment Configuration
The pipeline supports different execution environments controlled through environment variables.
The main variables that control behavior are:
- TARGET: Controls both S3 paths and SQLMesh environments (`dev`, `int`, `prod`). Default is `dev`
- USERNAME: Used for S3 paths in `dev` environment. Default is `default`
- GATEWAY: Used to specify whether to use a managed postgres database for SQLMesh's state connection. Possible values are `local` or `shared_state`. Default is `local`, which uses duckdb as the state connection.

####  Standard Execution
Run the complete pipeline with default settings:
```bash
docker compose up
```
By default, the container will run the entire elt process `just elt`.

To run the SQLMesh UI:
```bash
docker compose --profile ui up ui
```
Note: The UI must be run using the profile flag as shown above.

###### One-off Pipeline Components
You can run specific parts of the pipeline:
1. Run only the ingestion process:
```bash
docker compose run --rm pipeline ingest
```

2. Run only the transform process:
```bash
docker compose run --rm pipeline transform
```

3. Run only the upload process:
```bash
docker compose run --rm pipeline upload
```

###### Environment Variable Control at Runtime
You can override environment variables when running specific commands without modifying your `.env` file. This is useful for:
- Testing different environments
- Running as different users
- Temporary configuration changes

1. Example: Run as a specific user in development:
Uses `username` for S3 paths:
```bash
docker compose run --rm -e TARGET=dev -e USERNAME=johndoe pipeline etl
```

This creates S3 paths like: `s3://osaa-poc/dev_johndoe/landing/`

2. Run as integration environment:
Uses `int` for S3 paths:
```bash
docker compose run --rm -e TARGET=int pipeline etl
```

This creates S3 paths like: `s3://osaa-poc/int/landing/`

3. Run as production environment:
Uses `prod` for S3 paths:
```bash
docker compose run --rm -e TARGET=prod pipeline etl
```

This creates S3 paths like: `s3://osaa-poc/prod/landing/`

4. Run using the shared postgres database as SQLMesh's state connection:
```bash
docker compose run --rm -e TARGET=dev -e USERNAME=johndoe -e GATEWAY=shared_state pipeline etl
```

If you specified your gateway in `.env` file to use a shared database for SQLMesh's state.

##### Invoke Using cli tools and the `justfile`

Alternatively, run the process by installing all the required libraries and invoking the `just` workflows.

1. Check if `just` is installed:
   ```
   just --version
   ```
   If `just` is not installed, follow the instructions below to install it:

   - On macOS, you can use Homebrew:
     ```
     brew install just
     ```

   - On Linux, you can use the package manager for your distribution. For example, on Ubuntu:
     ```
     sudo apt install just
     ```

   - On Windows, you can use Scoop:
     ```
     scoop install just
     ```

2. Install dependencies using `just`:
   ```
   just install
   ```

3. Run the desired process using the appropriate `just` command. You can see all commands by running `just --list`

Common `just` commands:

```bash
just ingest    # Run the ingestion process
just transform # Run the SQLMesh transformations
just upload    # Run the upload process
just etl       # Run the complete pipeline (ingest → transform → upload)
```


## System Architecture
The system architecture diagram can be found in [system_architecture.md](system_architecture.md)


## Key Technologies

- **Ibis**: A Python framework for data analysis, used to write expressive and portable data transformations. It provides a high-level abstraction over SQL databases like DuckDB, allowing for cleaner, more Pythonic data manipulation.
- **DuckDB**: A highly performant in-memory SQL engine for analytical queries, used for efficient data processing and querying, in order to process, convert, and interact with Parquet files and S3.
- **Parquet**: A columnar storage file format, used for efficient data storage and retrieval. Used as the core format for storing processed data.
- **SQLMesh**: A SQL-based data management tool, used to manage the SQLMesh models and transformations.
- **S3**: Amazon Simple Storage Service, used as the cloud storage solution for the data lake, storing both raw (landing folder) and processed (staging folder) data.

## Project File Structure
The project repo consists of several key components:
1. The sqlMesh project containing all the transformation
2. Files necessary to build and run the code as a Docker container, including the dockerfile and docker-compose.yml
3. Files necessary to run the code in local dev environments, such as the .env_example and requirements.txt files

Please see details about each location below:

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
│   └── osaa_mvp.db            # DuckDB database for SQLMesh transformations
├── src/
│   └── pipeline/             # Core pipeline code
│       ├── ingest/           # Handles data ingestion from local raw csv to S3 parquet
│       ├── upload/           # Handles DuckDB transformed data upload to S3
│       ├── catalog.py        # Defines data catalog interactions
│       ├── config.py         # Stores configuration details (e.g., paths, S3 settings)
│       ├── utils.py          # Utility functions
├── .env_example              # Environment variables configuration (template file)
├── dockerfile                # Docker container definition
├── docker-compose.yml        # Docker services and environment setup
├── entrypoint.sh             # Docker container entry point script
├── justfile                  # Automates common tasks (installation, running pipelines) for local execution w/o Docker
└── requirements.txt          # Python package dependencies
```

#### S3 Folder Structure

```
s3://osaa-mvp/                           # Base bucket
│
├── dev_{username}/                      # For dev environment (e.g., johndoe/)
│   ├── landing/                         # Raw data landing zone
│   │   ├── edu/                         # Education data
│   │   │   ├── SDG_LABEL.parquet
│   │   │   ├── OPRI_DATA_NATIONAL.parquet
│   │   │   ├── SDG_DATA_NATIONAL.parquet
│   │   │   └── OPRI_LABEL.parquet
│   │   └── wdi/                         # World Development Indicators
│   │       ├── WDICSV.parquet
│   │       └── WDISeries.parquet
│   │
│   ├── transformed/                     # Transformed data
│   │   └── wdi/
│   │       └── wdi_transformed.parquet
│
├── int/                                 # Integration environment (CICD)
│   ├── landing/
│   ├── transformed/
│   └── staging/
│
└── prod/                                # Production environment
    ├── landing/
    ├── transformed/
    └── staging/
```



## Next Steps

The next phase of this project will focus on experimenting with different visualization layers to effectively present the processed data. This may include:

- Include a Motherduck destination in the etl pipeline
- Integrate the use of:
    - Iceberg tables for better cataloguing
    - Hamilton for orchestration
    - Open Lineage for data lineage
- Integration with BI tools like Tableau or Power BI
- Experimentation with code-based data app/report/dashboard development using Quarto, Evidence, Marimo and Observable Framework.
- Exploration of data science notebooks for advanced analytics, like Marimo, Quarto, Hex and Deepnote.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

Mirian Lima (Project Sponsor) - mirian.lima@un.org
Stephen Sciortino (Principal Engineer, Consultant) - stephen.sciortino@un.org; stephen@databasetycoon.com
Project Link: [https://github.com/UN-OSAA/osaa-mvp.git](https://github.com/UN-OSAA/osaa-mvp.git)


## Acknowledgement

This project was **heavily inspired by** the work of [Cody Peterson](https://github.com/lostmygithubaccount), specifically the [ibis-analytics](https://github.com/ibis-project/ibis-analytics) repository. While the initial direction and structure of the project were derived from Cody’s original work, significant modifications and expansions have been made to fit the needs and goals of this project, resulting in a codebase that diverges substantially from the original implementation.
