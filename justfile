# Automatically load environment variables from .env file
set dotenv-load

# Project-specific variables
package := "osaa-mvp"
venv_dir := ".venv"
requirements_file := "requirements.txt"
target := env_var_or_default("TARGET", "dev")
gateway := env_var_or_default("GATEWAY", "local")

# Include the src directory in PYTHONPATH
export PYTHONPATH := "src"

# Aliases for frequently used commands
alias fmt := format

# Display the list of recipes when no argument is passed
default:
    just --list

# Install runtime dependencies and set up virtual environment
install:
    @echo "Setting up {{venv_dir}} and dependencies..."
    @python -m venv {{venv_dir}}
    @. {{venv_dir}}/bin/activate
    @pip install --upgrade pip
    @pip install -r {{requirements_file}}
    @echo "Install complete!"

# Uninstall the package and clean up environment
uninstall:
    @echo "Uninstalling {{package}} and removing venv..."
    @pip uninstall -y {{package}}
    @rm -rf {{venv_dir}}
    @echo "Uninstall complete!"

# Format the codebase using ruff
format:
    @echo "Formatting the codebase using ruff..."
    @ruff format .

# Run Ingest pipeline with optional arguments for sources
ingest:
    @echo "Running the Ingest process..."
    @python -m pipeline.ingest.run

# Run SQLMesh transformations
transform:
    @echo "Running SQLMesh transformations..."
    @cd sqlMesh && sqlmesh --gateway {{gateway}} plan --auto-apply --include-unmodified --create-from prod --no-prompts {{target}}

# Run SQLMesh transformations in dry-run mode (no S3 uploads)
transform_dry_run:
    @echo "Running dry-run pipeline..."
    @export ENABLE_S3_UPLOAD=false
    @export RAW_DATA_DIR=data/raw
    @python -m pipeline.ingest.run
    @echo "Local ingestion complete"
    @cd sqlMesh && sqlmesh --gateway {{gateway}} plan --auto-apply --include-unmodified --create-from prod --no-prompts {{target}}
    @echo "Dry-run complete!"

# Run Upload pipeline with optional arguments for sources
upload:
    @echo "Running the Upload process"
    @python -m pipeline.upload.run

# Run the complete pipeline
etl: ingest transform upload
    @echo "Pipeline complete!"

# Open the project repository in the browser
repo:
    @echo "Opening the project repository in the browser..."
    @open https://github.com/UN-OSAA/osaa-mvp.git
