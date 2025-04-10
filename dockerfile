FROM python:3.11-slim

WORKDIR /app

# Create necessary directories
RUN mkdir -p datalake/raw sqlMesh

# Install git for GitHub dependencies
RUN apt-get update && \
    apt-get install -y git && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install uv
RUN uv venv

# Make sure boto3 is explicitly installed in the virtual environment
RUN uv pip install --no-cache-dir -r requirements.txt
RUN uv pip install --no-cache-dir boto3>=1.35.0

# Copy only necessary files
COPY src/ src/
COPY sqlMesh/ sqlMesh/
COPY entrypoint.sh .
COPY data/ data/

# Set PYTHONPATH
ENV PYTHONPATH=/app/src

# Make scripts executable
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["etl"]
