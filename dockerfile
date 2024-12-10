FROM python:3.11-slim

WORKDIR /app

# Create necessary directories
RUN mkdir -p datalake/raw sqlMesh


# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

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
