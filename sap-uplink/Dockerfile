# SAP Sailing Uplink Service Dockerfile

FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY src ./src

# Cursor state lives here (mounted as a volume in docker-compose)
VOLUME ["/data"]

# Run the service
CMD ["python", "-m", "src.main"]
