# Dockerfile for Producer service
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Install Playwright and dependencies
RUN pip install --no-cache-dir playwright && \
    playwright install chromium && \
    playwright install-deps

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY shared/ /app/shared/
COPY producer/ /app/producer/

# Create non-root user
RUN useradd -m -u 1000 producer
USER producer


# Run producer
ENTRYPOINT ["python", "-m", "producer.main"]
