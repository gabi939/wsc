FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY shared/ /app/shared/
COPY consumer/ /app/consumer/

# Create non-root user
RUN useradd -m -u 1000 consumer
USER consumer

# Run consumer
ENTRYPOINT ["tail", "-f", "/dev/null"]
