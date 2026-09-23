FROM python:3.11-slim

WORKDIR /app

# Install system runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    bash \
    gcc \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and scripts
COPY . .

# Ensure scripts are executable
RUN chmod +x scripts/*.sh 2>/dev/null || true

# Set Python path to /app for clean module imports
ENV PYTHONPATH=/app

CMD ["python", "-m", "src.data_generator.main"]
