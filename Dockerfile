FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first — Docker caches this layer
# Only reinstalls if requirements.txt changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    --index-url https://pypi.org/simple/ \
    --trusted-host pypi.org \
    --trusted-host files.pythonhosted.org

# Copy application code
COPY app/ ./app/

# Create storage directories
RUN mkdir -p storage/uploads storage/outputs

# Expose API port
EXPOSE 8000

# Default command — runs the API server
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
