FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create a directory for logs
RUN mkdir -p /app/logs

# Create an empty database file
RUN touch /app/smart_home_energy.db

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Expose the API port
EXPOSE 8000

# Create a startup script
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Run the startup script
CMD ["/app/start.sh"]