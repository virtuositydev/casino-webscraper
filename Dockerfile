# Dockerfile - Fixed version
FROM python:3.12-bookworm

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Add pandas and openpyxl for CSV processing
RUN pip install --no-cache-dir requests beautifulsoup4 playwright pandas openpyxl

# Install Playwright and its dependencies
RUN playwright install --with-deps chromium

# Copy application files
COPY casino_scraper.py .
COPY entrypoint.sh .

COPY cleanup.sh .
RUN chmod +x cleanup.sh

# Make entrypoint executable
RUN chmod +x entrypoint.sh

# Create directories
RUN mkdir -p /app/output /app/logs /app/archive

# Copy ren3 processor
COPY ren3_processor.py .
RUN chmod +x ren3_processor.py

# Set timezone
ENV TZ=Asia/Manila
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Health check
HEALTHCHECK --interval=1h --timeout=10s --start-period=5s --retries=3 \
    CMD test -d /app/output || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]