FROM mcr.microsoft.com/playwright/python:v1.55.0-jammy

WORKDIR /app

# Install cron
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir requests beautifulsoup4 pandas openpyxl playwright

# Install Playwright browsers (IMPORTANT: Install for root user too)
RUN playwright install chromium
RUN playwright install-deps chromium

# Copy application files
COPY casino_scraper.py .
COPY web_parser.py .
COPY jackpot_parser.py .
COPY entrypoint.sh .
COPY cleanup.sh .
COPY calendar_generator.py .
COPY email_script.py .

# Copy environment file
COPY .env.ren3 .

# Make scripts executable
RUN chmod +x entrypoint.sh cleanup.sh calendar_generator.py email_script.py jackpot_parser.py casino_scraper.py web_parser.py

# Create directories
RUN mkdir -p /app/output /app/logs /app/archive /app/processed /app/final_output

# Set timezone
ENV TZ=Asia/Manila
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Set Playwright environment variables
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=0

# Health check
HEALTHCHECK --interval=1h --timeout=10s --start-period=5s --retries=3 \
    CMD test -d /app/output || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]