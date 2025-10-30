#!/bin/bash

set -e

# Load environment variables for Ren3
if [ -f /app/.env.ren3 ]; then
    export $(cat /app/.env.ren3 | grep -v '^#' | xargs)
fi

# Create cron job with FULL PYTHON PATH
echo "Setting up cron job..."
cat > /etc/cron.d/scraper << 'EOF'
# Set PATH and PYTHONPATH for cron
PATH=/usr/local/bin:/usr/bin:/bin
PYTHONPATH=/usr/local/lib/python3.12/site-packages:/usr/lib/python3/dist-packages

# Load environment variables
SHELL=/bin/bash

# Run scraper at 8 AM, then process with agent
0 11 * * * root cd /app && /usr/bin/python3 casino_scraper.py >> /app/logs/scraper_$(date +\%Y\%m\%d_\%H\%M\%S).log 2>&1 && sleep 10 && /usr/bin/python3 /app/web_parser.py >> /app/logs/processor_$(date +\%Y\%m\%d_\%H\%M\%S).log 2>&1

# Cleanup old data at 2 AM
0 2 * * * root /app/cleanup.sh >> /app/logs/cleanup_$(date +\%Y\%m\%d).log 2>&1
EOF

# Set permissions
chmod 0644 /etc/cron.d/scraper

# Apply cron job
crontab /etc/cron.d/scraper

# Display installed cron jobs
echo "Installed cron jobs:"
crontab -l

# Start cron in foreground
echo "Starting cron..."
cron

# Keep container running
echo "Cron started. Container will keep running..."
tail -f /dev/null
