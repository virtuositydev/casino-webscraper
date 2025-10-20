#!/bin/bash

set -e

# Load environment variables for Ren3
export $(cat /app/.env.ren3 | grep -v '^#' | xargs)

# Create cron job with separate schedules
echo "Setting up cron jobs..."
cat > /etc/cron.d/scraper << 'EOF'
# Scraper runs at 8 AM
0 8 * * * cd /app && python3 casino_scraper.py >> /app/logs/scraper_$(date +\%Y\%m\%d_\%H\%M\%S).log 2>&1

# Processor runs at 9 AM (processes latest folder)
0 9 * * * cd /app && python3 /app/run_processor.py >> /app/logs/processor_$(date +\%Y\%m\%d_\%H\%M\%S).log 2>&1

# Cleanup at 2 AM
0 2 * * * /app/cleanup.sh >> /app/logs/cleanup_$(date +\%Y\%m\%d).log 2>&1
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