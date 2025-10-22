#!/bin/bash

set -e

# Create cron job with FULL PYTHON PATH
echo "Setting up cron job..."
cat > /etc/cron.d/scraper << 'EOF'
# Set PATH and PYTHONPATH for cron
PATH=/usr/local/bin:/usr/bin:/bin
PYTHONPATH=/usr/local/lib/python3.12/site-packages:/usr/lib/python3/dist-packages

# Load environment variables
SHELL=/bin/bash

# Run scraper at 8 AM, then process with agent
0 8 * * * root cd /app && /usr/bin/python3 casino_scraper.py >> /app/logs/scraper_$(date +\%Y\%m\%d_\%H\%M\%S).log 2>&1 

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