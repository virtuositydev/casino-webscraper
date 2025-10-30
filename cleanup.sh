#!/bin/bash

# Cleanup script for old data
OUTPUT_DIR="/app/output"
ARCHIVE_DIR="/app/archive"
LOGS_DIR="/app/logs"

echo "Starting cleanup at $(date)"

# Create archive directory if it doesn't exist
mkdir -p "$ARCHIVE_DIR"

# Move promo folders older than 7 days to archive
echo "Moving folders older than 7 days to archive..."
find "$OUTPUT_DIR" -maxdepth 1 -type d -name "promo_*" -mtime +7 -exec mv {} "$ARCHIVE_DIR/" \;

# Compress archives older than 30 days
echo "Compressing archives older than 30 days..."
find "$ARCHIVE_DIR" -maxdepth 1 -type d -name "promo_*" -mtime +30 | while read dir; do
    if [ -d "$dir" ]; then
        tar -czf "${dir}.tar.gz" -C "$(dirname "$dir")" "$(basename "$dir")"
        rm -rf "$dir"
        echo "Compressed: $(basename "$dir")"
    fi
done

# Delete compressed archives older than 90 days
echo "Deleting archives older than 90 days..."
find "$ARCHIVE_DIR" -name "promo_*.tar.gz" -mtime +90 -delete

# Delete logs older than 30 days
echo "Deleting logs older than 30 days..."
find "$LOGS_DIR" -name "scraper_*.log" -mtime +30 -delete

echo "Cleanup completed at $(date)"
echo "---"