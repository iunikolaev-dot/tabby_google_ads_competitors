#!/bin/bash
# Manual run of the Google Ads Transparency scraper
# Usage: ./run.sh

cd "$(dirname "$0")"
export PYTHONPATH="/Users/ilyanikolaev/Library/Python/3.9/lib/python/site-packages:$PYTHONPATH"
export PATH="/Users/ilyanikolaev/google-cloud-sdk/bin:$PATH"

echo "Starting Google Ads Transparency scraper..."
echo "Log file: scraper.log"
python3 scraper.py 2>&1 | tee -a scraper.log
echo "Done!"
