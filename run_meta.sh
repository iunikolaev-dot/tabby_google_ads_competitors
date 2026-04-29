#!/bin/bash
# Manual run of the Meta Ads Library scraper
# Usage: APIFY_TOKEN='your_token' ./run_meta.sh

cd "$(dirname "$0")"
export PYTHONPATH="/Users/ilyanikolaev/Library/Python/3.9/lib/python/site-packages:$PYTHONPATH"
export PATH="/Users/ilyanikolaev/google-cloud-sdk/bin:$PATH"

# Load .env file if it exists
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

if [ -z "$APIFY_TOKEN" ]; then
    echo "ERROR: APIFY_TOKEN is not set."
    echo "Set it in .env file or pass directly: APIFY_TOKEN='your_token' ./run_meta.sh"
    exit 1
fi

echo "Starting Meta Ads Library scraper..."
echo "Log file: meta_scraper.log"
python3 meta_scraper.py 2>&1 | tee -a meta_scraper.log
echo "Done!"
