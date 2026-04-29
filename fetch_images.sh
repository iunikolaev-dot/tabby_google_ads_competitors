#!/bin/bash
# Fetch missing image URLs for Google Ads on the dashboard.
# Run this when rate limit has reset (usually after 1-2 hours or next day).
# Safe to run multiple times — picks up where it left off.
#
# Usage:
#   ./fetch_images.sh          # fetch 250 images (safe batch)
#   ./fetch_images.sh 100      # fetch 100 images
#   ./fetch_images.sh all      # fetch all (auto-stops on rate limit)

cd "$(dirname "$0")"

LIMIT="${1:-250}"

if [ "$LIMIT" = "all" ]; then
    echo "=== Fetching ALL missing images (will auto-stop on rate limit) ==="
    python3 fetch_images.py --all
else
    echo "=== Fetching up to $LIMIT images ==="
    python3 fetch_images.py --limit "$LIMIT"
fi

# Deploy to Vercel if any images were fetched
if [ $? -eq 0 ]; then
    echo ""
    echo "=== Deploying to Vercel ==="
    npx vercel --prod
    echo ""
    echo "Done! Dashboard updated at https://tabby-ad-intelligence.vercel.app"
fi
