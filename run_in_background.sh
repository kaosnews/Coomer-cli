#!/bin/bash

# Check if URL is provided
if [ -z "$1" ]; then
    echo "Error: URL is required"
    echo "Usage: $0 <url>"
    exit 1
fi

# Get the URL from the first argument
URL="$1"

# Create a valid filename from the URL
LOGFILE=$(echo "$URL" | sed 's/[^a-zA-Z0-9]/%/g').LOG

# Create a temporary file for processing
TEMPFILE=$(mktemp)

# Run the command and capture output to temp file
python3 coomer.py "$URL" -d ./ -sv -t all -e -c 15 -fn 1 > "$TEMPFILE" 2>&1 &
PID=$!

# Start a background process to monitor the temp file and add timestamps
(
    # Wait for the temp file to be created
    while [ ! -f "$TEMPFILE" ]; do
        sleep 0.1
    done
    
    # Use tail to follow the file and add timestamps
    tail -f "$TEMPFILE" | while read -r line; do
        echo "$(date '+%I:%M%p %m/%d/%y') | $line" >> "$LOGFILE"
    done
    
    # Clean up temp file when the main process finishes
    wait $PID
    rm "$TEMPFILE"
) &

# Print confirmation message
echo "Started downloading $URL"
echo "Logs are being saved to $LOGFILE with timestamps"