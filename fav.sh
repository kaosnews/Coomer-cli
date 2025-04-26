#!/bin/bash

# uhhh ask for the login stuff
echo "Enter credentials and site URL (format: username:password:site):"
read -r input

# try to split the input string by ':'
IFS=':' read -r username password site <<< "$input"

# check if we got all three parts, kinda
if [ -z "$username" ] || [ -z "$password" ] || [ -z "$site" ]; then
    echo "Error: Invalid input format. Use username:password:site"
    exit 1
fi

# make a filename that won't break things, hopefully
LOGFILE=$(echo "$site-favorites" | sed 's/[^a-zA-Z0-9]/%/g').LOG

# temp file for logs i guess
TEMPFILE=$(mktemp)

# run the python thing in the background, fingers crossed
python3 coomer.py --favorites --site "$site" --login --username "$username" --password "$password" -d /mnt/external/nrop/downloads -sv -t all -c 15 -fn 1 -n > "$TEMPFILE" 2>&1 &
PID=$!

# this part watches the temp file and adds timestamps, pretty neat huh
(
    # gotta wait for the temp file first...
    while [ ! -f "$TEMPFILE" ]; do
        sleep 0.1
    done

    # follow the file and slap a timestamp on each line
    tail -f "$TEMPFILE" | while IFS= read -r line; do
        # using IFS= prevents read from messing with leading/trailing whitespace
        echo "$(date '+%I:%M%p %m/%d/%y') | $line" >> "$LOGFILE"
    done

    # clean up the mess when the main thing is done
    wait $PID
    rm "$TEMPFILE"
) &

# tell the user it's running, maybe
echo "Started downloading favorites for site $site"
echo "Logs are being saved to $LOGFILE with timestamps"