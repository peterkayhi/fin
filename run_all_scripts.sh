#!/bin/bash

# Iterate over each .py file in the current directory
for file in *.py; do
    # Ensure the file exists (handles the case where no .py files are found)
    if [[ -f "$file" ]]; then
        echo "Executing $file..."
        ~/Developer/fin/.venv/bin/python "$file"
    fi
done
