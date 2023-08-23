#!/usr/bin/env bash

# Start timer
start_time=$(date +%s)

# Check for correct number of arguments
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <path/to/accelerometer/files>"
    exit 1
fi

# Define input and output paths
cwa_path="$1"
temp_path="${cwa_path}/temp"
output_path="${cwa_path}/output"
stats_path="${cwa_path}/stats"

# Create necessary directories if they don't exist
mkdir -p "$temp_path" 
mkdir -p "$output_path"
mkdir -p "$stats_path"

# Ask user for file type and wait for input
echo "Please choose a file type (cwa or wav):"
read file_type

if [[ "$file_type" != "wav" && "$file_type" != "cwa" ]]; then
    echo "Error: Invalid choice. Only 'cwa' or 'wav' are accepted."
    exit 1
fi

# Process each split file
./resample_and_extract_initial_features.R "$cwa_path" "$file_type"

# Merge processed files from temp and write to the output directory
./extract_remaining_features.R "$temp_path" "$output_path"

# Calculate stats
./calculate_stats.R "$output_path" "$stats_path"

# Cleanup
rm -rf "${temp_path}"
rm -rf "${output_path}"

# Stop timer and print elapsed time
end_time=$(date +%s)
elapsed_time=$(expr $end_time - $start_time)
echo "Total runtime: $elapsed_time seconds"