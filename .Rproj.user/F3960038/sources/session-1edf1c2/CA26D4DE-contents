#!/usr/bin/env bash

# Check for correct number of arguments
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <path_to_cwa_files>"
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

# Process each split file
./resample_and_extract_initial_features.R "$cwa_path"

# Merge processed files from temp and write to the output directory
./merge_and_extract_remaining_features.R "$temp_path" "$output_path"

# Calculate stats
./calculate_stats.R "$output_path" "$stats_path"

# Remove processed split files to prevent reuse in the next loop
rm -rf "${temp_path}"
