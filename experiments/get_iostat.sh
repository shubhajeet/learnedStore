#!/bin/bash

DISK=sda

# Define input and output paths as command line arguments
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 input_path output_path"
  exit 1
fi

input=$1
output=$2

# Check if input file exists
if [ ! -f "$input" ]; then
  echo "Input file $input not found"
  exit 1
fi

# Generate plot using iostat-cli
iostat-cli --data "$input" --disk $DISK --fig-output "$output" plot

# Check if output file was generated successfully
if [ ! -f "$output" ]; then
  echo "Failed to generate output file $output"
  exit 1
fi

# Print output file type
file "$output"
