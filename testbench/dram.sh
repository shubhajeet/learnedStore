#!/bin/bash
# Get the total amount of memory in GB
total_mem=$(free -g | awk '/Mem:/ {print $2}')
echo "Total System DRAM: ${total_mem}GB"
# Calculate 75% of the total memory
mem_80_percent=$(echo "scale=2; ${total_mem} * 0.80" | bc)
echo "80% of DRAM: ${mem_80_percent}GB"
