#!/bin/bash
cwd=$(pwd)
# Check if there is a command line argument
if [[ $# -eq 1 ]]; then
  # Change to the directory specified by the argument
  cd "$1" && make
else
  # Search for all build* directories in parent directory
  for dir in $(find .. -maxdepth 1 -type d -name "build*"); do
    # Change to directory and compile
    cd "$dir" && make
    # Return to original directory
    cd -
  done
fi
cd $cwd