#!/bin/bash

echo "Running ETL Pipeline Test..."

# Run the Python test script
py -3.7 -W ignore ../project/etl_pipeline_test.py

# Check the exit status
if [ $? -eq 0 ]; then
    echo "All tests passed successfully!"
    # exit 0  ## commented out to avoid closing terminal, after all tests passed!
else
    echo "Tests failed!"
    # exit 1 ## commented out to avoid closing terminal, to know read what went wrong
fi


