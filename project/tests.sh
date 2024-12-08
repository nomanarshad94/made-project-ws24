#!/bin/bash

# echo "Running ETL Pipeline Test..."

# # Run the Python test script
# # py -3.7 -W ignore ../project/etl_pipeline_test.py
# py -3.7 -W ignore ../project/unit_tests.py
# py -3.7 -W ignore ../project/integration_tests.py


# # Check the exit status
# if [ $? -eq 0 ]; then
#     echo "All tests passed successfully!"
#     # exit 0  ## commented out to avoid closing terminal, after all tests passed!
# else
#     echo "Tests failed!"
#     # exit 1 ## commented out to avoid closing terminal, to know read what went wrong
# fi

# Improved test bash script:

echo "==============================="
echo "  Running ETL Pipeline Tests   "
echo "==============================="
# setting required variables
ALL_TESTS_PASSED=0
# Define Python executable and test files
PYTHON_EXEC="python3 -W ignore" # changing py -3.7 to python3 as py utility is only for windows and not available in unix.
UNIT_TESTS="./unit_tests.py"
INTEGRATION_TESTS="./integration_tests.py"

# Function to run tests
run_test() {
    local test_script=$1
    echo "----------------------------------"
    echo "Running: $test_script"
    echo "----------------------------------"
    $PYTHON_EXEC $test_script
    if [ $? -ne 0 ]; then
        echo "❌ Test failed: $test_script"
        echo "Stopping script execution due to a test failure."
    else
        ALL_TESTS_PASSED=1
        echo "✅ Test passed: $test_script"
    fi
}

# Run Unit Tests
run_test "$UNIT_TESTS"

# Run Integration Tests
run_test "$INTEGRATION_TESTS"


# Check the exit status of all tests
if [ $ALL_TESTS_PASSED -eq 1 ]; then
    echo "=================================="
    echo "  All tests passed successfully!  "
    echo "=================================="
    exit 0  ## Uncomment if you want to exit with success
else
    echo "=================================="
    echo "  Some test(s) failed to pass!    "
    echo "=================================="
    exit 1  ## Uncomment if you want to exit with failure
fi

# # Keep terminal open to review results
# read -p "Press Enter to exit..."


# As I have multiple python environments so only environement with python 3.7 contains required packages of python for this project.
# So I have to run *_tests.py file using py -3.7 instead of python3
# with -W ignore I am supressing warnings from python
# I have created a requirement.txt file with all the required packages need to be installed in python using pip before running pipeline
# lastly I am using kaggle api for dataset. kaggle api requires kaggle.json with valid username and api key to fetch the data. Due to security
# reasons I am not pushing api key and username to github repo. But to run the pipeline you have to have kaggle.json file in .kaggle folder under
# your user directory (~/.kaggle/kaggle.json on Linux and C:\Users\<Windows-username>\.kaggle\kaggle.json on Windows)

# I am running this in git bash with command: ". ./tests.sh", and running it from project directory itself in which tests.sh file exist. 
# So before running please change current directory to project folder of made project repo 