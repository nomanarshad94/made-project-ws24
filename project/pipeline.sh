#!/bin/bash
py -3.7 -W ignore ../project/etl_pipeline_improved.py

# As I have multiple python environments so only environement with python 3.7 contains required packages of python for this project.
# So I have to run pipeline.py file using py -3.7 instead of python3
# with -W ignore I am supressing warnings from python
# I have created a requirement.txt file with all the required packages need to be installed in python using pip before running pipeline
# lastly I am using kaggle api for dataset. kaggle api requires kaggle.json with valid username and api key to fetch the data. Due to security
# reasons I am not pushing api key and username to github repo. But to run the pipeline you have to have kaggle.json file in .kaggle folder under
# your user directory (~/.kaggle/kaggle.json on Linux and C:\Users\<Windows-username>\.kaggle\kaggle.json on Windows)