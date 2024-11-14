#!/bin/bash
py -3.7 -W ignore ../project/etl_pipeline.py

# As I have multiple python environments so only environement with python 3.7 contains required packages of python for this project.
# So I have to run pipeline.py file using py -3.7 instead of python3
# with -W ignore I am supressing warnings from python
# I have created a requirement.txt file with all the required packages need to be installed in python using pip before running pipeline