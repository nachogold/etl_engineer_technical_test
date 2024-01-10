import sys
import os 

#Set general directory parameters
basedir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__))) 

#Even though this is not a good practice, there are some non-base package dependencies to use pandas to_parquet, so I added a function to install these packages if they are not already installed.
#A proper way to do this would be to actually package the python app, although I believe this exceeds the scope of the test.
from modules.install_reqs import *
check_requirements()

from modules.data_extract_load import *
from modules.data_wrangler import *
import json
import time
import traceback

exception_sleep_secs = 15

json_file = input('Please input json configuration file name including extension (for example "config.json"). This file should be in the same directory as the transform_dataset.py file: ')

try:
    config_file = json.load(open(os.path.join(basedir, json_file)))
except:
    print('\n Error loading json configuration file. Check that you wrote the correct file name or that the file is not damaged.')
    time.sleep(exception_sleep_secs)
    sys.exit()


#Instantiate extractor/loader class and validate input/output 
try:
    dataExtractorLoader = DataExtractLoad(config_file,basedir)
    dataExtractorLoader.validate_input_output()
except:
    print('Error validating input/output.\n\n')
    print('Specific error message below:')
    traceback.print_exc()
    time.sleep(exception_sleep_secs)
    sys.exit()

#Read input dataset
try:
    input_dataset = dataExtractorLoader.input_to_dataframe()
except:
    print('Error reading input dataset.\n\n')
    print('Specific error message below:')
    traceback.print_exc()
    time.sleep(exception_sleep_secs)
    sys.exit()

try:
    #Instantiate transformation class
    trfm = DataWrangler(config_file,basedir)
    #Validate transformation requirements before actually running pipeline
    trfm.validate_transformations(input_dataset)
except:
    print('Error validating transformations.\n\n')
    print('Specific error message below:')
    traceback.print_exc()
    time.sleep(exception_sleep_secs)
    sys.exit()

#Once we validate the transformation specs, run transformations
try:
    transformed_dataset = trfm.birthdate_to_age(input_dataset) 
    del input_dataset #delete input dataset to release memory as we don't need it anymore
except:
    print('Error performing birthdate_to_age transformations. Check that you are not using the same field for different transformations.\n\n')
    print('Specific error message below:')
    traceback.print_exc()
    time.sleep(exception_sleep_secs)
    sys.exit()

try:
    transformed_dataset = trfm.hot_encoding(transformed_dataset) 
except:
    print('Error performing hot_encoding transformations. Check that you are not using the same field for different transformations.\n\n')
    print('Specific error message below:')
    traceback.print_exc()
    time.sleep(exception_sleep_secs)
    sys.exit()

try:
    transformed_dataset = trfm.fill_empty_values(transformed_dataset) 
except:
    print('Error performing fill_empty_values transformations. Check that you are not using the same field for different transformations.\n\n')
    print('Specific error message below:')
    traceback.print_exc()
    time.sleep(exception_sleep_secs)
    sys.exit()

#Once the transformations ran successfully, output the dataset
try:
    dataExtractorLoader.dataframe_to_output(transformed_dataset)
except:
    print('Error writing output dataset.\n\n')
    print('Specific error message below:')
    traceback.print_exc()
    time.sleep(exception_sleep_secs)
    sys.exit()

print('ETL ran successfully.')
time.sleep(10)