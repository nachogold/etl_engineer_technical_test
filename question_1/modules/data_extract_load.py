import os
import pandas as pd
import numpy as np

class DataExtractLoad():

    def __init__(self,config_file,basedir):
        self.config_file = config_file
        self.basedir = basedir
        #input config
        self.input_specs = os.path.join((config_file['source']['path']), (config_file['source']['dataset']+'.'+config_file['source']['format']))
        self.input_path = os.path.join(basedir,self.input_specs)
        self.input_format = config_file['source']['format'].lower()
        #output config
        self.output_format = config_file['sink']['format'].lower()
        self.output_directory = os.path.join(basedir,config_file['sink']['path'])
        self.output_path = os.path.join(basedir,(config_file['sink']['path']), (config_file['sink']['dataset']+'.'+config_file['sink']['format']))

    
    def validate_input_output(self):
        #input checks
        if os.path.exists(self.input_path) == False:
            raise ValueError('Input dataset does not exist or path is incorrect.')
        if self.input_format not in ['csv','jsonl','parquet']:
            raise ValueError('Input dataset format unsupported (has to be csv, jsonl or parquet).')
        #output checks
        if self.output_format not in ['csv','jsonl','parquet']:
            raise ValueError('Output dataset format unsupported (has to be csv, jsonl or parquet).')  

        print('Input/output validated successfully. \n')

    
    def input_to_dataframe(self):
        if self.input_format == 'csv':
            print('Reading CSV \n')
            input_dataset=pd.read_csv(self.input_path)   

        elif self.input_format == 'jsonl':
            print('Reading JSONL \n')
            input_dataset = pd.read_json(self.input_path,lines=True)
            input_dataset.replace('^\s*$', np.nan, regex=True, inplace=True) #missing values are read as an empty str, so we replace this with NaN to standardize

        elif self.input_format == 'parquet':
            print('Reading PARQUET \n')
            input_dataset=pd.read_parquet(self.input_path) 

        return input_dataset
    
    
    def dataframe_to_output(self,transformed_dataset):
        #create directory if it not exists
        if os.path.exists(self.output_directory) == False:
            os.makedirs(self.output_directory)

        if self.output_format == 'csv':
            print('Writing CSV \n')
            transformed_dataset.to_csv(self.output_path, index=False)  

        elif self.output_format == 'jsonl':
            print('Writing JSONL \n')
            transformed_dataset.to_json(self.output_path, orient='records', lines=True)  

        elif self.output_format == 'parquet':
            print('Writing PARQUET \n')
            transformed_dataset.to_parquet(self.output_path, index=False)  

        print(self.output_format+' file was written successfully. \n')
