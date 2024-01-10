import pandas as pd
import numpy as np


class DataWrangler():

    def __init__(self,config_file,basedir):
        self.config_file = config_file
        self.basedir = basedir
        #get general specs for each transformation, to make it easier to iterate
        self.specs_birthdate_to_age = []
        self.specs_hot_encoding = []
        self.specs_fill_empty_values = []
        for transform in config_file['transforms']: 
            if transform['transform'] == 'birthdate_to_age':
                self.specs_birthdate_to_age=transform['fields']
            elif transform['transform'] == 'hot_encoding':
                self.specs_hot_encoding=transform['fields']
            elif transform['transform'] == 'fill_empty_values':
                self.specs_fill_empty_values=transform['fields']        

    #Set of validations before running transformations. This allows to catch most common possible mistakes in the config file or dataset and show the user a useful error. This prevents running 1 or 2 transformations and then failing, catching any mistake beforehand, saving time and resources.
    def validate_transformations(self,input_dataset):
        #birthdate_to_age checks
        #check that field exists and is in fact a date and can be cast to date
        if self.specs_birthdate_to_age != []:
            for field in self.specs_birthdate_to_age:
                old_field = field['field']
                try:
                    input_dataset[old_field].astype('datetime64[ns]')
                except:
                    raise ValueError('Field "'+old_field+'" does not exist or has an incorrect format (it is not a date).')

        #hot_encoding checks
        #check if field actually exists in dataset, if not, abort
        if self.specs_hot_encoding != []:
            for field in self.specs_hot_encoding:
                if field not in list(input_dataset.columns):
                    raise ValueError('Field "'+field+'" does not exist in dataset, please add the field or correct the config file.')

        #fill_empty_values checks
        #check if field actually exists in dataset, if not, abort. Also, check if it will be possible to apply median, mean or mode calculations. We need to have at least one record and this record has to be numeric for mean and median.
        if self.specs_fill_empty_values != []:
            for field in self.specs_fill_empty_values:
                old_field = field['field']
                fill_type = field['value']
                if old_field not in list(input_dataset.columns):
                    raise ValueError('Field "'+old_field+'" does not exist in dataset, please add the field or correct the config file.')
                #check that field is not 100% empty as calcs will fail
                if (fill_type in ['median','mean','mode'] and pd.isnull(input_dataset[old_field]).all()):
                    raise ValueError('Cannot calculate mean, median or mode of field "'+old_field+'" because field only has empty values. Consider filling with a constant.')
                #only check if field has numeric data for median and mean
                if fill_type in ['median','mean']:
                    try:
                        input_dataset[old_field].median()
                    except:
                        raise ValueError('Cannot calculate mean or median of field "'+old_field+'". Check that data is numeric type or consider filling with a constant.')    

        print('Transformations validated successfully. \n')
        

    #birthdate_to_age transformation
    def birthdate_to_age(self,input_dataset):
        if self.specs_birthdate_to_age != []:
            now = pd.Timestamp('now') 
            for field in self.specs_birthdate_to_age:
                old_field = field['field']
                new_field = field['new_field']
                input_dataset[new_field] = ((now - pd.to_datetime(input_dataset[old_field]))/ pd.Timedelta('365 days')).astype(int)
                input_dataset.drop(old_field,axis=1,inplace=True)

        return input_dataset
        #Even though this is not a requirement for now, we could add an extra step to this function (or to the validator) to check that the new field name does not already exist in the dataset as it will be overwritten. We could either let user know and overwrite it anyway or interrupt the program


    #hot_encoding transformation
    def hot_encoding(self,input_dataset):
        if self.specs_hot_encoding != []:
            for field in self.specs_hot_encoding:
                old_field = field
                input_dataset[old_field].fillna('NULL',inplace=True) #replace nans with the string 'NULL' to standardize
                input_dataset[old_field] = input_dataset[old_field].astype(str) #cast whole column to string to standardize
                codes = pd.unique(input_dataset[old_field]).tolist()
                for code in codes:
                    input_dataset['is_'+code] = np.where(input_dataset[old_field]==code, 1, 0)
                input_dataset.drop(old_field,axis=1,inplace=True)

        return input_dataset
        #Even though this is not a requirement for now, we could add an extra step to this function (or to the validator) to check that the new field name does not already exist in the dataset as it will be overwritten. We could either let user know and overwrite it anyway or interrupt the program


    #fill_empty_values transformation
    def fill_empty_values(self,input_dataset):
        if self.specs_fill_empty_values != []:
            for field in self.specs_fill_empty_values:
                old_field = field['field']
                fill_type = field['value']
                #Originally was going to use match-case statement. However, as this was implemented in Python 3.10 it could lead to incompatibilities with some users.
                if fill_type == "mean":
                    col_mean = input_dataset[old_field].mean()
                    input_dataset[old_field].fillna(col_mean,inplace=True)
                elif fill_type == "median":
                    col_median = input_dataset[old_field].median()
                    input_dataset[old_field].fillna(col_median,inplace=True)     
                elif fill_type == "mode":
                    col_mode = input_dataset[old_field].mode()[0]
                    input_dataset[old_field].fillna(col_mode,inplace=True)                
                else:
                    input_dataset[old_field].fillna(fill_type,inplace=True)      

        return input_dataset  