
# Technical Test README #

## Test ##
QUESTION 1: Python
Implement a software that can read a dataset from a specific file path, apply some transformations to its fields and write the result into another path.
Requirements:
- Datasets can be read and written in CSV and at least another format: Parquet or JSONL (JSON Lines).
- The program has a library of transformations that can potentially be applied to the dataset, but on each execution we select which ones we want to apply to which columns.
- Anyone without programming skills should be able to execute the script. To do that, the user will provide a JSON configuration file. Your solution has to parse this configuration and generate the result based on it.
- The transformations that you need to implement are:
    - birthdate_to_age: Given a date, it computes the age of a person and creates a new column with the result.
    - hot_encoding: Given a categorical column with n possible values, it replaces it with n binary columns. For example, given the column “color” with 3 possible values: “blue”, “red” and “green”, we will create 3 columns: “is_blue”, “is_red” and “is_green”, that will be 1 or 0 depending of the value of the original column.
    - fill_empty_values: It replaces the empty values of a column with another value. The value to be replaced with is passed as a parameter and it can be a constant or one of the following keywords: “mean”, “median” or “mode”. If we pass one of these keywords, we replace the empty values with the mean / median / mode of the rest of the elements of the column.
Example: To test your code, we provide you a small dataset called bookings.csv and the json config file (clean_bookings.json) that should be passed to your program as an argument to process it and generate a clean one

## Remarks ##

For the python ETL app I set up the following structure:
- Main directory (`question_1`): where we have our main app file plus the json config file. Both files have to be in the same directory for the app to work. If this is not the case, the user will be alerted of this and requested to have both files in the same directory.
    - `transform_dataset.py`: this is our main app file that will call all necessary methods to run the ETL pipeline.
    - `clean_bookings.json`: this is the json config file that the user has to provide. When running `transform_dataset.py` the user will be prompted to enter this file's name.
    - `modules` directory
        - `data_extract_load.py`: contains class and methods to read and write datasets. The app can read and write csv, jsonl and parquet. Additionally, it contains some basic validations that are run before the transformations are performed.
        - `data_wrangler.py`: contains class and methods to run all necessary transformations. Additionally, it contains some basic validations that are run before the transformations are performed.
        - `install_reqs.py`: basic helper module to check and install some required packages.
    - `inputs` directory: this is a placeholder input directory from where the inputs will be read. However, the user is free to change this from the json config file to provide their own input directory (which needs to be inside the main directory).

Some remarks:
- The app runs some validations before running the transformations to check that they will actually be able to be run. This has the main goal of avoiding the scenario when one transformation is run and the next one fails, thus saving time and resources as we catch the error beforehand. 
    - Even though I tried to think of the most common errors that we can have with each transformation, there could be other untracked exceptions that will be caught without a specific error message to the user.
- The app is not meant to apply more than one transformation to one specific field. Original fields are dropped after being transformed to release memory (except in the fill_empty_values transformation), so they may not be available for a second transformation. This can be fixed by leaving all column deletions for the end of the pipeline.
- The app is also not meant to chain transformations, for example: birthdate_to_age and then use hot_encoding on the result of the first transformation. This is because, when validations are run in the beginning, the new transformed field name does not exist yet in the dataset and this will be caught as an error. Again, this could be easily fixed by changing how the validations work.
- The transformations are run in a set order and, for now, cannot be changed by the user. This functionality can be added by taking into account in what order the transformations appear in the json file and calling the transformation methods in this same order.

