import pandas as pd

def auto_validation(data):
    validation_report = {}   
    missing = data.isnull().sum()
    validation_report['missing_values'] = missing[missing > 0].to_dict()
    validation_report['data_types'] = data.dtypes.apply(lambda x: x.name).to_dict()
    validation_report['unique_values'] = {col: data[col].nunique() for col in data.columns}
    validation_report['data_shape'] = data.shape   
    validation_report['date_validation'] = {
        col: "Invalid Date (Feb 30 or 31) Found" 
        for col in data.columns 
        if data[col].astype(str).str.contains(r'-02-(?:30|31)', regex=True).any()
    }
    validation_report['duplicate_rows'] = data.duplicated().sum()
    return validation_report