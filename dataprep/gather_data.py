import pandas as pd
import requests
from csv import reader
import os
import argparse
import json
from azureml.core import Run, Workspace, Datastore, Dataset, Experiment

parser = argparse.ArgumentParser("Aggregate Data")

parser.add_argument("--target_path", type=str, help="Target path for data upload")

args = parser.parse_args()

#Get current run
current_run = Run.get_context()
#Get associated AML workspace
ws = current_run.experiment.workspace
#Get default datastore - used to upload data
datastore = ws.get_default_datastore()

#Target path is passed as a variable argument (can be timestamped)
target_path = args.target_path

#Pull sample dataset and add to pandas dataframe
r = requests.get('https://dprepdata.blob.core.windows.net/demo/Titanic.csv')
rows = r.text.split('\r\n')
formatted_rows = []
for row in rows:
    read = reader([row], skipinitialspace=True)
    vals = [x for x in read]
    formatted_rows.append(vals[0])
df = pd.DataFrame(formatted_rows[1:], columns=formatted_rows[0])

#Partition source dataframe based on values in 'Embarked' column
df_s = df[df['Embarked']=='S']
df_c = df[df['Embarked']=='C']
df_q = df[df['Embarked']=='Q']

#Write partitioned dataframes to files in processed data
df_s.to_csv('./processed/sourcedata_s.csv', index=False)
df_c.to_csv('./processed/sourcedata_c.csv', index=False)
df_q.to_csv('./processed/sourcedata_q.csv', index=False)

#Upload processed directory to default datastore
datastore.upload(src_dir='./processed', target_path=target_path, overwrite=True)


from azureml.core.dataset import Dataset

# Create file datasets
ds_train = Dataset.File.from_files(path=datastore.path(target_path), validate=False)

# Register the file datasets
dataset_name = 'etf_data'
train_dataset_name = dataset_name + '_train'
ds_train.register(ws, train_dataset_name, create_new_version=True)
