
#Write script to be run in pipeline. Should gather data, upload to AML associated datastore,
#create and register datasets

import pandas as pd
import requests
from csv import reader
import os
import argparse
import json
from azureml.core import Run, Workspace, Datastore, Dataset, Experiment
import snowflake.connector
import os
import shutil

def split_data(data_path, time_column_name, split_date):

    train_data_path = os.path.join(data_path, "upload_train_data")
    inference_data_path = os.path.join(data_path, "upload_inference_data")
    os.makedirs(train_data_path, exist_ok=True)
    os.makedirs(inference_data_path, exist_ok=True)

    files_list = [os.path.join(path, f) for path, _, files in os.walk(data_path) for f in files
                  if path not in (train_data_path, inference_data_path)]

    for file in files_list:
        file_name = os.path.basename(file)
        file_extension = os.path.splitext(file_name)[1].lower()
        df = read_file(file, file_extension)

        before_split_date = df[time_column_name] < split_date
        train_df, inference_df = df[before_split_date], df[~before_split_date]

        write_file(train_df, os.path.join(train_data_path, file_name), file_extension)
        write_file(inference_df, os.path.join(inference_data_path, file_name), file_extension)

    return train_data_path, inference_data_path

def read_file(path, extension):
    if extension == ".parquet":
        return pd.read_parquet(path)
    else:
        return pd.read_csv(path)


def write_file(data, path, extension):
    if extension == ".parquet":
        data.to_parquet(path)
    else:
        data.to_csv(path, index=None, header=True)


parser = argparse.ArgumentParser("Aggregate Data")

parser.add_argument("--train_path", dest='train_path', required=True)
parser.add_argument("--inference_path", dest = 'inference_path', required=True)
parser.add_argument("--data_path", dest='data_path', required=True)

args = parser.parse_args()

#Get current run
current_run = Run.get_context()
#Get associated AML workspace
ws = current_run.experiment.workspace
#Get default datastore - used to upload data
datastore = ws.get_default_datastore()

#Target path is passed as a variable argument (can be timestamped)
train_path = args.train_path
inference_path = args.inference_path
data_path = args.data_path

os.makedirs(data_path, exist_ok=True)
os.makedirs(train_path, exist_ok=True)
os.makedirs(inference_path, exist_ok=True)

#Data has been collected already
timestamp_column = 'WeekStarting'
split_date = '1992-05-28'
source_dir = 'local_data'
# # Split each file and store in corresponding directory
local_train_path, local_inference_path = split_data(source_dir, timestamp_column, split_date)

print(local_train_path)
print(local_inference_path)

#move train data
source_dir = './' + local_train_path
target_dir = train_path
raw_files = os.listdir(source_dir)
print(raw_files)
for file_name in raw_files:
    shutil.move(os.path.join(source_dir, file_name), target_dir)

#move inferencing data
source_dir = './' + local_inference_path
target_dir = inference_path
raw_files = os.listdir(source_dir)
print(raw_files)
for file_name in raw_files:
    shutil.move(os.path.join(source_dir, file_name), target_dir) 

#move raw data
source_dir = 'local_data'
target_dir = data_path
raw_files = os.listdir(source_dir)
for file_name in raw_files:
    shutil.move(os.path.join(source_dir, file_name), target_dir)