# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import pandas as pd
import os
import datetime
import argparse

# Parse input arguments
parser = argparse.ArgumentParser("parallel run step results directory")
parser.add_argument("--automl_parallel_run_step_output", type=str, help="output directory from parallel run step",
                    required=True)
parser.add_argument("--custom_model_parallel_run_step_output", type=str, help="output directory from parallel run step",
                    required=True)
parser.add_argument("--copy_outputs_dir", type=str, help="output directory from parallel run step",
                    required=True)

# add list for the columns to pull ?

args, _ = parser.parse_known_args()

os.makedirs(args.copy_outputs_dir, exist_ok=True)

automl_result_file = os.path.join(args.automl_parallel_run_step_output, 'parallel_run_step.txt')
custom_result_file = os.path.join(args.custom_model_parallel_run_step_output, 'parallel_run_step.txt')

# Read the log file and set the column names from the input timeseries schema
# The parallel run step log does not have a header row, so add it for easier downstream processing
df_automl = pd.read_csv(automl_result_file, delimiter=" ", header=None)
df_automl.columns = ["Week Starting", "Store", "Brand", "Quantity",  "Advert", "Price" , "Revenue", "Predicted-AutoML", "Metric-AutoML" ]

df_custom = pd.read_csv(custom_result_file, delimiter=" ", header=None)
df_custom.columns = ['WeekStarting', 'Predicted-Custom', 'Metric-Custom', 'Quantity', 'Store', 'Brand']

df_joined = df_automl.merge(df_custom, how='left')

df_joined.to_csv(os.path.join(args.copy_outputs_dir, 'joined_data.csv'), index=False)

df_joined['Predictions'] = [row['Predicted-AutoML'] if row['Metric-AutoML'] < row['Metric-Custom'] else row['Predicted-Custom'] for idx, row in df_joined.iterrows()]
df_joined['Chosen Model'] = ['AutoML' if row['Metric-AutoML'] < row['Metric-Custom'] else 'Custom' for idx, row in df_joined.iterrows()]
df_merged = df_joined.drop(['Predicted-Custom', 'Predicted-AutoML', 'Metric-AutoML', 'Metric-Custom'], axis=1)

df_merged.to_csv(os.path.join(args.copy_outputs_dir, 'merged_data.csv'), index=False)

# Logic to write dataframe to Snowflake here

