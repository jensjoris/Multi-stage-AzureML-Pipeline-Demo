import os
import argparse
import numpy as np
import pandas as pd
from utils.debug import report_time
from utils.export import Exporter, get_directory_info


def init():
    global side_input_dir
    global partition_cache

    parser = argparse.ArgumentParser()
    parser.add_argument('--side_input_dir', dest="side_input_dir", required=False)
    args, _ = parser.parse_known_args()

    print(args)

    side_input_dir = None
    if hasattr(args, 'side_input_dir'):
        side_input_dir = args.side_input_dir
    
    partition_cache = {}


def run(mini_batch):
    # parquet files arrive in full because azureml fails to split them into mini-batches
    # that's why this step is needed to explicitly split all parquet files into chunks of 1000 vacancies
    report_time(mini_batch)

    # retrieve partition keys from file location
    dirCountry, dirYear, dirMonth, dirFilename = get_directory_info(mini_batch[0])

    # set partition keys explicitly as columns in the parquet files
    df = pd.read_parquet(mini_batch[0])
    df["dirCountry"] = dirCountry
    df["dirYear"] = dirYear
    df["dirMonth"] = dirMonth
    df["dirFilename"] = dirFilename

    exporter = Exporter(
        country=dirCountry,
        year=dirYear,
        month=dirMonth
    )

    def get_already_processed_partitions():
        prefix = f"country={dirCountry}/year={dirYear}/month={dirMonth}"
        if prefix in partition_cache:
            return partition_cache[prefix]
        month_path = os.path.join(side_input_dir, prefix)
        filenames = []
        if os.path.exists(month_path):
            filenames = os.listdir(month_path)
        partition_cache[prefix] = filenames
        return filenames
        

    def split_given_size(a, size):
        return np.split(a, np.arange(size,len(a),size))
    
    existing_files = []
    if side_input_dir is not None:
        existing_files = get_already_processed_partitions()

    for index, dataframe in enumerate(split_given_size(df, size=1000)):
        batch_filename = f"{dirFilename}.{index}.parquet"
        if batch_filename in existing_files:
            continue
        dataframe["batchFilename"] = batch_filename
        if not exporter.exists(batch_filename):
            exporter.export_df(dataframe, batch_filename)

    return [True]
