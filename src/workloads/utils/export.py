import os
from pathlib import Path
from azureml_user.parallel_run import EntryScript


def get_directory_info(filename):
    # filename is formatted like this: 
    # '/mnt/batch/tasks/shared/.../.../country=UK/year=2020/month=1/filename.parquet'
    parts = filename.split("/")
    dirCountry = parts[-4][len("country="):]
    dirYear = parts[-3][len("year="):]
    dirMonth = parts[-2][len("month="):]
    dirFilename = parts[-1][:-len(".parquet")]
    return dirCountry, dirYear, dirMonth, dirFilename


class Exporter:
    def __init__(self, country, year, month):
        entry_script = EntryScript()
        self.output_dir = Path(entry_script.output_dir)
        # prepare output folder
        self.local_path = os.path.join(self.output_dir, f"country={country}/year={year}/month={month}")
        os.makedirs(self.local_path, exist_ok=True)
    
    def exists(self, filename):
        local_filename = os.path.join(self.local_path, filename)
        return os.path.exists(local_filename)
    
    def export_df(self, df, filename):
        local_filename = os.path.join(self.local_path, filename)
        df.to_parquet(local_filename, index=False, compression='snappy')
