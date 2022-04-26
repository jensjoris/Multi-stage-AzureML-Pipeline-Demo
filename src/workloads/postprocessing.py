import json
import pandas as pd
from utils.debug import report_time
from utils.export import Exporter, get_directory_info

def init():
    global postprocessing
    report_time("Initializing postprocessing module")
    postprocessing = lambda skill_tags: skill_tags  # dummy code
    report_time("INIT DONE")


def run(mini_batch):
    report_time(mini_batch)
    dirCountry, dirYear, dirMonth, dirFilename = get_directory_info(mini_batch[0])
    dirFilename += ".parquet"

    exporter = Exporter(
        country=dirCountry,
        year=dirYear,
        month=dirMonth
    )

    # Skip if output file already exists
    if exporter.exists(dirFilename):
        report_time(f"{dirFilename} already exists, skipping...")
        return [True]
    
    df = pd.read_parquet(mini_batch[0])
    report_time(f"Working on {dirFilename}, length={len(df)}")

    df["final_skill_profile"] = df.tagged_skills.apply(postprocessing)
    df["final_skill_profile"] = df.final_skill_profile.apply(json.dumps)

    # These columns cannot be in the file if we want to partition it by these columns.
    df = df.drop(['dirFilename', 'dirCountry', 'dirYear', 'dirMonth'], axis=1)
    exporter.export_df(df, dirFilename)
    print(f"Done writing to filename {dirFilename}")

    return [True]
