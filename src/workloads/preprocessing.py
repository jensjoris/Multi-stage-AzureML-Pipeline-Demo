import json
import pandas as pd
from utils.debug import report_time
from utils.export import Exporter, get_directory_info


def init():
    global clean_split_method
    # initialize any objects or models needed for this stage
    report_time("Initializing Spacy")
    clean_split_method = lambda x: []  # dummy code
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

    # Run pre-processing step on vacancies
    report_time("Starting preprocessing")
    df["clean_description_sentences"] = df.description.apply(clean_split_method)
    df["clean_description_sentences"] = df.clean_description_sentences.apply(json.dumps)  # make data exportable

    report_time(f"Writing to filename {dirFilename}")
    exporter.export_df(df, dirFilename)

    return [True]
