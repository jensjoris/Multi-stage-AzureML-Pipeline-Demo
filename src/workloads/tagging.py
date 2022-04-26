import json
import pandas as pd
from utils.debug import report_time
from utils.export import Exporter, get_directory_info


class DummyModel:
    def tag(self, sentences):
        return [[] for _ in sentences]


def init():
    global model
    report_time("Initializing SkillTagging Model")
    model = DummyModel()
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

    # Output skills for all sentences
    len_df = len(df)
    def tag_skills(row):
        report_time(f"Working on {row.name} / {len_df}")
        clean_sentences = json.loads(row["clean_description_sentences"])
        report_time("Tagging the skills")
        skills = model.tag(clean_sentences)
        # parquet can't store complicated structures so export to json
        report_time("json dumps")
        return json.dumps(skills)
    
    report_time("Starting tagging")
    df["tagged_skills"] = df.apply(tag_skills, axis=1)

    exporter.export_df(df, dirFilename)
    print(f"Done writing to filename {dirFilename}")

    return [True]
