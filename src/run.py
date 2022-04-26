import os
import sys
import azureml.core
from utils.pipeline import get_pipeline
from azureml.core import Workspace


SRC_DIR = os.path.dirname(os.path.realpath(__file__))


def main(countries=None, years=None, months=None):
    """
    Main methods takes in the parameters from the command line.
    """
    print("Starting multi-stage pipeline.")
    print("Countries:", countries or "all")
    print("Years:", years or "all")
    print("Months:", months or "all")
    
    print("\nAzureML SDK version:", azureml.core.VERSION, "\n")
    ws = Workspace.from_config()

    print("Constructing the pipeline...\n")
    pipeline_run = get_pipeline(ws, countries=countries, years=years, months=months)
    published_pipeline = pipeline_run.publish_pipeline(name="My-Pipeline",
                                                      description="Description of my pipeline",
                                                      version="1.0",
                                                      continue_on_step_failure=True)
    return pipeline_run, published_pipeline


def parse_args(data_selectors, strict=True):
    """
    Simple method to parse the arguments passed to the command.
    """
    countries = None
    years = None
    months = None

    help_message = "All script arguments should have the form 'selector=value', where selector is one of 'countries', 'years', 'months'. Split enumerations by using '_'"
    for selector in data_selectors:
        assert selector.count('=') == 1, help_message
        assert not strict or selector.split('=')[0] in {"countries", "years", "months"}, help_message

        if selector.split('=')[0] == "countries":
            countries = selector.split('=')[1]
            countries = countries.split('_')
            for country in countries:
                assert len(country) == 2 and country.isupper(), "Country code should be uppercase and contain 2 letters."

        if selector.split('=')[0] == "years":
            years = selector.split('=')[1]
            years = years.split('_')
            for year in years:
                assert len(year) == 4 and year.isdigit(), "Year value should be an integer with 4 digits."

        if selector.split('=')[0] == "months":
            months = selector.split('=')[1]
            months = months.split('_')
            for month in months:
                assert month in list(str(x) for x in range(1,13)), "Month value should be an integer value between 1 and 12."
    
    return countries, years, months



if __name__=='__main__':
    assert sys.argv[0].endswith('run.py')
    data_selectors = sys.argv[1:]
    countries, years, months = parse_args(data_selectors)
    pipeline_run, published_pipeline = main(countries=countries, years=years, months=months)
    pipeline_run.wait_for_completion(show_output=True)
