# Multi-Stage AzureML Pipeline Demo

Demo code for running Multi-Stage [AzureML Pipelines](https://docs.microsoft.com/en-us/azure/machine-learning/concept-ml-pipelines) developed at [TechWolf](https://techwolf.ai). It is meant as boilerplate code that can easily be tailored to your own use case. TechWolf uses these pipelines for enriching a Datalake of vacancies with skill information, as you will see from the example workload files.

## Project structure

All utility code that is required to run a pipeline is located under the `/src` directory. Each pipeline stage is linked to a python script which can be found in the `/src/workloads` folder.

## Running a pipeline

Running a pipeline is as simple as running the following command:

```
python run.py countries=BE_NL years=2020_2021
```

You can change the input parameters to suit any use case.
