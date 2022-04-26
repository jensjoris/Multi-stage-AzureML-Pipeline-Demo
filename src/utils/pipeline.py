import uuid
from utils.environment import get_cpu_environment, get_gpu_environment
from utils.datalake import get_already_processed_vacancies, get_vacancies

from azureml.pipeline.steps import ParallelRunStep
from azureml.data import OutputFileDatasetConfig
from azureml.pipeline.steps import ParallelRunConfig
from azureml.pipeline.core import Pipeline, StepSequence
from azureml.core.compute import ComputeTarget
from azureml.core import Experiment


class DatasetLocation:
    raw_vacancies = "/dataset/raw-vacancies"
    splitted_vacancies = "/dataset/intermediate/splitted-vacancies"
    preprocessed_vacancies = "/dataset/intermediate/preprocessed-vacancies"
    tagged_vacancies = "/dataset/intermediate/tagged-vacancies"
    enriched_vacancies = "/dataset/enriched-vacancies"


def get_pipeline(ws, countries=None, years=None, months=None):
    # Default workspace datastore
    blob_datastore = ws.get_default_datastore()

    # Input FileDatasets
    processed_vacancies = get_already_processed_vacancies(ws)
    raw_vacancies_dataset = get_vacancies(ws, DatasetLocation.raw_vacancies, countries=countries, years=years, months=months)
    splitted_vacancies_dataset = get_vacancies(ws, DatasetLocation.splitted_vacancies, countries=countries, years=years, months=months)
    preprocessed_vacancies_dataset = get_vacancies(ws, DatasetLocation.preprocessed_vacancies, countries=countries, years=years, months=months)
    tagged_vacancies_dataset = get_vacancies(ws, DatasetLocation.tagged_vacancies, countries=countries, years=years, months=months)

    # Compute targets (will have to be created beforehand)
    cpu_memory = ComputeTarget(workspace=ws, name="E2a-v4-spot")
    gpu_compute = ComputeTarget(workspace=ws, name="NC6-spot")

    # Environments (will be created on runtime of they don't exist yet)
    cpu_environment = get_cpu_environment()
    gpu_environment = get_gpu_environment()

    # zero'th run set configuration
    zeroth_parallel_run_config = ParallelRunConfig(
        environment=cpu_environment,
        entry_script="splitting.py",
        source_directory="workloads",
        output_action="summary_only",
        mini_batch_size=1,  # one file at a time
        error_threshold=1,
        compute_target=cpu_memory,
        process_count_per_node=1,
        run_invocation_timeout=600,
        node_count=100
    )

    # first run step configuration
    first_parallel_run_config = ParallelRunConfig(
        environment=cpu_environment,
        entry_script="preprocessing.py",
        source_directory="workloads",
        output_action="summary_only",
        mini_batch_size=1,  # one file at a time
        error_threshold=1,
        compute_target=cpu_memory,
        process_count_per_node=1,
        run_invocation_timeout=600,
        node_count=100
    )

    # second run step configuration
    second_parallel_run_config = ParallelRunConfig(
        environment=gpu_environment,
        entry_script="tagging.py",
        source_directory="workloads",
        output_action="summary_only",
        mini_batch_size=1,  # one file at a time
        error_threshold=100,
        compute_target=gpu_compute,
        process_count_per_node=1,
        run_invocation_timeout=1200,
        node_count=33
    )

    # third run step configuration
    third_parallel_run_config = ParallelRunConfig(
        environment=cpu_environment,
        entry_script="postprocessing.py",
        source_directory="workloads",
        output_action="summary_only",
        mini_batch_size=1,  # one file at a time
        error_threshold=100,
        compute_target=cpu_memory,
        process_count_per_node=1,
        run_invocation_timeout=1200,
        node_count=100
    )

    # splitting parquet files in smaller batch sizes (file dataset)
    zeroth_dir = OutputFileDatasetConfig(
        name="splitted_vacancies",
        destination=(blob_datastore, DatasetLocation.splitted_vacancies)
    ).as_mount()

    # preprocessed vacancies dataset (file dataset)
    first_dir = OutputFileDatasetConfig(
        name="preprocessed_vacancies",
        destination=(blob_datastore, DatasetLocation.preprocessed_vacancies)
    ).as_mount()

    # tagged vacancies dataset (file dataset)
    second_dir = OutputFileDatasetConfig(
        name="tagged_vacancies",
        destination=(blob_datastore, DatasetLocation.tagged_vacancies)
    ).as_mount()

    # enriched vacancies dataset (tabular dataset)
    third_dir = OutputFileDatasetConfig(
        name="enriched_vacancies",
        destination=(blob_datastore, DatasetLocation.enriched_vacancies),
        partition_format="country={dirCountry}/year={dirYear}/month={dirMonth}/{dirFilename}.*.parquet"
    ).read_parquet_files().register_on_complete(
        name="enriched-vacancies", 
        description="Vacancies enriched with skill profiles."
    )

    
    # side-input dataset for skipping already processed vacancies
    splitting_arguments = None
    splitting_side_inputs = None
    if processed_vacancies is not None:
        processed_vacancies_local_path = "/tmp/{}".format(str(uuid.uuid4()))
        processed_vacancies = processed_vacancies.as_mount(processed_vacancies_local_path)
        splitting_arguments = ["--side_input_dir", processed_vacancies]
        splitting_side_inputs = [processed_vacancies]
    

    zeroth_step_name = "splitting"
    first_step_name = "preprocessing"
    second_step_name = "tagging"
    third_step_name = "postprocessing"

    # zeroth run step
    zeroth_step = ParallelRunStep(
        name=zeroth_step_name,
        inputs=[raw_vacancies_dataset.as_named_input("raw_vacancies")],
        output=zeroth_dir,
        arguments=splitting_arguments,
        side_inputs=splitting_side_inputs,
        parallel_run_config=zeroth_parallel_run_config,
        allow_reuse=True
    )

    # first run step
    first_step = ParallelRunStep(
        name=first_step_name,
        inputs=[splitted_vacancies_dataset.as_named_input("splitted_vacancies")],
        output=first_dir,
        parallel_run_config=first_parallel_run_config,
        allow_reuse=True
    )

    # second run step
    second_step = ParallelRunStep(
        name=second_step_name,
        inputs=[preprocessed_vacancies_dataset.as_named_input("preprocessed_vacancies")],
        output=second_dir,
        parallel_run_config=second_parallel_run_config,
        allow_reuse=True
    )

    # third run step
    third_step = ParallelRunStep(
        name=third_step_name,
        inputs=[tagged_vacancies_dataset.as_named_input("tagged_vacancies")],
        output=third_dir,
        parallel_run_config=third_parallel_run_config,
        allow_reuse=True
    )

    # create full pipeline
    pipeline = Pipeline(
        workspace=ws,
        steps=StepSequence(steps=[zeroth_step, first_step, second_step, third_step]),
        description=f"countries={countries} years={years} months={months}"
    )
    pipeline_run = Experiment(ws, 'my-experiment').submit(pipeline)
    return pipeline_run
