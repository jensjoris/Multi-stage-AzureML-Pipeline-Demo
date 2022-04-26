from azureml.core import Environment
from azureml.core.conda_dependencies import CondaDependencies
from azureml.core.runconfig import DEFAULT_GPU_IMAGE, DEFAULT_CPU_IMAGE
from azureml.core import Workspace


ws = Workspace.from_config()


def _get_or_create_environment(
    BASE_IMAGE,
    env_name,
    extra_pip_packages=[]
):
    try:
        environment = Environment.get(ws, env_name)
    except Exception:
        environment = Environment(name=env_name)
        all_pip_packages = [
            "azureml-core~=1.33.0",
            "azureml-dataprep[pandas]",
            "python-dotenv==0.19.0"
        ] + extra_pip_packages
        cd = CondaDependencies.create(
            pip_packages=all_pip_packages
        )
        cd.set_python_version("3.8.1")
        environment.python.conda_dependencies = cd
        environment.docker.base_image = BASE_IMAGE
        environment.register(ws)
    return environment


def get_cpu_environment():
    env_name = "conda-docker-cpu-dataprep[pandas]"

    return _get_or_create_environment(
        BASE_IMAGE = DEFAULT_CPU_IMAGE,
        env_name = env_name
    )


def get_gpu_environment():
    env_name = "conda-docker-gpu-dataprep[pandas]"

    return _get_or_create_environment(
        BASE_IMAGE = DEFAULT_GPU_IMAGE,
        env_name = env_name,
        extra_pip_packages=[
            "sentence-transformers==1.2.0",
            "torch==1.8.2",
        ]  # add whatever packages are necessary in this environment
    )
