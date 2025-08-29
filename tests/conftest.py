import shutil

from cookiecutter.main import cookiecutter
from kedro import __version__ as kedro_version
from kedro.framework.cli.starters import TEMPLATE_PATH
import pytest

from kedro_datasentinel.utils import write_template


_FAKE_PROJECT_NAME = "fake_project"


@pytest.fixture(scope="function")
def kedro_project(tmp_path):
    config = {
        # "output_dir": tmp_path,
        "project_name": _FAKE_PROJECT_NAME,
        "repo_name": _FAKE_PROJECT_NAME,
        "python_package": _FAKE_PROJECT_NAME,
        "kedro_version": kedro_version,
        "tools": "['None']",
        "example_pipeline": "False",
    }

    cookiecutter(
        str(TEMPLATE_PATH),
        output_dir=tmp_path,
        no_input=True,
        extra_context=config,
        accept_hooks=False,
    )

    shutil.rmtree(tmp_path / _FAKE_PROJECT_NAME / "tests")  # avoid conflicts with pytest

    return tmp_path / _FAKE_PROJECT_NAME


@pytest.fixture(scope="function")
def kedro_project_with_datasentinel_conf(kedro_project):
    write_template(
        template_name="datasentinel.yml",
        dst_path=kedro_project / "conf" / "local" / "datasentinel.yml",
    )

    return kedro_project
