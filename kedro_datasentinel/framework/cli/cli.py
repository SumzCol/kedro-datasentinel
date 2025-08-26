from importlib import resources
import logging
from pathlib import Path

import click
from datasentinel.session import DataSentinelSession
from kedro.framework.cli.utils import LazyGroup
from kedro.framework.project import settings
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from kedro.utils import _find_kedro_project
from pydantic import ValidationError

from kedro_datasentinel.config.data_validation import ValidationWorkflowConfig
from kedro_datasentinel.core import DataValidationConfigError, Mode
from kedro_datasentinel.utils import dataset_has_validations


@click.group()
def commands():
    pass


@commands.group(
    name="datasentinel",
    cls=LazyGroup,
    lazy_subcommands={
        "validate": "kedro_datasentinel.framework.cli.cli.validate",
    },
)
def datasentinel():
    """Kedro plugin to interact with DataSentinel."""
    pass


@click.command(name="init")
@click.option(
    "--env",
    "-e",
    default="local",
    help="The name of the kedro environment where the 'datasentinel.yml' should be created. "
    "Default to 'local'",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Update the template without any checks.",
)
def init(env: str, force: bool):
    filename = "datasentinel.yml"
    # Load the template from the package
    config_template = resources.read_text("kedro_datasentinel.template", filename)
    project_path = _find_kedro_project(Path.cwd()) or Path.cwd()
    bootstrap_project(project_path)
    dst_path = project_path / settings.CONF_SOURCE / env / filename

    if dst_path.is_file() and not force:
        click.secho(
            click.style(
                f"A 'datasentinel.yml' already exists at '{dst_path}' You can use the "
                f"``--force`` option to override it.",
                fg="red",
            )
        )
    else:
        try:
            with open(dst_path, "w", encoding="utf-8") as file:
                file.write(config_template)
        except FileNotFoundError:
            click.secho(
                click.style(
                    f"No env '{env}' found. Please check this folder exists inside "
                    f"'{settings.CONF_SOURCE}' folder.",
                    fg="red",
                )
            )


@click.command(name="validate")
@click.option(
    "--dataset",
    "-d",
    required=True,
    help="The name of the dataset to be validated",
)
@click.option(
    "--env",
    "-e",
    required=False,
    default="local",
    help="The name of the environment",
)
def validate(dataset: str, env: str):
    """Validate a Kedro dataset using DataSentinel."""
    project_path = Path.cwd()
    with KedroSession.create(
        project_path=project_path,
        env=env,
    ) as session:
        context = session.load_context()
        catalog = context.catalog
        dataset_instance = catalog._get_dataset(dataset_name=dataset)

        if not dataset_has_validations(dataset_instance):
            logging.getLogger(__name__).info(
                f"Dataset '{dataset}' doesnt have validations configured."
            )
            return

        try:
            validation_conf_model = ValidationWorkflowConfig(
                **dataset_instance.metadata["kedro-dataguard"]
            )
        except ValidationError as e:
            raise DataValidationConfigError(
                f"The validation node configuration of the '{dataset}' dataset "
                f"could not be parsed, please verify that it has a valid structure: {e!s}"
            ) from e

        if not validation_conf_model.has_offline_checks:
            logging.getLogger(__name__).info(
                f"Dataset '{dataset}' does not have checks with 'OFFLINE' or 'BOTH' mode."
            )
            return

        validation_workflow = validation_conf_model.create_validation_workflow(
            dataset_name=dataset,
            data=dataset_instance.load(),
            mode=Mode.OFFLINE,
        )

        ds = DataSentinelSession.get_or_create()
        ds.run_validation_workflow(validation_workflow)
