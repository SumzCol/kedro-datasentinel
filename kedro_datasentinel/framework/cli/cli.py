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
from kedro_datasentinel.utils import dataset_has_validations, write_template


@click.group()
def commands():
    pass  # pragma: no cover


@commands.group(
    name="datasentinel",
    cls=LazyGroup,
    lazy_subcommands={
        "init": "kedro_datasentinel.framework.cli.cli.init",
        "validate": "kedro_datasentinel.framework.cli.cli.validate",
    },
)
def datasentinel():
    """Kedro plugin to interact with DataSentinel."""
    pass  # pragma: no cover


@click.command(name="init")  # type: ignore
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
    """Initialize a 'datasentinel.yml' configuration file in the specified environment."""
    filename = "datasentinel.yml"
    # Load the template from the package
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
            write_template(filename, dst_path)
            click.secho(
                click.style(
                    f"'{settings.CONF_SOURCE}/{env}/{filename}' successfully updated.",
                    fg="green",
                )
            )
        except FileNotFoundError:
            click.secho(
                click.style(
                    f"No env '{env}' found. Please check this folder exists inside "
                    f"'{settings.CONF_SOURCE}' folder.",
                    fg="red",
                )
            )


@click.command(name="validate")  # type: ignore
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
    """Validate a Kedro dataset using Data Sentinel."""
    project_path = _find_kedro_project(Path.cwd()) or Path.cwd()
    with KedroSession.create(
        project_path=project_path,
        env=env,
    ) as session:
        context = session.load_context()
        catalog = context.catalog
        dataset_instance = catalog._get_dataset(dataset_name=dataset)

        if not dataset_has_validations(dataset_instance):
            click.secho(
                click.style(
                    f"Dataset '{dataset}' doesn't have validations configured.",
                    fg="yellow",
                )
            )
            return

        try:
            validation_conf_model = ValidationWorkflowConfig(
                **dataset_instance.metadata["kedro-datasentinel"]
            )
        except ValidationError as e:
            raise DataValidationConfigError(
                f"The validation node configuration of the '{dataset}' dataset "
                f"could not be parsed, please verify that it has a valid structure: {e!s}"
            ) from e

        if not validation_conf_model.has_offline_checks:
            click.secho(
                click.style(
                    f"Dataset '{dataset}' does not have checks with 'OFFLINE' or 'BOTH' mode.",
                    fg="yellow",
                )
            )
            return

        validation_workflow = validation_conf_model.create_validation_workflow(
            dataset_name=dataset,
            data=dataset_instance.load(),
            mode=Mode.OFFLINE,
        )

        ds = DataSentinelSession.get_or_create()
        ds.run_validation_workflow(validation_workflow)
