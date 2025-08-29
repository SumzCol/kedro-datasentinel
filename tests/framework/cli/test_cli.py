from unittest.mock import MagicMock, patch

from click.testing import CliRunner
from kedro.io import MemoryDataset
from pydantic import ValidationError
import pytest

from kedro_datasentinel.core import DataValidationConfigError, Mode
from kedro_datasentinel.framework.cli import cli


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_kedro_session():
    with patch("kedro_datasentinel.framework.cli.cli.KedroSession") as mock_session:
        yield mock_session


@pytest.mark.unit
class TestCliUnit:
    def test_datasentinel_group_exists(self, runner):
        """Test that the datasentinel group exists and contains validate subcommand."""
        result = runner.invoke(cli.datasentinel, ["--help"])
        assert result.exit_code == 0
        assert "validate" in result.output

    @pytest.mark.parametrize("inside_subdirectory", (True, False))
    def test_cli_init(self, runner, monkeypatch, kedro_project, inside_subdirectory):
        """Test that the datasentinel init command creates a datasentinel.yml file."""
        if inside_subdirectory is True:
            monkeypatch.chdir(kedro_project / "src")
        else:
            monkeypatch.chdir(kedro_project)
        result = runner.invoke(cli.init)

        # the command should have executed properly
        assert result.exit_code == 0

        # check datasentinel.yml file
        assert "'conf/local/datasentinel.yml' successfully updated." in result.output
        assert (kedro_project / "conf" / "local" / "datasentinel.yml").is_file()

    def test_cli_init_existing_file_without_force_flag(
        self, runner, monkeypatch, kedro_project_with_datasentinel_conf
    ):
        """Test that the datasentinel init command fails when a datasentinel.yml file already
        exists."""
        monkeypatch.chdir(kedro_project_with_datasentinel_conf)
        result = runner.invoke(cli.init)

        # the command should have executed properly
        assert result.exit_code == 0

        # check datasentinel.yml file
        assert "A 'datasentinel.yml' already exists at" in result.output

    def test_cli_init_existing_file_with_force_flag(
        self, runner, monkeypatch, kedro_project_with_datasentinel_conf
    ):
        """Test that the datasentinel init command overwrites a datasentinel.yml file when the
        --force flag is used."""
        monkeypatch.chdir(kedro_project_with_datasentinel_conf)
        result = runner.invoke(cli.init, ["--force"])

        # the command should have executed properly
        assert result.exit_code == 0

        # check datasentinel.yml file
        assert "'conf/local/datasentinel.yml' successfully updated." in result.output
        assert (
            kedro_project_with_datasentinel_conf / "conf" / "local" / "datasentinel.yml"
        ).is_file()

    @pytest.mark.parametrize(
        "env",
        ["base", "local"],
    )
    def test_cli_init_with_env(self, runner, monkeypatch, kedro_project, env):
        """Test that the datasentinel init command creates a datasentinel.yml file in the
        specified environment."""
        monkeypatch.chdir(kedro_project)
        result = runner.invoke(cli.init, f"--env {env}")

        # the command should have executed properly
        assert result.exit_code == 0

        # check datasentinel.yml file
        assert f"'conf/{env}/datasentinel.yml' successfully updated." in result.output
        assert (kedro_project / "conf" / env / "datasentinel.yml").is_file()

    def test_cli_init_with_wrong_env(self, runner, monkeypatch, kedro_project):
        """Test that the datasentinel init command fails when the specified environment does
        not exist."""
        env = "prod"
        monkeypatch.chdir(kedro_project)
        result = runner.invoke(cli.init, f"--env {env}")

        # A warning message should appear
        assert f"No env '{env}' found" in result.output

    @patch("kedro_datasentinel.framework.cli.cli.DataSentinelSession")
    @patch("kedro_datasentinel.framework.cli.cli.ValidationWorkflowConfig")
    @patch("kedro_datasentinel.framework.cli.cli.dataset_has_validations", return_value=True)
    def test_validate_successful(
        self,
        mock_has_validations,
        mock_validation_config,
        mock_ds_session,
        mock_kedro_session,
        runner,
    ):
        """Test successful validation workflow execution."""
        # Setup
        mock_validation_config.return_value.has_offline_checks = True
        dataset = MemoryDataset(
            data=[1, 2, 3],
            metadata={"kedro-datasentinel": {"name": "test"}},
        )
        session_instance = mock_kedro_session.create.return_value.__enter__.return_value
        context_instance = session_instance.load_context.return_value
        context_instance.catalog._get_dataset.return_value = dataset
        mock_workflow = (
            mock_validation_config.return_value.create_validation_workflow.return_value
        )

        # Execute
        result = runner.invoke(cli.validate, ["--dataset", "test_dataset"])

        # Verify
        assert result.exit_code == 0
        mock_has_validations.assert_called_once_with(dataset)
        mock_validation_config.assert_called_once_with(**dataset.metadata["kedro-datasentinel"])
        mock_validation_config.return_value.create_validation_workflow.assert_called_once_with(
            dataset_name="test_dataset",
            data=dataset.load(),
            mode=Mode.OFFLINE,
        )
        mock_ds_session.get_or_create.return_value.run_validation_workflow.assert_called_once_with(
            mock_workflow
        )

    @patch("kedro_datasentinel.framework.cli.cli.DataSentinelSession")
    @patch("kedro_datasentinel.framework.cli.cli.dataset_has_validations", return_value=False)
    def test_validate_no_validations(
        self, mock_has_validations, mock_ds_session, mock_kedro_session, runner
    ):
        """Test validation when dataset has no validations configured."""
        # Setup
        dataset = MemoryDataset()
        session_instance = mock_kedro_session.create.return_value.__enter__.return_value
        context_instance = session_instance.load_context.return_value
        context_instance.catalog._get_dataset.return_value = dataset

        # Execute
        result = runner.invoke(cli.validate, ["--dataset", "test_dataset"])

        # Verify
        assert result.exit_code == 0
        assert "doesn't have validations configured" in result.output
        mock_has_validations.assert_called_once_with(dataset)
        mock_ds_session.get_or_create.assert_not_called()

    @patch("kedro_datasentinel.framework.cli.cli.dataset_has_validations", return_value=True)
    @patch(
        "kedro_datasentinel.framework.cli.cli.ValidationWorkflowConfig",
        side_effect=ValidationError.from_exception_data("ValidationWorkflowConfig", []),
    )
    def test_validate_config_error(
        self, mock_validation_config, mock_has_validations, mock_kedro_session, runner
    ):
        """Test validation when config has validation errors."""
        # Setup
        mock_dataset = MagicMock()
        mock_dataset.metadata = {"kedro-datasentinel": {}}
        session_instance = mock_kedro_session.create.return_value.__enter__.return_value
        context_instance = session_instance.load_context.return_value
        context_instance.catalog._get_dataset.return_value = mock_dataset

        # Execute and Verify
        with pytest.raises(DataValidationConfigError, match="could not be parsed"):
            runner.invoke(cli.validate, ["--dataset", "test_dataset"], catch_exceptions=False)

    @patch("kedro_datasentinel.framework.cli.cli.DataSentinelSession")
    @patch("kedro_datasentinel.framework.cli.cli.ValidationWorkflowConfig")
    @patch("kedro_datasentinel.framework.cli.cli.dataset_has_validations", return_value=True)
    def test_validate_no_offline_checks(
        self,
        mock_has_validations,
        mock_validation_config,
        mock_ds_session,
        mock_kedro_session,
        runner,
    ):
        """Test validation when dataset has no offline checks configured."""
        # Setup
        mock_validation_config.return_value.has_offline_checks = False
        mock_dataset = MemoryDataset(
            data=[1, 2, 3],
            metadata={"kedro-datasentinel": {"name": "test"}},
        )
        session_instance = mock_kedro_session.create.return_value.__enter__.return_value
        context = session_instance.load_context.return_value
        context.catalog._get_dataset.return_value = mock_dataset

        # Execute
        result = runner.invoke(cli.validate, ["--dataset", "test_dataset"])

        # Verify
        assert result.exit_code == 0
        assert "does not have checks with 'OFFLINE' or 'BOTH' mode" in result.output
        mock_ds_session.get_or_create.return_value.run_validation_workflow.assert_not_called()

    @patch("kedro_datasentinel.framework.cli.cli.KedroSession")
    def test_validate_with_custom_env(self, mock_kedro_session, runner):
        """Test validate command with custom environment parameter."""
        # Setup
        session_instance = mock_kedro_session.create.return_value.__enter__.return_value
        context_instance = session_instance.load_context.return_value
        context_instance.catalog._get_dataset.return_value = MagicMock()

        # Execute
        result = runner.invoke(cli.validate, ["--dataset", "test_dataset", "--env", "custom_env"])

        # Verify
        assert result.exit_code == 0
        mock_kedro_session.create.assert_called_once()
        _, kwargs = mock_kedro_session.create.call_args
        assert kwargs.get("env") == "custom_env"
