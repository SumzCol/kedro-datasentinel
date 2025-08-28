from unittest.mock import MagicMock, patch

from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline.node import Node
from pydantic import ValidationError
import pytest

from kedro_datasentinel.core import DataValidationConfigError, Mode
from kedro_datasentinel.framework.hooks.datasentinel_hooks import DataSentinelHooks


@pytest.fixture
def mock_catalog_with_validations():
    """Create a mock catalog with datasets that have validation metadata."""
    dataset = MemoryDataset()
    dataset.metadata = {
        "kedro-datasentinel": {
            "name": "test_validation",
            "check_list": {
                "test_check": {
                    "type": "CualleeCheck",
                    "mode": "ONLINE",
                    "level": "ERROR",
                    "rules": [],
                }
            },
        }
    }
    catalog = MagicMock(spec=DataCatalog)
    catalog._get_dataset.return_value = dataset
    return catalog


@pytest.fixture
def mock_catalog_without_validations():
    """Create a mock catalog with datasets that have no validation metadata."""
    dataset = MemoryDataset()
    catalog = MagicMock(spec=DataCatalog)
    catalog._get_dataset.return_value = dataset
    return catalog


@pytest.fixture
def mock_node_outputs():
    """Create mock node outputs."""
    return {"test_dataset": {"col1": [1, 2, 3], "col2": [4, 5, 6]}}


@pytest.fixture
def mock_node():
    """Create a mock node."""
    return MagicMock(spec=Node, name="test_node")


@pytest.mark.unit
class TestOnlineValidationsUnit:
    def test_no_validations_configured(
        self, mock_catalog_without_validations, mock_node_outputs, mock_node
    ):
        """Test that no validation workflow is executed when datasets have no validations."""
        mock_session = MagicMock()
        hook = DataSentinelHooks()
        hook._session = mock_session
        hook._audit_enabled = False  # Disable audit logging to focus on validation logic

        with patch(
            "kedro_datasentinel.framework.hooks.datasentinel_hooks.dataset_has_validations",
            return_value=False,
        ):
            # Call the public hook method instead of the private method
            hook.after_node_run(
                node=mock_node,
                catalog=mock_catalog_without_validations,
                inputs={},
                outputs=mock_node_outputs,
            )

        # Verify that no validation workflow was executed
        mock_session.run_validation_workflow.assert_not_called()

    def test_invalid_validation_config_raises_error(
        self, mock_catalog_with_validations, mock_node_outputs, mock_node
    ):
        """Test that DataValidationConfigError is raised when validation config is invalid."""
        mock_session = MagicMock()
        hook = DataSentinelHooks()
        hook._session = mock_session
        hook._audit_enabled = False  # Disable audit logging to focus on validation logic

        with (
            patch(
                "kedro_datasentinel.framework.hooks.datasentinel_hooks.dataset_has_validations",
                return_value=True,
            ),
            patch(
                "kedro_datasentinel.framework.hooks.datasentinel_hooks.ValidationWorkflowConfig",
                side_effect=ValidationError.from_exception_data("ValidationWorkflowConfig", []),
            ),
        ):
            with pytest.raises(DataValidationConfigError, match="could not be parsed"):
                # Call the public hook method instead of the private method
                hook.after_node_run(
                    node=mock_node,
                    catalog=mock_catalog_with_validations,
                    inputs={},
                    outputs=mock_node_outputs,
                )

        # Verify that no validation workflow was executed
        mock_session.run_validation_workflow.assert_not_called()

    def test_offline_only_checks_skipped(
        self, mock_catalog_with_validations, mock_node_outputs, mock_node
    ):
        """Test that validation workflow is not executed for datasets with only offline checks."""
        mock_session = MagicMock()
        hook = DataSentinelHooks()
        hook._session = mock_session
        hook._audit_enabled = False  # Disable audit logging to focus on validation logic

        mock_validation_config = MagicMock()
        mock_validation_config.has_online_checks = False

        with (
            patch(
                "kedro_datasentinel.framework.hooks.datasentinel_hooks.dataset_has_validations",
                return_value=True,
            ),
            patch(
                "kedro_datasentinel.framework.hooks.datasentinel_hooks.ValidationWorkflowConfig",
                return_value=mock_validation_config,
            ),
        ):
            # Call the public hook method instead of the private method
            hook.after_node_run(
                node=mock_node,
                catalog=mock_catalog_with_validations,
                inputs={},
                outputs=mock_node_outputs,
            )

        # Verify that no validation workflow was executed
        mock_session.run_validation_workflow.assert_not_called()

    def test_online_checks_executed(
        self, mock_catalog_with_validations, mock_node_outputs, mock_node
    ):
        """Test that validation workflow is executed for datasets with online checks."""
        mock_session = MagicMock()
        hook = DataSentinelHooks()
        hook._session = mock_session
        hook._audit_enabled = False  # Disable audit logging to focus on validation logic

        mock_validation_config = MagicMock()
        mock_validation_config.has_online_checks = True
        mock_validation_workflow = MagicMock()
        mock_validation_config.create_validation_workflow.return_value = mock_validation_workflow

        with (
            patch(
                "kedro_datasentinel.framework.hooks.datasentinel_hooks.dataset_has_validations",
                return_value=True,
            ),
            patch(
                "kedro_datasentinel.framework.hooks.datasentinel_hooks.ValidationWorkflowConfig",
                return_value=mock_validation_config,
            ),
        ):
            # Call the public hook method instead of the private method
            hook.after_node_run(
                node=mock_node,
                catalog=mock_catalog_with_validations,
                inputs={},
                outputs=mock_node_outputs,
            )

        # Verify that validation workflow was created with correct parameters
        mock_validation_config.create_validation_workflow.assert_called_once_with(
            dataset_name="test_dataset",
            data=mock_node_outputs["test_dataset"],
            mode=Mode.ONLINE,
        )

        # Verify that validation workflow was executed
        mock_session.run_validation_workflow.assert_called_once_with(mock_validation_workflow)
