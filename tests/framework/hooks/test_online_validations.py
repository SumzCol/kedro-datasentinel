from unittest.mock import MagicMock, patch

from datasentinel.session import DataSentinelSession
from kedro.framework.hooks import _create_hook_manager
from kedro.framework.hooks.manager import _register_hooks
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import Pipeline
from kedro.pipeline.node import node
from kedro.runner import ThreadRunner
from pandas import DataFrame
from pydantic import ValidationError
import pytest

from kedro_datasentinel.core import DataValidationConfigError
from kedro_datasentinel.framework.hooks.datasentinel_hooks import DataSentinelHooks


@pytest.fixture
def dummy_pipeline():
    def extract_data():
        return DataFrame(data={"col1": [1, 2, 3], "col2": [4, 5, 6]})

    return Pipeline(
        nodes=[
            node(
                func=extract_data,
                inputs=None,
                outputs="raw_data",
                name="extract_data",
            ),
        ]
    )


@pytest.fixture
def catalog_with_validations():
    return DataCatalog(
        {
            "raw_data": MemoryDataset(
                metadata={
                    "kedro-datasentinel": {
                        "name": "test_validation",
                        "data_asset": "raw_data",
                        "data_asset_schema": "raw",
                        "check_list": {
                            "type": "CualleeCheck",
                            "mode": "ONLINE",
                            "level": "ERROR",
                            "rules": [{"name": "is_less_than", "column": "col1", "value": 2}],
                        },
                    }
                }
            ),
        }
    )


@pytest.mark.unit
class TestOnlineValidationsUnit:
    def test_dataset_with_badly_configured_validations(
        self,
        kedro_project_with_datasentinel_conf,
        dummy_pipeline,
        catalog_with_validations,
    ):
        """Test that a exception is raised when dataset has badly configured validations"""
        mock_session = MagicMock(spec=DataSentinelSession)
        mock_audit_store_manager = MagicMock()

        # Disable audit logging
        mock_audit_store_manager.count.return_value = 0
        mock_session.audit_store_manager = mock_audit_store_manager

        bootstrap_project(kedro_project_with_datasentinel_conf)
        with KedroSession.create(
            project_path=kedro_project_with_datasentinel_conf,
        ) as session:
            context = session.load_context()

            with (
                patch(
                    "kedro_datasentinel.framework.hooks.datasentinel_hooks."
                    "DataSentinelHooks._init_session",
                    return_value=mock_session,
                ),
                patch(
                    "kedro_datasentinel.framework.hooks.datasentinel_hooks."
                    "ValidationWorkflowConfig",
                    side_effect=ValidationError.from_exception_data(
                        "DataValidationConfigError",
                        [],
                    ),
                ),
            ):
                runner = ThreadRunner()
                datasentinel_hook = DataSentinelHooks()
                datasentinel_hook.after_context_created(context)

                datasentinel_hook.before_pipeline_run(
                    run_params={},
                )

                hook_manager = _create_hook_manager()
                _register_hooks(hook_manager, (datasentinel_hook,))

                with pytest.raises(DataValidationConfigError):
                    runner.run(
                        pipeline=dummy_pipeline,
                        catalog=catalog_with_validations,
                        hook_manager=hook_manager,
                    )

    @pytest.mark.parametrize(
        "has_online_checks",
        [True, False],
        ids=["with_online_checks", "without_online_checks"],
    )
    def test_dataset_with_and_without_online_checks(
        self,
        kedro_project_with_datasentinel_conf,
        dummy_pipeline,
        catalog_with_validations,
        has_online_checks,
    ):
        """Test that validation workflow is executed when dataset has online checks and not
        executed when dataset has no online checks"""
        mock_session = MagicMock(spec=DataSentinelSession)
        mock_audit_store_manager = MagicMock()

        # Disable audit logging
        mock_audit_store_manager.count.return_value = 0
        mock_session.audit_store_manager = mock_audit_store_manager

        mock_validation_config = MagicMock()
        mock_validation_config.has_online_checks = has_online_checks
        mock_validation_workflow = MagicMock()
        mock_validation_config.create_validation_workflow.return_value = mock_validation_workflow

        bootstrap_project(kedro_project_with_datasentinel_conf)
        with KedroSession.create(
            project_path=kedro_project_with_datasentinel_conf,
        ) as session:
            context = session.load_context()

            with (
                patch(
                    "kedro_datasentinel.framework.hooks.datasentinel_hooks."
                    "DataSentinelHooks._init_session",
                    return_value=mock_session,
                ),
                patch(
                    "kedro_datasentinel.framework.hooks.datasentinel_hooks."
                    "ValidationWorkflowConfig",
                    return_value=mock_validation_config,
                ),
            ):
                runner = ThreadRunner()
                datasentinel_hook = DataSentinelHooks()
                datasentinel_hook.after_context_created(context)

                datasentinel_hook.before_pipeline_run(
                    run_params={},
                )

                hook_manager = _create_hook_manager()
                _register_hooks(hook_manager, (datasentinel_hook,))

                runner.run(
                    pipeline=dummy_pipeline,
                    catalog=catalog_with_validations,
                    hook_manager=hook_manager,
                )

        if has_online_checks:
            mock_session.run_validation_workflow.assert_called_once_with(mock_validation_workflow)
        else:
            mock_session.run_validation_workflow.assert_not_called()
