from unittest.mock import MagicMock, patch

from datasentinel.session import DataSentinelSession
from datasentinel.validation.runner.core import CriticalCheckFailedError
from kedro.framework.hooks import _create_hook_manager
from kedro.framework.hooks.manager import _register_hooks
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import Pipeline, node
from kedro.runner import ThreadRunner
from pandas import DataFrame
import pytest

from kedro_datasentinel.framework.hooks.datasentinel_hooks import DataSentinelHooks


@pytest.fixture
def create_pipeline():
    def _create(add_exception: bool = False):
        def extract_data():
            return DataFrame(data={"col1": [1, 2, 3]})

        def transform_data(data: DataFrame):
            if add_exception:
                raise ValueError("Test exception")
            return data

        return Pipeline(
            nodes=[
                node(
                    func=extract_data,
                    inputs=None,
                    outputs="raw_data",
                    name="extract_data",
                ),
                node(
                    func=transform_data,
                    inputs="raw_data",
                    outputs="cleaned_data",
                    name="transform_data",
                ),
            ]
        )

    return _create


@pytest.fixture
def dummy_catalog():
    return DataCatalog(
        {
            "raw_data": MemoryDataset(),
            "cleaned_data": MemoryDataset(),
        }
    )


@pytest.fixture
def dummy_run_params(tmp_path):
    dummy_run_params = {
        "project_path": tmp_path.as_posix(),
        "session_id": "20250101",
        "env": "local",
        "kedro_version": "0.19.14",
        "tags": [],
        "from_nodes": ["extract_data", "transform_data"],
        "to_nodes": {"transform_data"},
        "node_names": {"extract_data", "transform_data"},
        "from_inputs": [],
        "load_versions": [],
        "pipeline_name": "my_pipeline",
        "extra_params": {"key": "value"},
    }
    return dummy_run_params


@pytest.mark.unit
class TestAuditLoggingUnit:
    @pytest.mark.parametrize(
        "add_exception", [True, False], ids=["with_exception", "without_exception"]
    )
    def test_audit_logging_enabled(
        self,
        add_exception,
        kedro_project_with_datasentinel_conf,
        create_pipeline,
        dummy_catalog,
        dummy_run_params,
    ):
        """Test audit logging enabled"""
        dummy_pipeline = create_pipeline(add_exception=add_exception)
        mock_session = MagicMock(spec=DataSentinelSession)
        mock_audit_store_manager = MagicMock()

        # Audit logging is enabled when the available audit store's count is 1 or more
        mock_audit_store_manager.count.return_value = 1
        mock_session.audit_store_manager = mock_audit_store_manager

        bootstrap_project(kedro_project_with_datasentinel_conf)
        with KedroSession.create(
            project_path=kedro_project_with_datasentinel_conf,
        ) as session:
            context = session.load_context()

            with patch(
                "kedro_datasentinel.framework.hooks.datasentinel_hooks."
                "DataSentinelHooks._init_session",
                return_value=mock_session,
            ):
                runner = ThreadRunner()
                datasentinel_hook = DataSentinelHooks()
                datasentinel_hook.after_context_created(context)

                datasentinel_hook.before_pipeline_run(
                    run_params=dummy_run_params,
                )

                hook_manager = _create_hook_manager()
                _register_hooks(hook_manager, (datasentinel_hook,))
                if add_exception:
                    with pytest.raises(ValueError, match="Test exception"):
                        runner.run(
                            pipeline=dummy_pipeline,
                            catalog=dummy_catalog,
                            hook_manager=hook_manager,
                        )
                else:
                    runner.run(
                        pipeline=dummy_pipeline, catalog=dummy_catalog, hook_manager=hook_manager
                    )

        assert mock_audit_store_manager.count.call_count == 1
        # Each node run generates two events, one when it starts and one when it finishes or fails
        # so the total number of events is 2*(number of nodes executed in the pipeline)
        expected_log_append_events = 2 * len(dummy_pipeline.nodes)
        assert (
            mock_audit_store_manager.append_to_all_stores.call_count == expected_log_append_events
        )

        call_args_list = mock_audit_store_manager.append_to_all_stores.call_args_list

        # Check the first call to append_to_all_stores
        row = call_args_list[0][1]["row"]
        assert row.pipeline_name == "my_pipeline"
        assert row.node_name == "extract_data"
        assert row.env == "local"
        assert row.inputs is None
        assert row.outputs is None
        row.node_names.sort()
        assert row.node_names == ["extract_data", "transform_data"]
        assert row.from_nodes == ["extract_data", "transform_data"]
        assert row.event == "STARTED"

        # Check the second call to append_to_all_stores
        row = call_args_list[1][1]["row"]
        assert row.pipeline_name == "my_pipeline"
        assert row.node_name == "extract_data"
        assert row.inputs is None
        assert row.outputs == ["raw_data"]
        row.node_names.sort()
        assert row.event == "COMPLETED"

        # Check the third call to append_to_all_stores
        row = call_args_list[2][1]["row"]
        assert row.pipeline_name == "my_pipeline"
        assert row.node_name == "transform_data"
        assert row.inputs == ["raw_data"]
        assert row.outputs is None
        row.node_names.sort()
        assert row.event == "STARTED"

        # Check the fourth call to append_to_all_stores
        row = call_args_list[3][1]["row"]
        assert row.pipeline_name == "my_pipeline"
        assert row.node_name == "transform_data"
        assert row.inputs == ["raw_data"]
        if add_exception:
            assert row.outputs is None
        else:
            assert row.outputs == ["cleaned_data"]
        row.node_names.sort()
        assert row.event == "COMPLETED" if not add_exception else "FAILED"

    def test_audit_logging_disabled(
        self,
        kedro_project_with_datasentinel_conf,
        create_pipeline,
        dummy_catalog,
        dummy_run_params,
    ):
        """Test audit logging disabled"""
        dummy_pipeline = create_pipeline(add_exception=False)
        mock_session = MagicMock(spec=DataSentinelSession)
        mock_audit_store_manager = MagicMock()

        # Audit logging is disabled when the available audit store's count is 0
        mock_audit_store_manager.count.return_value = 0
        mock_session.audit_store_manager = mock_audit_store_manager

        bootstrap_project(kedro_project_with_datasentinel_conf)
        with KedroSession.create(
            project_path=kedro_project_with_datasentinel_conf,
        ) as session:
            context = session.load_context()

            with patch(
                "kedro_datasentinel.framework.hooks.datasentinel_hooks."
                "DataSentinelHooks._init_session",
                return_value=mock_session,
            ):
                runner = ThreadRunner()
                datasentinel_hook = DataSentinelHooks()
                datasentinel_hook.after_context_created(context)

                datasentinel_hook.before_pipeline_run(
                    run_params=dummy_run_params,
                )

                hook_manager = _create_hook_manager()
                _register_hooks(hook_manager, (datasentinel_hook,))

                runner.run(
                    pipeline=dummy_pipeline, catalog=dummy_catalog, hook_manager=hook_manager
                )

        assert mock_audit_store_manager.count.call_count == 1
        # No events should be appended/logged when audit logging is disabled
        assert mock_audit_store_manager.append_to_all_stores.call_count == 0

    def test_audit_logging_with_data_validation_exception(
        self,
        kedro_project_with_datasentinel_conf,
        create_pipeline,
        dummy_catalog,
        dummy_run_params,
    ):
        """Test audit logging with data validation exception"""
        dummy_pipeline = create_pipeline(add_exception=False)
        mock_session = MagicMock(spec=DataSentinelSession)
        mock_audit_store_manager = MagicMock()

        # Audit logging is enabled when the available audit store's count is 1 or more
        mock_audit_store_manager.count.return_value = 1
        mock_session.audit_store_manager = mock_audit_store_manager

        bootstrap_project(kedro_project_with_datasentinel_conf)
        with KedroSession.create(
            project_path=kedro_project_with_datasentinel_conf,
        ) as session:
            context = session.load_context()

            with patch.multiple(
                DataSentinelHooks,
                _init_session=MagicMock(return_value=mock_session),
                _run_online_validations=MagicMock(
                    side_effect=CriticalCheckFailedError("Validation failed")
                ),
            ):
                runner = ThreadRunner()
                datasentinel_hook = DataSentinelHooks()
                datasentinel_hook.after_context_created(context)

                datasentinel_hook.before_pipeline_run(
                    run_params=dummy_run_params,
                )

                hook_manager = _create_hook_manager()
                _register_hooks(hook_manager, (datasentinel_hook,))

                with pytest.raises(CriticalCheckFailedError, match="Validation failed"):
                    runner.run(
                        pipeline=dummy_pipeline, catalog=dummy_catalog, hook_manager=hook_manager
                    )

        assert mock_audit_store_manager.count.call_count == 1
        # Two events should be appended/logged because only the first node ran as it
        # raised a CriticalCheckFailedError exception, causing the second node
        # to be skipped.
        assert mock_audit_store_manager.append_to_all_stores.call_count == 2

        # Check the second call to append_to_all_stores, it should have FAILED as the event
        # and the exception should be the CriticalCheckFailedError exception.
        call_args_list = mock_audit_store_manager.append_to_all_stores.call_args_list
        row = call_args_list[1][1]["row"]
        assert row.event == "FAILED"
        assert row.exception == "CriticalCheckFailedError: Validation failed"
