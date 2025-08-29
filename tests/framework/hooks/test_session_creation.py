from unittest.mock import patch

from datasentinel.session import DataSentinelSession
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from pydantic import ValidationError
import pytest

from kedro_datasentinel.core import DataSentinelConfigError
from kedro_datasentinel.framework.hooks.datasentinel_hooks import DataSentinelHooks


@pytest.mark.unit
class TestSessionCreationUnit:
    def teardown_method(self):
        # Code to run after each test method
        DataSentinelSession._active_sessions.clear()

    def test_datasentinel_session_creation_with_datasentinel_conf(
        self, kedro_project_with_datasentinel_conf
    ):
        """Test that the DataSentinelSession is created correctly when a config file exists."""
        bootstrap_project(kedro_project_with_datasentinel_conf)
        with KedroSession.create(
            project_path=kedro_project_with_datasentinel_conf,
        ) as session:
            context = session.load_context()
            datasentinel_hook = DataSentinelHooks()
            datasentinel_hook.after_context_created(context)

            ds_session = DataSentinelSession.get_or_create()
            # Check that the session is created with the name provided in the config file
            assert ds_session.name == "example_session"
            # Check that no stores or notifiers are created as they are commented out in the
            # config file
            assert ds_session.notifier_manager.count() == 0
            assert ds_session.audit_store_manager.count() == 0
            assert ds_session.result_store_manager.count() == 0

    def test_datasentinel_session_creation_without_datasentinel_conf(self, kedro_project):
        """Test that the DataSentinelSession is created correctly when a config file does not
        exist."""
        bootstrap_project(kedro_project)
        with KedroSession.create(
            project_path=kedro_project,
        ) as session:
            context = session.load_context()
            datasentinel_hook = DataSentinelHooks()
            datasentinel_hook.after_context_created(context)

            ds_session = DataSentinelSession.get_or_create()
            # Check that the session is created with random name and not "example_session"
            # as the config file does not exist
            assert ds_session.name != "example_session"
            # Check that no stores or notifiers are created as they are not configured
            assert ds_session.notifier_manager.count() == 0
            assert ds_session.audit_store_manager.count() == 0
            assert ds_session.result_store_manager.count() == 0

    def test_datasentinel_config_error_on_invalid_config(
        self, kedro_project_with_datasentinel_conf
    ):
        """Test that DataSentinelConfigError is raised when config validation fails."""
        bootstrap_project(kedro_project_with_datasentinel_conf)
        with KedroSession.create(
            project_path=kedro_project_with_datasentinel_conf,
        ) as session:
            context = session.load_context()
            datasentinel_hook = DataSentinelHooks()

            # Mock DataSentinelSessionConfig to raise ValidationError
            with patch(
                "kedro_datasentinel.framework.hooks.datasentinel_hooks.DataSentinelSessionConfig"
            ) as mock_config:
                mock_config.side_effect = ValidationError.from_exception_data(
                    "DataSentinelSessionConfig",
                    [],
                )

                # Test that DataSentinelConfigError is raised with proper message
                with pytest.raises(DataSentinelConfigError, match="could not be parsed"):
                    datasentinel_hook.after_context_created(context)
