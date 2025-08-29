from unittest.mock import Mock, patch

from kedro.framework.context import KedroContext
from pydantic import ValidationError
import pytest

from kedro_datasentinel.config.data_sentinel_session import (
    AuditStoreConfig,
    DataSentinelSessionConfig,
    NotifierConfig,
    ResultStoreConfig,
    make_credentials_loader,
)


@pytest.mark.unit
class TestDataSentinelSessionUnit:
    """Test suite for DataSentinelSessionConfig and related configuration classes."""

    def test_notifier_config_initialization(self):
        """Test NotifierConfig initialization with valid parameters."""
        config = NotifierConfig(type="email.EmailNotifier")
        assert config.type == "email.EmailNotifier"
        assert config.disabled is False

        # Test with disabled=True
        config = NotifierConfig(type="email.EmailNotifier", disabled=True)
        assert config.disabled is True

        # Test with extra fields
        config = NotifierConfig(type="email.EmailNotifier", extra_param="value")
        assert hasattr(config, "extra_param")
        assert config.extra_param == "value"

    def test_result_store_config_initialization(self):
        """Test ResultStoreConfig initialization with valid parameters."""
        config = ResultStoreConfig(type="text.CSVResultStore")
        assert config.type == "text.CSVResultStore"
        assert config.disabled is False

        # Test with disabled=True
        config = ResultStoreConfig(type="text.CSVResultStore", disabled=True)
        assert config.disabled is True

        # Test with extra fields
        config = ResultStoreConfig(type="text.CSVResultStore", path="/tmp/results")  # noqa: S108
        assert hasattr(config, "path")
        assert config.path == "/tmp/results"  # noqa: S108

    def test_audit_store_config_initialization(self):
        """Test AuditStoreConfig initialization with valid parameters."""
        config = AuditStoreConfig(type="text.CSVAuditStore")
        assert config.type == "text.CSVAuditStore"
        assert config.disabled is False

        # Test with disabled=True
        config = AuditStoreConfig(type="text.CSVAuditStore", disabled=True)
        assert config.disabled is True

        # Test with extra fields
        config = AuditStoreConfig(type="text.CSVAuditStore", path="/tmp/audit")  # noqa: S108
        assert hasattr(config, "path")
        assert config.path == "/tmp/audit"  # noqa: S108

    def test_data_sentinel_session_config_initialization(self):
        """Test DataSentinelSessionConfig initialization with valid parameters."""
        # Test with default values
        config = DataSentinelSessionConfig()
        assert config.session_name is None
        assert config.result_stores == {}
        assert config.notifiers == {}
        assert config.audit_stores == {}

        # Test with custom session name
        config = DataSentinelSessionConfig(session_name="test_session")
        assert config.session_name == "test_session"

        # Test with stores and notifiers
        config = DataSentinelSessionConfig(
            session_name="test_session",
            result_stores={"csv": ResultStoreConfig(type="text.CSVResultStore")},
            notifiers={"email": NotifierConfig(type="email.EmailNotifier")},
            audit_stores={"csv": AuditStoreConfig(type="text.CSVAuditStore")},
        )
        assert config.session_name == "test_session"
        assert "csv" in config.result_stores
        assert "email" in config.notifiers
        assert "csv" in config.audit_stores

    def test_data_sentinel_session_config_validation(self):
        """Test DataSentinelSessionConfig validation."""
        # Test with invalid extra field
        with pytest.raises(ValidationError):
            DataSentinelSessionConfig(invalid_field="value")

    @patch("kedro_datasentinel.config.data_sentinel_session.DataSentinelSession")
    def test_create_session_empty_config(self, mock_data_sentinel_session):
        """Test create_session with empty configuration."""
        # Setup mock
        mock_session = Mock()
        mock_data_sentinel_session.get_or_create.return_value = mock_session

        # Create config and session
        config = DataSentinelSessionConfig(session_name="test_session")
        mock_context = Mock(spec=KedroContext)

        session = config.create_session(mock_context)

        # Assertions
        mock_data_sentinel_session.get_or_create.assert_called_once_with("test_session")
        assert session == mock_session
        # No stores or notifiers should be registered
        assert not mock_session.notifier_manager.register.called
        assert not mock_session.result_store_manager.register.called
        assert not mock_session.audit_store_manager.register.called

    @patch("kedro_datasentinel.config.data_sentinel_session.try_load_obj_from_class_paths")
    @patch("kedro_datasentinel.config.data_sentinel_session.DataSentinelSession")
    def test_create_session_with_notifier(self, mock_data_sentinel_session, mock_try_load_obj):
        """Test create_session with notifier configuration."""
        # Setup mocks
        mock_session = Mock()
        mock_data_sentinel_session.get_or_create.return_value = mock_session
        mock_notifier_class = Mock()
        mock_try_load_obj.return_value = mock_notifier_class

        # Create config with notifier
        config = DataSentinelSessionConfig(
            session_name="test_session",
            notifiers={"email": NotifierConfig(type="email.EmailNotifier")},
        )
        mock_context = Mock(spec=KedroContext)

        session = config.create_session(mock_context)

        # Assertions
        mock_data_sentinel_session.get_or_create.assert_called_once_with("test_session")
        assert session == mock_session
        mock_session.notifier_manager.register.assert_called_once()
        mock_notifier_class.assert_called_once()

    @patch("kedro_datasentinel.config.data_sentinel_session.try_load_obj_from_class_paths")
    @patch("kedro_datasentinel.config.data_sentinel_session.DataSentinelSession")
    def test_create_session_with_result_store(
        self, mock_data_sentinel_session, mock_try_load_obj
    ):
        """Test create_session with result store configuration."""
        # Setup mocks
        mock_session = Mock()
        mock_data_sentinel_session.get_or_create.return_value = mock_session
        mock_store_class = Mock()
        mock_try_load_obj.return_value = mock_store_class

        # Create config with result store
        config = DataSentinelSessionConfig(
            session_name="test_session",
            result_stores={"csv": ResultStoreConfig(type="text.CSVResultStore")},
        )
        mock_context = Mock(spec=KedroContext)

        session = config.create_session(mock_context)

        # Assertions
        mock_data_sentinel_session.get_or_create.assert_called_once_with("test_session")
        assert session == mock_session
        mock_session.result_store_manager.register.assert_called_once()
        mock_store_class.assert_called_once()

    @patch("kedro_datasentinel.config.data_sentinel_session.try_load_obj_from_class_paths")
    @patch("kedro_datasentinel.config.data_sentinel_session.DataSentinelSession")
    def test_create_session_with_audit_store(self, mock_data_sentinel_session, mock_try_load_obj):
        """Test create_session with audit store configuration."""
        # Setup mocks
        mock_session = Mock()
        mock_data_sentinel_session.get_or_create.return_value = mock_session
        mock_store_class = Mock()
        mock_try_load_obj.return_value = mock_store_class

        # Create config with audit store
        config = DataSentinelSessionConfig(
            session_name="test_session",
            audit_stores={"csv": AuditStoreConfig(type="text.CSVAuditStore")},
        )
        mock_context = Mock(spec=KedroContext)

        session = config.create_session(mock_context)

        # Assertions
        mock_data_sentinel_session.get_or_create.assert_called_once_with("test_session")
        assert session == mock_session
        mock_session.audit_store_manager.register.assert_called_once()
        mock_store_class.assert_called_once()

    def test_make_credentials_loader(self):
        """Test make_credentials_loader function."""
        # Setup mock context
        mock_context = Mock(spec=KedroContext)
        mock_context._get_config_credentials.return_value = {
            "test_creds": {"username": "user", "password": "pass"}
        }

        # Create credentials loader
        credentials_loader = make_credentials_loader(context=mock_context)

        # Test loading credentials
        creds = credentials_loader("test_creds")
        assert creds == {"username": "user", "password": "pass"}

        # Test loading non-existent credentials
        creds = credentials_loader("non_existent")
        assert creds is None

        # Verify credentials are loaded only once
        mock_context._get_config_credentials.assert_called_once()

    @patch("kedro_datasentinel.config.data_sentinel_session.try_load_obj_from_class_paths")
    @patch("kedro_datasentinel.config.data_sentinel_session.DataSentinelSession")
    def test_create_session_with_invalid_notifier_class(
        self, mock_data_sentinel_session, mock_try_load_obj
    ):
        """Test create_session with invalid notifier class."""
        # Setup mocks
        mock_session = Mock()
        mock_data_sentinel_session.get_or_create.return_value = mock_session
        mock_try_load_obj.return_value = None  # Simulate class not found

        # Create config with notifier
        config = DataSentinelSessionConfig(
            session_name="test_session",
            notifiers={"email": NotifierConfig(type="invalid.EmailNotifier")},
        )
        mock_context = Mock(spec=KedroContext)

        # Should raise ValueError
        with pytest.raises(ValueError) as excinfo:
            config.create_session(mock_context)

        # Check error message
        assert "The notifier class path" in str(excinfo.value)
        assert "invalid.EmailNotifier" in str(excinfo.value)

    @patch("kedro_datasentinel.config.data_sentinel_session.try_load_obj_from_class_paths")
    @patch("kedro_datasentinel.config.data_sentinel_session.DataSentinelSession")
    def test_create_session_with_missing_credentials(
        self, mock_data_sentinel_session, mock_try_load_obj
    ):
        """Test create_session with missing credentials."""
        # Setup mocks
        mock_session = Mock()
        mock_data_sentinel_session.get_or_create.return_value = mock_session
        mock_notifier_class = Mock()
        mock_try_load_obj.return_value = mock_notifier_class

        # Create config with notifier that requires credentials
        config = DataSentinelSessionConfig(
            session_name="test_session",
            notifiers={
                "email": NotifierConfig(type="email.EmailNotifier", credentials="missing_creds")
            },
        )

        # Setup context with credentials loader that returns None
        mock_context = Mock(spec=KedroContext)
        mock_context._get_config_credentials.return_value = {}

        # Should raise KeyError
        with pytest.raises(KeyError) as excinfo:
            config.create_session(mock_context)

        # Check error message
        assert "Could not find credentials with key" in str(excinfo.value)
        assert "missing_creds" in str(excinfo.value)

    @patch("kedro_datasentinel.config.data_sentinel_session.try_load_obj_from_class_paths")
    @patch("kedro_datasentinel.config.data_sentinel_session.DataSentinelSession")
    def test_create_session_with_nested_config_objects(
        self, mock_data_sentinel_session, mock_try_load_obj
    ):
        """Test create_session with nested configuration objects."""
        # Setup mocks for multiple class loads
        mock_session = Mock()
        mock_data_sentinel_session.get_or_create.return_value = mock_session

        # Mock for the main notifier class
        mock_notifier_class = Mock()
        # Mock for a nested renderer class
        mock_renderer_class = Mock()

        # Setup try_load_obj_from_class_paths to return different classes based on input
        def side_effect(class_paths):
            if any("email.EmailNotifier" in path for path in class_paths):
                return mock_notifier_class
            elif any("html.HTMLRenderer" in path for path in class_paths):
                return mock_renderer_class
            return None

        mock_try_load_obj.side_effect = side_effect

        # Create config with notifier that has nested renderer config
        config = DataSentinelSessionConfig(
            session_name="test_session",
            notifiers={
                "email": NotifierConfig(
                    type="email.EmailNotifier",
                    renderer={"type": "html.HTMLRenderer", "template": "default.html"},
                )
            },
        )

        mock_context = Mock(spec=KedroContext)
        session = config.create_session(mock_context)

        # Assertions
        assert session == mock_session
        mock_session.notifier_manager.register.assert_called_once()
        mock_notifier_class.assert_called_once()
        # Verify the renderer was created
        mock_renderer_class.assert_called_once_with(template="default.html")

    def test_model_validator_for_empty_stores(self):
        """Test the model_validator that sets empty stores and notifiers."""
        # Test with None values
        config = DataSentinelSessionConfig(
            session_name="test_session", result_stores=None, notifiers=None, audit_stores=None
        )

        # Verify empty dicts are created
        assert config.result_stores == {}
        assert config.notifiers == {}
        assert config.audit_stores == {}

    @patch("kedro_datasentinel.config.data_sentinel_session.try_load_obj_from_class_paths")
    @patch("kedro_datasentinel.config.data_sentinel_session.DataSentinelSession")
    def test_create_session_with_invalid_result_store_class(
        self, mock_data_sentinel_session, mock_try_load_obj
    ):
        """Test create_session with invalid result store class."""
        # Setup mocks
        mock_session = Mock()
        mock_data_sentinel_session.get_or_create.return_value = mock_session
        mock_try_load_obj.return_value = None  # Simulate class not found

        # Create config with result store
        config = DataSentinelSessionConfig(
            session_name="test_session",
            result_stores={"csv": ResultStoreConfig(type="invalid.CSVResultStore")},
        )
        mock_context = Mock(spec=KedroContext)

        # Should raise ValueError
        with pytest.raises(ValueError) as excinfo:
            config.create_session(mock_context)

        # Check error message
        assert "The result store class path" in str(excinfo.value)
        assert "invalid.CSVResultStore" in str(excinfo.value)

    @patch("kedro_datasentinel.config.data_sentinel_session.try_load_obj_from_class_paths")
    @patch("kedro_datasentinel.config.data_sentinel_session.DataSentinelSession")
    def test_create_session_with_invalid_audit_store_class(
        self, mock_data_sentinel_session, mock_try_load_obj
    ):
        """Test create_session with invalid audit store class."""
        # Setup mocks
        mock_session = Mock()
        mock_data_sentinel_session.get_or_create.return_value = mock_session
        mock_try_load_obj.return_value = None  # Simulate class not found

        # Create config with audit store
        config = DataSentinelSessionConfig(
            session_name="test_session",
            audit_stores={"csv": AuditStoreConfig(type="invalid.CSVAuditStore")},
        )
        mock_context = Mock(spec=KedroContext)

        # Should raise ValueError
        with pytest.raises(ValueError) as excinfo:
            config.create_session(mock_context)

        # Check error message
        assert "The audit store class path" in str(excinfo.value)
        assert "invalid.CSVAuditStore" in str(excinfo.value)

    @pytest.mark.parametrize(
        "session_inputs",
        [
            {
                "result_stores": {
                    "result_store": ResultStoreConfig(
                        type="ResultStore", credentials="missing_creds"
                    )
                }
            },
            {
                "audit_stores": {
                    "audit_store": AuditStoreConfig(
                        type="AuditStore", credentials="missing_creds"
                    )
                }
            },
            {
                "notifiers": {
                    "notifier": NotifierConfig(type="Notifier", credentials="missing_creds")
                }
            },
        ],
        ids=[
            "result_store_missing_credentials",
            "audit_store_missing_credentials",
            "notifier_missing_credentials",
        ],
    )
    @patch("kedro_datasentinel.config.data_sentinel_session.try_load_obj_from_class_paths")
    @patch("kedro_datasentinel.config.data_sentinel_session.DataSentinelSession")
    def test_create_session_missing_credentials(
        self,
        mock_data_sentinel_session,
        mock_try_load_obj,
        session_inputs: dict,
    ):
        """Test create_session with result store missing credentials."""
        # Setup mocks
        mock_session = Mock()
        mock_data_sentinel_session.get_or_create.return_value = mock_session
        mock_store_class = Mock()
        mock_try_load_obj.return_value = mock_store_class

        # Create config with result store that requires credentials
        config = DataSentinelSessionConfig(
            session_name="test_session",
            **session_inputs,
        )

        # Setup context with credentials loader that returns None
        mock_context = Mock(spec=KedroContext)
        mock_context._get_config_credentials.return_value = {}

        # Should raise KeyError
        with pytest.raises(KeyError, match="Could not find credentials with key"):
            config.create_session(mock_context)

    @patch("kedro_datasentinel.config.data_sentinel_session.try_load_obj_from_class_paths")
    @patch("kedro_datasentinel.config.data_sentinel_session.DataSentinelSession")
    def test_create_session_with_stores_and_notifiers_with_credentials(
        self, mock_data_sentinel_session, mock_try_load_obj
    ):
        """Test create_session with stores and notifier configuration that uses credentials."""
        # Setup mocks
        mock_session = Mock()
        mock_data_sentinel_session.get_or_create.return_value = mock_session

        # Mock for the notifier class
        mock_notifier = Mock()
        # Mock for a audit store class
        mock_audit_store = Mock()
        # Mock for a result store class
        mock_result_store = Mock()

        # Setup try_load_obj_from_class_paths to return different classes based on input
        def side_effect(class_paths):
            if any("DummyNotifier" in path for path in class_paths):
                return mock_notifier
            elif any("DummyAuditStore" in path for path in class_paths):
                return mock_audit_store
            elif any("DummyResultStore" in path for path in class_paths):
                return mock_result_store
            return None

        mock_try_load_obj.side_effect = side_effect

        # Create config with audit store that requires credentials
        config = DataSentinelSessionConfig(
            session_name="test_session",
            notifiers={"notifier": NotifierConfig(type="DummyNotifier", credentials="creds1")},
            result_stores={
                "result_store": ResultStoreConfig(type="DummyResultStore", credentials="creds2"),
            },
            audit_stores={
                "audit_store": AuditStoreConfig(type="DummyAuditStore", credentials="creds3")
            },
        )

        # Setup context with credentials
        mock_context = Mock(spec=KedroContext)
        mock_context._get_config_credentials.return_value = {
            "creds1": {"username": "user", "password": "pass"},
            "creds2": {"username": "user", "password": "pass"},
            "creds3": {"username": "user", "password": "pass"},
        }

        session = config.create_session(mock_context)

        # Assertions
        mock_data_sentinel_session.get_or_create.assert_called_once_with("test_session")
        assert session == mock_session
        # Assert that the notifier was registered
        mock_session.notifier_manager.register.assert_called_once()
        mock_notifier.assert_called_once_with(
            name="notifier", credentials={"username": "user", "password": "pass"}, disabled=False
        )
        # Assert that the audit store was registered
        mock_session.audit_store_manager.register.assert_called_once()
        mock_audit_store.assert_called_once_with(
            name="audit_store",
            credentials={"username": "user", "password": "pass"},
            disabled=False,
        )
        # Assert that the result store was registered
        mock_session.result_store_manager.register.assert_called_once()
        mock_result_store.assert_called_once_with(
            name="result_store",
            credentials={"username": "user", "password": "pass"},
            disabled=False,
        )
