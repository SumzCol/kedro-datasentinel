from unittest.mock import Mock, patch

from datasentinel.validation.check.level import CheckLevel
from datasentinel.validation.core import NotifyOnEvent
import pytest

from kedro_datasentinel.config.data_validation import (
    CheckConfig,
    RuleConfig,
    ValidationWorkflowConfig,
)
from kedro_datasentinel.core import Mode, RuleNotImplementedError


@pytest.mark.unit
class TestDataValidationUnit:
    """Test suite for data validation configuration classes."""

    def test_rule_config_initialization(self):
        """Test RuleConfig initialization with valid parameters."""
        config = RuleConfig(name="is_not_null")
        assert config.name == "is_not_null"

        # Test with extra fields
        config = RuleConfig(name="is_not_null", threshold=0.9)
        assert config.threshold == 0.9

    @pytest.mark.parametrize(
        "type_val, mode_val, level_val, rules_val, expected_rules",
        [
            ("CualleeCheck", Mode.ONLINE, CheckLevel.ERROR, None, []),
            (
                "CualleeCheck",
                Mode.ONLINE,
                CheckLevel.ERROR,
                [RuleConfig(name="is_not_null")],
                [RuleConfig(name="is_not_null")],
            ),
        ],
        ids=[
            "CheckConfig with no rules",
            "CheckConfig with rules",
        ],
    )
    def test_check_config_initialization(
        self, type_val, mode_val, level_val, rules_val, expected_rules
    ):
        """Test CheckConfig initialization with valid parameters."""
        config = CheckConfig(type=type_val, mode=mode_val, level=level_val, rules=rules_val)
        assert config.type == type_val
        assert config.mode == mode_val
        assert config.level == level_val
        assert config.rules == expected_rules

    def test_check_config_initialization_with_extra_params(self):
        # Test with extra fields
        config = CheckConfig(
            type="CualleeCheck", mode=Mode.ONLINE, level=CheckLevel.ERROR, extra_param="value"
        )
        assert hasattr(config, "extra_param")
        assert config.extra_param == "value"

    @pytest.mark.parametrize(
        "level_val, expected_level",
        [
            ("WARNING", CheckLevel.WARNING),
            ("ERROR", CheckLevel.ERROR),
            ("CRITICAL", CheckLevel.CRITICAL),
            (CheckLevel.WARNING, CheckLevel.WARNING),
            (CheckLevel.ERROR, CheckLevel.ERROR),
            (CheckLevel.CRITICAL, CheckLevel.CRITICAL),
            (0, CheckLevel.WARNING),
            (1, CheckLevel.ERROR),
            (2, CheckLevel.CRITICAL),
        ],
        ids=[
            "Warning check level as string",
            "Error check level as string",
            "Critical check level as string",
            "Warning check level as Enum",
            "Error check level as Enum",
            "Critical check level as Enum",
            "Warning check level as int",
            "Error check level as int",
            "Critical check level as int",
        ],
    )
    def test_check_config_level_validation_valid(self, level_val, expected_level):
        """Test CheckConfig level validation with valid levels."""
        config = CheckConfig(
            type="CualleeCheck",
            mode=Mode.ONLINE,
            level=level_val,
        )
        assert config.level == expected_level

    def test_check_config_level_validation_invalid(self):
        """Test CheckConfig level validation with invalid level."""
        with pytest.raises(ValueError, match="Invalid level"):
            CheckConfig(
                type="CualleeCheck",
                mode=Mode.ONLINE,
                level="INVALID_LEVEL",
            )

    @patch("kedro_datasentinel.config.data_validation.try_load_obj_from_class_paths")
    def test_create_check_valid_class(self, mock_try_load_obj):
        """Test create_check method with valid check class."""
        # Setup mock
        mock_check = Mock()
        mock_check_class = Mock(return_value=mock_check)
        mock_try_load_obj.return_value = mock_check_class

        # Create config
        config = CheckConfig(
            type="CualleeCheck",
            mode=Mode.ONLINE,
            level=CheckLevel.ERROR,
        )

        # Create check
        check = config.create_check(name="test_check")

        # Assertions
        mock_try_load_obj.assert_called_once()
        mock_check_class.assert_called_once()
        assert check == mock_check

    @patch("kedro_datasentinel.config.data_validation.try_load_obj_from_class_paths")
    def test_create_check_invalid_class(self, mock_try_load_obj):
        """Test create_check method with invalid check class."""
        # Setup mock
        mock_try_load_obj.return_value = None

        # Create config
        config = CheckConfig(
            type="InvalidCheck",
            mode=Mode.ONLINE,
            level=CheckLevel.ERROR,
        )

        # Should raise ValueError
        with pytest.raises(ValueError, match="The check class path"):
            config.create_check(name="test_check")

    @patch("kedro_datasentinel.config.data_validation.try_load_obj_from_class_paths")
    def test_create_check_with_rules(self, mock_try_load_obj):
        """Test create_check method with rules."""
        # Setup mock check with rule methods
        mock_check = Mock()
        mock_check.is_not_null = Mock(return_value=mock_check)
        mock_check.is_unique = Mock(return_value=mock_check)
        mock_check.name = "test_check"
        mock_check.__class__.__name__ = "MockCheck"

        # Setup mock check class
        mock_check_class = Mock(return_value=mock_check)
        mock_try_load_obj.return_value = mock_check_class

        # Create config with rules
        config = CheckConfig(
            type="CualleeCheck",
            mode=Mode.ONLINE,
            level=CheckLevel.ERROR,
            rules=[RuleConfig(name="is_not_null"), RuleConfig(name="is_unique", threshold=0.9)],
        )

        # Create check
        check = config.create_check(name="test_check")

        # Assertions
        assert check == mock_check
        assert check.name == "test_check"
        mock_check.is_not_null.assert_called_once_with()
        mock_check.is_unique.assert_called_once_with(threshold=0.9)

    @patch("kedro_datasentinel.config.data_validation.try_load_obj_from_class_paths")
    def test_create_check_with_invalid_rule(self, mock_try_load_obj):
        """Test create_check method with invalid rule."""
        # Setup mock check without the required rule method
        mock_check = Mock(spec=[])  # Empty spec means no attributes are auto-created
        mock_check.name = "test_check"
        mock_check.__class__.__name__ = "MockCheck"

        # Setup mock check class
        mock_check_class = Mock(return_value=mock_check)
        mock_try_load_obj.return_value = mock_check_class

        # Create config with invalid rule
        config = CheckConfig(
            type="CualleeCheck",
            mode=Mode.ONLINE,
            level=CheckLevel.ERROR,
            rules=[RuleConfig(name="invalid_rule")],
        )

        # Should raise RuleNotImplementedError
        with pytest.raises(
            RuleNotImplementedError, match="Rule 'invalid_rule' is not implemented"
        ):
            config.create_check(name="test_check")

    @patch("kedro_datasentinel.config.data_validation.try_load_obj")
    @patch("kedro_datasentinel.config.data_validation.try_load_obj_from_class_paths")
    def test_create_check_with_custom_function(self, mock_try_load_obj_paths, mock_try_load_obj):
        """Test create_check method with custom function."""
        # Setup mocks
        mock_check = Mock()
        mock_check.custom_rule = Mock(return_value=mock_check)
        mock_check.name = "test_check"
        mock_check.__class__.__name__ = "MockCheck"

        mock_check_class = Mock(return_value=mock_check)
        mock_try_load_obj_paths.return_value = mock_check_class

        mock_fn = Mock()
        mock_try_load_obj.return_value = mock_fn

        # Create config with rule that uses custom function
        config = CheckConfig(
            type="CualleeCheck",
            mode=Mode.ONLINE,
            level=CheckLevel.ERROR,
            rules=[RuleConfig(name="custom_rule", fn="module.custom_function")],
        )

        # Create check
        check = config.create_check(name="test_check")

        # Assertions
        assert check == mock_check
        mock_check.custom_rule.assert_called_once_with(fn=mock_fn)
        mock_try_load_obj.assert_called_once_with("module.custom_function")

    @patch("kedro_datasentinel.config.data_validation.try_load_obj")
    @patch("kedro_datasentinel.config.data_validation.try_load_obj_from_class_paths")
    def test_create_check_with_invalid_custom_function(
        self, mock_try_load_obj_paths, mock_try_load_obj
    ):
        """Test create_check method with invalid custom function."""
        # Setup mocks
        mock_check = Mock()
        mock_check.custom_rule = Mock()
        mock_check.name = "test_check"
        mock_check.__class__.__name__ = "MockCheck"

        mock_check_class = Mock(return_value=mock_check)
        mock_try_load_obj_paths.return_value = mock_check_class

        # Function not found
        mock_try_load_obj.return_value = None

        # Create config with rule that uses custom function
        config = CheckConfig(
            type="CualleeCheck",
            mode=Mode.ONLINE,
            level=CheckLevel.ERROR,
            rules=[RuleConfig(name="custom_rule", fn="invalid.function")],
        )

        # Should raise ValueError
        with pytest.raises(ValueError, match="Could not load the function from path"):
            config.create_check(name="test_check")

    @pytest.mark.parametrize(
        "check_list, result_stores, notifiers_by_events",
        [
            (
                {
                    "check1": CheckConfig(
                        type="CualleeCheck", mode=Mode.ONLINE, level=CheckLevel.ERROR
                    )
                },
                None,
                None,
            ),
            (
                {
                    "check1": CheckConfig(
                        type="CualleeCheck", mode=Mode.ONLINE, level=CheckLevel.ERROR
                    )
                },
                ["csv"],
                {NotifyOnEvent.FAIL: ["email"]},
            ),
        ],
        ids=[
            "without_result_stores_and_notifiers_config",
            "with_result_stores_and_notifiers_config",
        ],
    )
    def test_validation_workflow_config_initialization(
        self, check_list, result_stores, notifiers_by_events
    ):
        """Test ValidationWorkflowConfig initialization with valid parameters."""
        config = ValidationWorkflowConfig(
            name="test_workflow",
            data_asset="test_asset",
            data_asset_schema="test_schema",
            check_list=check_list,
            result_stores=result_stores,
            notifiers_by_events=notifiers_by_events,
        )

        assert config.name == "test_workflow"
        assert config.data_asset == "test_asset"
        assert config.data_asset_schema == "test_schema"
        assert config.check_list == check_list
        assert config.result_stores == (result_stores or [])
        assert config.notifiers_by_events == (notifiers_by_events or {})

    @pytest.mark.parametrize(
        "check_configs, expected_result",
        [
            (
                {
                    "check1": CheckConfig(
                        type="CualleeCheck", mode=Mode.ONLINE, level=CheckLevel.ERROR
                    )
                },
                True,
            ),
            (
                {
                    "check1": CheckConfig(
                        type="CualleeCheck", mode=Mode.BOTH, level=CheckLevel.ERROR
                    )
                },
                True,
            ),
            (
                {
                    "check1": CheckConfig(
                        type="CualleeCheck", mode=Mode.OFFLINE, level=CheckLevel.ERROR
                    )
                },
                False,
            ),
            (
                {
                    "check1": CheckConfig(
                        type="CualleeCheck", mode=Mode.OFFLINE, level=CheckLevel.ERROR
                    ),
                    "check2": CheckConfig(
                        type="CualleeCheck", mode=Mode.ONLINE, level=CheckLevel.ERROR
                    ),
                },
                True,
            ),
        ],
        ids=[
            "check_with_online_as_mode",
            "check_with_both_as_mode",
            "check_with_offline_as_mode",
            "checks_with_mixed_modes",
        ],
    )
    def test_has_online_checks(self, check_configs, expected_result):
        """Test has_online_checks property."""
        config = ValidationWorkflowConfig(check_list=check_configs)
        assert config.has_online_checks is expected_result

    @pytest.mark.parametrize(
        "check_configs, expected_result",
        [
            (
                {
                    "check1": CheckConfig(
                        type="CualleeCheck", mode=Mode.OFFLINE, level=CheckLevel.ERROR
                    )
                },
                True,
            ),
            (
                {
                    "check1": CheckConfig(
                        type="CualleeCheck", mode=Mode.BOTH, level=CheckLevel.ERROR
                    )
                },
                True,
            ),
            (
                {
                    "check1": CheckConfig(
                        type="CualleeCheck", mode=Mode.ONLINE, level=CheckLevel.ERROR
                    )
                },
                False,
            ),
            (
                {
                    "check1": CheckConfig(
                        type="CualleeCheck", mode=Mode.ONLINE, level=CheckLevel.ERROR
                    ),
                    "check2": CheckConfig(
                        type="CualleeCheck", mode=Mode.OFFLINE, level=CheckLevel.ERROR
                    ),
                },
                True,
            ),
        ],
        ids=[
            "check_with_offline_as_mode",
            "check_with_both_as_mode",
            "check_with_online_as_mode",
            "checks_with_mixed_modes",
        ],
    )
    def test_has_offline_checks(self, check_configs, expected_result):
        """Test has_offline_checks property."""
        config = ValidationWorkflowConfig(check_list=check_configs)
        assert config.has_offline_checks is expected_result

    @pytest.mark.parametrize(
        "mode, validation_name, data_asset_name, check_configs, expected_checks, "
        "expected_notifiers",
        [
            # Test ONLINE mode with default names
            (
                Mode.ONLINE,
                None,
                None,
                {
                    "check1": CheckConfig(
                        type="CualleeCheck", mode=Mode.ONLINE, level=CheckLevel.ERROR
                    ),
                    "check2": CheckConfig(
                        type="CualleeCheck", mode=Mode.BOTH, level=CheckLevel.ERROR
                    ),
                    "check3": CheckConfig(
                        type="CualleeCheck", mode=Mode.OFFLINE, level=CheckLevel.ERROR
                    ),
                },
                2,  # Only ONLINE and BOTH checks should be included
                True,  # Notifiers should be included for ONLINE mode
            ),
            # Test OFFLINE mode with custom names
            (
                Mode.OFFLINE,
                "custom_workflow",
                "custom_asset",
                {
                    "check1": CheckConfig(
                        type="CualleeCheck", mode=Mode.ONLINE, level=CheckLevel.ERROR
                    ),
                    "check2": CheckConfig(
                        type="CualleeCheck", mode=Mode.BOTH, level=CheckLevel.ERROR
                    ),
                    "check3": CheckConfig(
                        type="CualleeCheck", mode=Mode.OFFLINE, level=CheckLevel.ERROR
                    ),
                },
                2,  # Only OFFLINE and BOTH checks should be included
                False,  # Notifiers should be empty for OFFLINE mode
            ),
        ],
        ids=["online_mode_default_names", "offline_mode_custom_names"],
    )
    @patch("kedro_datasentinel.config.data_validation.ValidationWorkflow")
    @patch("kedro_datasentinel.config.data_validation.DataValidation")
    @patch("kedro_datasentinel.config.data_validation.MemoryDataAsset")
    def test_create_validation_workflow(
        self,
        mock_memory_asset,
        mock_data_validation,
        mock_validation_workflow,
        mode,
        validation_name,
        data_asset_name,
        check_configs,
        expected_checks,
        expected_notifiers,
    ):
        """Test create_validation_workflow method with different modes and configurations."""
        # Setup mocks
        mock_asset = Mock()
        mock_memory_asset.return_value = mock_asset

        mock_validation = Mock()
        mock_data_validation.return_value = mock_validation

        mock_workflow = Mock()
        mock_validation_workflow.return_value = mock_workflow

        # Create config with test parameters
        config = ValidationWorkflowConfig(
            name=validation_name,
            data_asset=data_asset_name,
            check_list=check_configs,
            result_stores=["csv"],
            notifiers_by_events={NotifyOnEvent.FAIL: ["email"]},
        )

        # Mock check creation
        check_mocks = {}
        for name, check_conf in check_configs.items():
            check_mock = Mock()
            check_mocks[name] = check_mock

        # Call the method under test
        with patch.object(
            CheckConfig, "create_check", side_effect=lambda name: check_mocks[name]
        ):
            workflow = config.create_validation_workflow(
                dataset_name="test_dataset", data={"column1": [1, 2, 3]}, mode=mode
            )

        # Verify MemoryDataAsset creation
        expected_asset_name = data_asset_name if data_asset_name else "test_dataset"
        mock_memory_asset.assert_called_once_with(
            name=expected_asset_name, schema=None, data={"column1": [1, 2, 3]}
        )

        # Verify DataValidation creation
        expected_validation_name = (
            validation_name if validation_name else "test_dataset_validation"
        )
        mock_data_validation.assert_called_once()
        call_args = mock_data_validation.call_args[1]
        assert call_args["name"] == expected_validation_name
        assert call_args["data_asset"] == mock_asset
        assert len(call_args["check_list"]) == expected_checks

        # Verify ValidationWorkflow creation
        mock_validation_workflow.assert_called_once()
        call_args = mock_validation_workflow.call_args[1]
        assert call_args["data_validation"] == mock_validation
        assert call_args["result_stores"] == ["csv"]
        if expected_notifiers:
            assert call_args["notifiers_by_event"] == {NotifyOnEvent.FAIL: ["email"]}
        else:
            assert call_args["notifiers_by_event"] == {}

        # Verify the returned workflow
        assert workflow == mock_workflow
