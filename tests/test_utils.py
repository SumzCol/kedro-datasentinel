from unittest.mock import Mock, patch

from kedro.io import AbstractDataset
import pytest

from kedro_datasentinel.utils import (
    dataset_has_validations,
    exception_to_str,
    is_relative_class_path,
    load_obj,
    try_load_obj,
    try_load_obj_from_class_paths,
)


@pytest.mark.unit
class TestUtilsUnit:
    """Test suite for utility functions in kedro_datasentinel.utils."""

    def test_is_relative_class_path(self):
        """Test is_relative_class_path function."""
        # Test absolute paths
        assert not is_relative_class_path("package.module.Class")
        assert not is_relative_class_path("package.Class")
        assert not is_relative_class_path("Class")

        # Test relative paths
        assert is_relative_class_path(".module.Class")
        assert is_relative_class_path("..module.Class")
        assert is_relative_class_path("...module.Class")

    def test_load_obj_valid_path(self):
        """Test load_obj with valid class path."""
        # Test loading a real Python object
        obj = load_obj("os.path.join")
        assert obj is not None
        assert callable(obj)

        # Test loading another real Python object
        obj = load_obj("datetime.datetime")
        assert obj is not None

    def test_load_obj_invalid_path(self):
        """Test load_obj with invalid class path."""
        # Test with relative path
        with pytest.raises(ValueError) as excinfo:
            load_obj(".module.Class")
        assert "Can not use relative paths" in str(excinfo.value)

        # Test with non-existent module
        with pytest.raises(ModuleNotFoundError):
            load_obj("non_existent_module.Class")

        # Test with non-existent attribute
        with pytest.raises(AttributeError):
            load_obj("os.non_existent_attribute")

    def test_try_load_obj_valid_path(self):
        """Test try_load_obj with valid class path."""
        # Test loading a real Python object
        obj = try_load_obj("os.path.join")
        assert obj is not None
        assert callable(obj)

    def test_try_load_obj_invalid_path(self):
        """Test try_load_obj with invalid class path."""
        # Test with relative path
        obj = try_load_obj(".module.Class")
        assert obj is None

        # Test with non-existent module
        obj = try_load_obj("non_existent_module.Class")
        assert obj is None

        # Test with non-existent attribute
        obj = try_load_obj("os.non_existent_attribute")
        assert obj is None

    @patch("kedro_datasentinel.utils.try_load_obj")
    def test_try_load_obj_from_class_paths(self, mock_try_load_obj):
        """Test try_load_obj_from_class_paths function."""
        # Setup mock to return None for first path and a mock object for second path
        mock_obj = Mock()
        mock_try_load_obj.side_effect = [None, mock_obj, Mock()]

        # Test with multiple paths
        result = try_load_obj_from_class_paths(["path1.Class", "path2.Class", "path3.Class"])
        assert result == mock_obj
        assert mock_try_load_obj.call_count == 2  # Should stop after finding a valid object

        # Reset mock
        mock_try_load_obj.reset_mock()
        mock_try_load_obj.side_effect = [None, None, None]

        # Test with all invalid paths
        result = try_load_obj_from_class_paths(["path1.Class", "path2.Class", "path3.Class"])
        assert result is None
        assert mock_try_load_obj.call_count == 3  # Should try all paths

    def test_try_load_obj_dependency_error(self):
        """Test try_load_obj with module that has dependency errors."""

        # Create a custom ModuleNotFoundError for a dependency
        dependency_error = ModuleNotFoundError("No module named 'missing_dependency'")

        with patch("importlib.import_module") as mock_import:
            # Setup mock to raise ModuleNotFoundError for a dependency
            mock_import.side_effect = dependency_error

            # Should re-raise the exception since it's a dependency error
            with pytest.raises(ModuleNotFoundError) as excinfo:
                try_load_obj("existing.module.with.missing.dependency")

            assert "missing_dependency" in str(excinfo.value)

    def test_dataset_has_validations(self):
        """Test dataset_has_validations function."""
        # Test with dataset that has validations
        mock_dataset = Mock(spec=AbstractDataset)
        mock_dataset.metadata = {"kedro-datasentinel": {"some_validation": "exists"}}
        assert dataset_has_validations(mock_dataset) is True

        # Test with dataset that has non-dict kedro-datasentinel value
        mock_dataset = Mock(spec=AbstractDataset)
        mock_dataset.metadata = {"kedro-datasentinel": True}
        assert dataset_has_validations(mock_dataset) is False

        # Test with dataset that has empty validations dict
        mock_dataset = Mock(spec=AbstractDataset)
        mock_dataset.metadata = {"kedro-datasentinel": {}}
        assert dataset_has_validations(mock_dataset) is False

        # Test with dataset that has no kedro-datasentinel key
        mock_dataset = Mock(spec=AbstractDataset)
        mock_dataset.metadata = {"other-key": "value"}
        assert dataset_has_validations(mock_dataset) is False

        # Test with dataset that has no metadata
        mock_dataset = Mock(spec=AbstractDataset)
        mock_dataset.metadata = None
        assert dataset_has_validations(mock_dataset) is False

        # Test with dataset that has no metadata attribute
        mock_dataset = Mock(spec=AbstractDataset)
        assert dataset_has_validations(mock_dataset) is False

    def test_exception_to_str(self):
        """Test exception_to_str function."""
        # Test with ValueError
        exception = ValueError("This is a test error")
        result = exception_to_str(exception)
        assert result == "ValueError: This is a test error"

        # Test with custom exception
        class CustomException(Exception):
            pass

        exception = CustomException("Custom error message")
        result = exception_to_str(exception)
        assert result == "CustomException: Custom error message"
