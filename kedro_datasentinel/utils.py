import importlib
from typing import Any

from kedro.io import AbstractDataset


def is_relative_class_path(class_path: str) -> bool:
    return not len(class_path.strip(".")) == len(class_path)


def load_obj(class_path: str) -> Any:
    if is_relative_class_path(class_path):
        raise ValueError("Can not use relative paths while defining an object class path")

    module_path, object_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, object_name)


def try_load_obj(class_path: str) -> Any | None:
    try:
        class_obj = load_obj(class_path)
    except ModuleNotFoundError as e:
        # Only return None if the module not found is the class_path module itself
        # If it's a dependency within the module, re-raise the exception
        module_path = class_path.rsplit(".", 1)[0]
        if module_path in str(e):
            return None
        else:
            # The ModuleNotFoundError is for a dependency, not the target module
            raise
    except (ValueError, AttributeError):
        return None
    return class_obj


def try_load_obj_from_class_paths(class_paths: list[str]) -> Any | None:
    class_obj = None
    for full_class_path in class_paths:
        class_obj = try_load_obj(full_class_path)
        if class_obj is not None:
            break
    return class_obj


def dataset_has_validations(dataset: AbstractDataset) -> bool:
    return (
        hasattr(dataset, "metadata")
        and dataset.metadata
        and "kedro-datasentinel" in dataset.metadata
        and dataset.metadata.get("kedro-datasentinel")
    )


def exception_to_str(exception: Exception) -> str:
    return f"{type(exception).__name__}: {exception!s}"
