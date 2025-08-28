from collections.abc import Callable
import operator
from typing import Any

from datasentinel.validation.check.core import AbstractCheck
from datasentinel.validation.check.level import CheckLevel
from datasentinel.validation.core import NotifyOnEvent
from datasentinel.validation.data_asset.memory import MemoryDataAsset
from datasentinel.validation.data_validation import DataValidation
from datasentinel.validation.workflow import ValidationWorkflow
from pydantic import BaseModel, Field, field_validator

from kedro_datasentinel.core import Mode, RuleNotImplementedError
from kedro_datasentinel.utils import try_load_obj, try_load_obj_from_class_paths


class RuleConfig(BaseModel):
    name: str

    class Config:
        # Allow passing extra fields
        extra = "allow"


class CheckConfig(BaseModel):
    type: str
    mode: Mode
    level: CheckLevel
    rules: list[RuleConfig] | None = Field(default_factory=list)

    class Config:
        # Allow passing extra fields
        extra = "allow"

    @field_validator("rules")
    def fill_empty_rules(value):
        return [] if value is None else value

    @field_validator("level", mode="before")
    def map_level_value(value):
        if isinstance(value, CheckLevel) or isinstance(value, int):
            return value
        _check_level_map = {
            "WARNING": 0,
            "ERROR": 1,
            "CRITICAL": 2,
        }

        if value not in _check_level_map:
            raise ValueError(
                f"Invalid level '{value}' it should be one of {list(_check_level_map.keys())}"
            )
        return _check_level_map[value]

    def create_check(self, name: str) -> AbstractCheck:
        check = self._create_check_obj(name=name)
        if self.rules:
            self._add_rules_to_check(check=check, rules=self.rules)
        return check

    def _create_check_obj(self, name: str) -> AbstractCheck:
        class_path = self.type
        class_obj = try_load_obj_from_class_paths(
            class_paths=[class_path, f"datasentinel.validation.check.{class_path}"]
        )
        if class_obj is None:
            raise ValueError(
                f"The check class path '{class_path}' is not valid, it should be a full path like"
                "'datasentinel.validation.check.CualleeCheck'. For custom checks, a full class "
                "path is required"
            )

        check_obj_args = self.model_dump(exclude={"rules", "mode", "type"})

        check_obj_args["name"] = name

        return class_obj(**check_obj_args)

    def _add_rules_to_check(self, check: AbstractCheck, rules: list[RuleConfig]) -> AbstractCheck:
        for rule in rules:
            if not hasattr(check, rule.name):
                raise RuleNotImplementedError(
                    f"Rule '{rule.name}' is not implemented in check '{check.name}' class "
                    f"({check.__class__.__name__})."
                )

            rule_args = rule.model_dump(exclude={"name"})
            if "fn" in rule_args:
                rule_args["fn"] = self._get_rule_custom_function(rule_args["fn"])

            operator.methodcaller(rule.name, **rule_args)(check)
        return check

    @staticmethod
    def _get_rule_custom_function(fn_path: str) -> Callable:
        fn = try_load_obj(fn_path)

        if fn is None:
            raise ValueError(f"Could not load the function from path '{fn_path}'")

        return fn


class ValidationWorkflowConfig(BaseModel):
    name: str | None = None
    data_asset: str | None = None
    data_asset_schema: str | None = None
    check_list: dict[str, CheckConfig]
    result_stores: list[str] | None = Field(default_factory=list)
    notifiers_by_events: dict[NotifyOnEvent, list[str]] | None = Field(default_factory=dict)

    class Config:
        # raise an error if an unknown key is passed to the constructor
        extra = "forbid"

    @field_validator("result_stores")
    def result_stores_validator(value):
        return [] if value is None else value

    @field_validator("notifiers_by_events")
    def notifiers_by_events_validator(value):
        return {} if value is None else value

    @property
    def has_online_checks(self) -> bool:
        return any(
            [
                check_conf.mode in {Mode.ONLINE, Mode.BOTH}
                for check_conf in self.check_list.values()
            ]
        )

    @property
    def has_offline_checks(self) -> bool:
        return any(
            [
                check_conf.mode in {Mode.OFFLINE, Mode.BOTH}
                for check_conf in self.check_list.values()
            ]
        )

    def create_validation_workflow(
        self, dataset_name: str, data: Any, mode: Mode
    ) -> ValidationWorkflow:
        return ValidationWorkflow(
            data_validation=DataValidation(
                name=self.name or f"{dataset_name}_validation",
                data_asset=MemoryDataAsset(
                    name=self.data_asset or dataset_name, schema=self.data_asset_schema, data=data
                ),
                check_list=[
                    check_conf.create_check(name=name)
                    for name, check_conf in self.check_list.items()
                    if check_conf.mode in {mode, Mode.BOTH}
                ],
            ),
            result_stores=self.result_stores,
            notifiers_by_event=self.notifiers_by_events if mode == Mode.ONLINE else {},
        )
