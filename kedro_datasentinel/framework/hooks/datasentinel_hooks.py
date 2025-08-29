from copy import deepcopy
from datetime import datetime
import logging
import os
from typing import Any

from datasentinel.session import DataSentinelSession
from kedro.config import MissingConfigException
from kedro.framework.context import KedroContext
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.pipeline.node import Node
from pydantic import ValidationError
from ulid import ULID

from kedro_datasentinel.config import DataSentinelSessionConfig, ValidationWorkflowConfig
from kedro_datasentinel.core import (
    DataSentinelConfigError,
    DataValidationConfigError,
    Event,
    Mode,
)
from kedro_datasentinel.framework.hooks.kedro_audit_row import KedroAuditRow
from kedro_datasentinel.utils import dataset_has_validations, exception_to_str


class DataSentinelHooks:
    def __init__(self):
        self._audit_enabled = False
        self._run_id = None
        self._env = None
        self._extra_params = None
        self._run_params: dict[str, Any] = {}
        self._session: DataSentinelSession | None = None

    @property
    def _logger(self) -> logging.Logger:
        return logging.getLogger(__name__)

    @hook_impl
    def after_context_created(self, context: KedroContext):
        self._session = self._init_session(context=context)
        self._audit_enabled = self._session.audit_store_manager.count(enabled_only=True) > 0

    @hook_impl
    def before_pipeline_run(self, run_params: dict[str, Any]):
        if self._audit_enabled:
            self._run_id = str(ULID())
            self._env = run_params.get("env") or os.getenv("KEDRO_ENV") or "local"
            self._extra_params = run_params.get("extra_params")
            self._run_params = deepcopy(run_params)

    @hook_impl
    def before_node_run(self, node: Node, inputs: dict[str, Any]):
        if self._audit_enabled:
            self._log_event(
                node=node.name,
                event=Event.STARTED,
                node_inputs=list(inputs.keys()) if inputs else None,
            )

    @hook_impl
    def after_node_run(
        self, node: Node, catalog: DataCatalog, inputs: dict[str, Any], outputs: dict[str, Any]
    ):
        if not self._audit_enabled:
            self._run_online_validations(catalog, outputs)
            return

        try:
            self._run_online_validations(catalog, outputs)
        except Exception as e:
            self._log_event(
                node=node.name,
                event=Event.FAILED,
                node_inputs=list(inputs.keys()) if inputs else None,
                exception=e,
            )
            raise e
        else:
            self._log_event(
                node=node.name,
                event=Event.COMPLETED,
                node_inputs=list(inputs.keys()) if inputs else None,
                node_outputs=list(outputs.keys()) if outputs else None,
            )

    @hook_impl
    def on_node_error(self, error: Exception, node: Node, inputs: dict[str, Any]):
        if self._audit_enabled:
            self._log_event(
                node=node.name,
                event=Event.FAILED,
                node_inputs=list(inputs.keys()) if inputs else None,
                exception=error,
            )

    def _init_session(self, context: KedroContext) -> DataSentinelSession:
        try:
            if "datasentinel" not in context.config_loader.config_patterns.keys():
                context.config_loader.config_patterns.update(
                    {"datasentinel": ["datasentinel*", "datasentinel*/**", "**/datasentinel*"]}
                )
            conf_datasentinel_yml = context.config_loader["datasentinel"]
        except MissingConfigException:
            self._logger.warning(
                "No datasentinel configuration file found, using an empty datasentinel "
                "configuration (without any notifiers, audit stores or result stores)."
            )
            conf_datasentinel_yml = {}

        conf_datasentinel_yml = {} if conf_datasentinel_yml is None else conf_datasentinel_yml
        try:
            session_config_model = DataSentinelSessionConfig(**conf_datasentinel_yml)
        except ValidationError as e:
            raise DataSentinelConfigError(
                "The datasentinel configuration file (datasentinel.yml) could not be parsed. "
                "Please verify that it has a valid structure: {e!s}"
            ) from e
        return session_config_model.create_session(context=context)

    def _run_online_validations(self, catalog: DataCatalog, node_outputs: dict[str, Any]):
        for dataset_name, data in node_outputs.items():
            dataset = catalog._get_dataset(dataset_name)
            if not dataset_has_validations(dataset):
                continue

            try:
                validation_conf_model = ValidationWorkflowConfig(
                    **dataset.metadata["kedro-datasentinel"]
                )
            except ValidationError as e:
                raise DataValidationConfigError(
                    f"The data validation configuration of the '{dataset_name}' dataset "
                    f"could not be parsed, please verify that it has a valid structure: {e!s}"
                ) from e

            if not validation_conf_model.has_online_checks:
                continue

            validation_workflow = validation_conf_model.create_validation_workflow(
                dataset_name=dataset_name,
                data=data,
                mode=Mode.ONLINE,
            )
            self._session.run_validation_workflow(validation_workflow)

    @staticmethod
    def _format_set_params(value: Any) -> list | None:
        if not value:
            return None

        return list(value) if not isinstance(value, list) else value

    def _log_event(
        self,
        node: str,
        event: Event,
        node_inputs: list[str] | None = None,
        node_outputs: list[str] | None = None,
        exception: Exception | None = None,
    ):
        """Logs an event.

        Args:
            node: The name of the node being executed.
            event: The event being logged.
            node_inputs: A list with the name of the input datasets of the node.
            node_outputs: A list with the name of the output datasets of the node.
            exception: If the node execution failed, the exception occurred.
        """
        self._session.audit_store_manager.append_to_all_stores(
            row=KedroAuditRow(
                run_id=self._run_id,
                pipeline_name=(
                    self._run_params.get("pipeline_name")
                    if self._run_params.get("pipeline_name")
                    else "__default__"
                ),
                node_name=node,
                inputs=node_inputs,
                outputs=node_outputs,
                session_id=self._run_params.get("session_id"),
                project_path=self._run_params.get("project_path"),
                env=self._env,
                kedro_version=self._run_params.get("kedro_version"),
                tags=self._format_set_params(self._run_params.get("tags")),
                from_nodes=self._format_set_params(self._run_params.get("from_nodes")),
                to_nodes=self._format_set_params(self._run_params.get("to_nodes")),
                node_names=self._format_set_params(self._run_params.get("node_names")),
                from_inputs=self._format_set_params(self._run_params.get("from_inputs")),
                to_outputs=self._format_set_params(self._run_params.get("to_outputs")),
                load_versions=self._format_set_params(self._run_params.get("load_versions")),
                extra_params=self._extra_params,
                namespace=self._run_params.get("namespace"),
                runner=self._run_params.get("runner"),
                exception=exception_to_str(exception) if exception else None,
                event=event.value,
                event_time=datetime.now(),
            )
        )


hooks = DataSentinelHooks()
