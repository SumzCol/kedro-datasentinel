from collections.abc import Callable
from copy import deepcopy
from typing import Any

from datasentinel.notification.notifier.core import AbstractNotifier
from datasentinel.session import DataSentinelSession
from datasentinel.store.audit.core import AbstractAuditStore
from datasentinel.store.result.core import AbstractResultStore
from kedro.framework.context import KedroContext
from pydantic import BaseModel, Field, model_validator

from kedro_datasentinel.utils import try_load_obj_from_class_paths


class NotifierConfig(BaseModel):
    type: str
    disabled: bool | None = False

    class Config:
        # Allow passing extra fields
        extra = "allow"


class ResultStoreConfig(BaseModel):
    type: str
    disabled: bool | None = False

    class Config:
        # Allow passing extra fields
        extra = "allow"


class AuditStoreConfig(BaseModel):
    type: str
    disabled: bool | None = False

    class Config:
        # Allow passing extra fields
        extra = "allow"


class DataSentinelSessionConfig(BaseModel):
    session_name: str | None = None
    result_stores: dict[str, ResultStoreConfig] | None = Field(default_factory=dict)
    notifiers: dict[str, NotifierConfig] | None = Field(default_factory=dict)
    audit_stores: dict[str, AuditStoreConfig] | None = Field(default_factory=dict)

    class Config:
        # raise an error if an unknown key is passed to the constructor
        extra = "forbid"

    @model_validator(mode="before")
    def set_empty_stores_and_notifiers(cls, values):
        for field in ["result_stores", "notifiers", "audit_stores"]:
            values[field] = {} if values.get(field) is None else values.get(field)
        return values

    def create_session(self, context: KedroContext) -> DataSentinelSession:
        session = DataSentinelSession.get_or_create(self.session_name)
        credentials_loader = make_credentials_loader(context=context)

        for name, notifier_conf in self.notifiers.items():
            session.notifier_manager.register(
                notifier=_create_notifier(
                    name=name,
                    notifier_conf=notifier_conf,
                    credentials_loader=credentials_loader,
                )
            )

        for name, result_store_conf in self.result_stores.items():
            session.result_store_manager.register(
                result_store=_create_result_store(
                    name=name,
                    result_store_conf=result_store_conf,
                    credentials_loader=credentials_loader,
                )
            )

        for name, audit_store_conf in self.audit_stores.items():
            session.audit_store_manager.register(
                audit_store=_create_audit_store(
                    name=name,
                    audit_store_conf=audit_store_conf,
                    credentials_loader=credentials_loader,
                )
            )

        return session


def _create_arg_objs(args: dict) -> dict:
    for key, value in args.items():
        if isinstance(value, dict) and value.get("type") is not None:
            _obj_args = deepcopy(value)
            _type = _obj_args.pop("type")
            class_obj = try_load_obj_from_class_paths(
                class_paths=[
                    f"datasentinel.notification.notifier.{_type}",
                    f"datasentinel.store.audit.{_type}",
                    f"datasentinel.store.result.{_type}",
                    f"datasentinel.notification.renderer.{_type}",
                    _type,
                ]
            )
            if class_obj is not None:
                # Recursively process the object arguments before creating the object
                _obj_args = _create_arg_objs(_obj_args)
                args[key] = class_obj(**_obj_args)
    return args


def _create_notifier(
    name: str,
    notifier_conf: NotifierConfig,
    credentials_loader: Callable[[str], dict[str, Any] | None],
) -> AbstractNotifier:
    class_path = notifier_conf.type
    class_obj = try_load_obj_from_class_paths(
        class_paths=[class_path, f"datasentinel.notification.notifier.{class_path}"]
    )
    if class_obj is None:
        raise ValueError(
            f"The notifier class path '{class_path}' is not valid, it should be a full path like"
            "'datasentinel.notification.notifier.email.EmailNotifier' or one that reflects the "
            "module of the notifier like 'email.EmailNotifier'. For custom notifiers, a "
            "full class path is required"
        )

    notifier_obj_args = notifier_conf.model_dump(exclude={"type"})
    notifier_obj_args = _create_arg_objs(notifier_obj_args)
    notifier_obj_args["name"] = name

    credentials_key = notifier_obj_args.get("credentials")
    if credentials_key:
        credentials = credentials_loader(credentials_key)
        if credentials is not None:
            notifier_obj_args["credentials"] = credentials
        else:
            raise KeyError(
                f"Could not find credentials with key '{credentials_key}' while creating "
                f"notifier '{name}'"
            )

    return class_obj(**notifier_obj_args)


def _create_audit_store(
    name: str,
    audit_store_conf: AuditStoreConfig,
    credentials_loader: Callable[[str], dict[str, Any] | None],
) -> AbstractAuditStore:
    class_path = audit_store_conf.type
    class_obj = try_load_obj_from_class_paths(
        class_paths=[class_path, f"datasentinel.store.audit.{class_path}"]
    )
    if class_obj is None:
        raise ValueError(
            f"The audit store class path '{class_path}' is not valid, it should be a full path "
            "like 'datasentinel.store.audit.text.CSVAuditStore' or one that reflects "
            "the module of the audit store like 'text.CSVAuditStore'. "
            "For custom audit stores, a full class path is required"
        )

    audit_store_obj_args = audit_store_conf.model_dump(exclude={"type"})
    audit_store_obj_args = _create_arg_objs(audit_store_obj_args)
    audit_store_obj_args["name"] = name

    credentials_key = audit_store_obj_args.get("credentials")
    if credentials_key:
        credentials = credentials_loader(credentials_key)
        if credentials is not None:
            audit_store_obj_args["credentials"] = credentials
        else:
            raise KeyError(
                f"Could not find credentials with key '{credentials_key}' while creating "
                f"audit store '{name}'"
            )

    return class_obj(**audit_store_obj_args)


def _create_result_store(
    name: str,
    result_store_conf: ResultStoreConfig,
    credentials_loader: Callable[[str], dict[str, Any] | None],
) -> AbstractResultStore:
    class_path = result_store_conf.type
    class_obj = try_load_obj_from_class_paths(
        class_paths=[class_path, f"datasentinel.store.result.{class_path}"]
    )
    if class_obj is None:
        raise ValueError(
            f"The result store class path '{class_path}' is not valid, it should be a full path "
            "like 'datasentinel.store.result.text.CSVResultStore' or one that reflects "
            "the module of the result store like 'text.CSVResultStore'. "
            "For custom result stores, a full class path is required"
        )

    result_store_obj_args = result_store_conf.model_dump(exclude={"type"})
    result_store_obj_args = _create_arg_objs(result_store_obj_args)
    result_store_obj_args["name"] = name

    credentials_key = result_store_obj_args.get("credentials")
    if credentials_key:
        credentials = credentials_loader(credentials_key)
        if credentials is not None:
            result_store_obj_args["credentials"] = credentials
        else:
            raise KeyError(
                f"Could not find credentials with key '{credentials_key}' while creating "
                f"result store '{name}'"
            )

    return class_obj(**result_store_obj_args)


def make_credentials_loader(context: KedroContext) -> Callable[[str], dict[str, Any] | None]:
    credentials = None

    def read_credentials(key: str) -> dict[str, Any] | None:
        nonlocal credentials
        if credentials is None:
            credentials = context._get_config_credentials()

        return credentials.get(key)

    return read_credentials
