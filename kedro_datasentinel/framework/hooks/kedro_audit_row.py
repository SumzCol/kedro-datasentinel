from datetime import datetime
from typing import Any

from datasentinel.store.audit.row import BaseAuditRow


class KedroAuditRow(BaseAuditRow):
    run_id: str
    pipeline_name: str
    node_name: str
    inputs: list[str] | None = None
    outputs: list[str] | None = None
    session_id: str | None = None
    project_path: str | None = None
    env: str | None = None
    kedro_version: str | None = None
    tags: list[str] | None = None
    from_nodes: list[str] | None = None
    to_nodes: list[str] | None = None
    node_names: list[str] | None = None
    from_inputs: list[str] | None = None
    to_outputs: list[str] | None = None
    load_versions: list[str] | None = None
    extra_params: dict[str, Any] | None = None
    namespace: str | None = None
    runner: str | None = None
    exception: str | None = None
    event: str
    event_time: datetime
