"""Run configuration: the Databricks-side replacement for PowerCenter's
parameter files, session properties and `$PM*` built-in variables.

Nothing here is hardcoded to an environment. On Databricks these values come from
job parameters / `dbutils.widgets`; in tests they are constructed directly, which
is what makes the ported logic deterministic.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class RunConfig:
    """Session-level context for a ported mapping.

    Attributes mirror PowerCenter concepts one-for-one:

    * `session_start_time` - SESSSTARTTIME. Constant for the whole session in
      PowerCenter, so it must be captured once and passed down rather than being
      re-evaluated per row with `current_timestamp()`.
    * `mapping_name` - $PMMappingName, written into COUNTER_TBL.PROCESS_NAME.
    * `repository_service_name` - $PMRepositoryServiceName, which the message
      expressions slice to derive the environment prefix ('Dev_'/'Test'/'Prod').
    * `catalog` / `schema` - replace the `$Source` / `$Target` relational
      connection objects.
    * `parameters` - the contents of the parameter file ($$PP_NUM and friends).
      Values are strings because a parameter file has no types; that is exactly
      why the mappings run IS_NUMBER over them.
    """

    catalog: str
    schema: str
    session_start_time: datetime
    mapping_name: str
    repository_service_name: str = ""
    parameters: dict[str, str] = field(default_factory=dict)
    target_file_dir: str = ""
    source_file: str = ""

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    def parameter(self, name: str) -> str | None:
        """Read a mapping parameter. An absent parameter and an empty one behave
        identically in PowerCenter: the expression sees an empty value."""
        value = self.parameters.get(name)
        return value if value else None


def add_common_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog (replaces the $Target connection)")
    parser.add_argument("--schema", required=True, help="Unity Catalog schema")
    parser.add_argument(
        "--session-start-time",
        default=None,
        help="ISO-8601 SESSSTARTTIME override; defaults to now. Set it for reproducible reruns.",
    )
    parser.add_argument(
        "--repository-service-name",
        default="",
        help="$PMRepositoryServiceName equivalent; its first 4 characters select the message prefix",
    )
    return parser


def session_start_time_from_arg(value: str | None) -> datetime:
    return datetime.now() if value is None else datetime.fromisoformat(value)
