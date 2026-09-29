"""Shared extension point for Fabric SQL control-plane managers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any, Generic, TypeVar

from enterprise_fabric_framework.connector.connectors import Connector

Plan = TypeVar("Plan")


class ControlPlaneManager(ABC, Generic[Plan]):
    """Base class for a customizable manager over one frozen control-plane run type."""

    def __init__(self, connector: Connector) -> None:
        """Create a manager over a deployment-specific database connector."""

        self._connector = connector

    @abstractmethod
    def load_plan(self, run_id: str) -> Plan:
        """Load the frozen plan for one run owned by this manager."""

        raise NotImplementedError

    def _fetch_one(
        self,
        statement: str,
        parameters: Mapping[str, object],
    ) -> Mapping[str, object] | None:
        """Run a parameterized control-plane read through the configured connector."""

        with self._open_connection("connect") as connection:
            row = connection.execute(statement, dict(parameters)).mappings().one_or_none()
        return None if row is None else dict(row)

    def _fetch_all(
        self,
        statement: str,
        parameters: Mapping[str, object],
    ) -> tuple[Mapping[str, object], ...]:
        """Run a parameterized control-plane read that returns zero or more rows."""

        with self._open_connection("connect") as connection:
            rows = connection.execute(statement, dict(parameters)).mappings().all()
        return tuple(dict(row) for row in rows)

    def _execute(self, statement: str, parameters: Mapping[str, object]) -> int:
        """Run one transactional parameterized control-plane command."""

        with self._open_connection("begin") as connection:
            return connection.execute(statement, dict(parameters)).rowcount

    def _open_connection(self, operation: str) -> Any:
        opener = getattr(self._connector, operation, None)
        if not callable(opener):
            raise TypeError(
                f"{type(self._connector).__name__} does not support control-plane {operation}"
            )
        return opener()


def _required_text(row: Mapping[str, object], field_name: str) -> str:
    value = _optional_text(row, field_name)
    if value is None:
        raise ValueError(f"control-plane plan requires {field_name}")
    return value


def _optional_text(row: Mapping[str, object], field_name: str) -> str | None:
    value = row.get(field_name)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


__all__ = ["ControlPlaneManager"]