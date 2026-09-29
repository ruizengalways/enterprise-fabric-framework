"""Fabric SQL Database access used by control-plane adapters."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from enterprise_fabric_framework.connector.connectors import (
    ConnectionHealth,
    ConnectionHealthStatus,
    Connector,
)

SqlParameters = Mapping[str, object]
SqlRow = Mapping[str, object]


class FabricSqlDatabase(Connector):
    """Parameterized SQLAlchemy access to one Fabric SQL Database.

    The connection URL and its secret-bearing authentication options are resolved by the
    deployment. Framework metadata stores logical connection references, never these values.
    """

    def __init__(
        self,
        connection_url: str,
        *,
        connect_args: Mapping[str, object] | None = None,
        **engine_options: object,
    ) -> None:
        """Create a database adapter from a SQLAlchemy-compatible connection URL."""

        from sqlalchemy import create_engine

        self._engine = create_engine(
            connection_url,
            connect_args=dict(connect_args or {}),
            **engine_options,
        )

    @classmethod
    def from_engine(cls, engine: Any) -> FabricSqlDatabase:
        """Wrap an existing SQLAlchemy engine, primarily for deployment composition and tests."""

        database = cls.__new__(cls)
        database._engine = engine
        return database

    def new_session(self) -> _SqlAlchemyConnection:
        """Open a connection for a control-plane read operation."""

        return _SqlAlchemyConnection(self._engine.connect())

    def connect(self) -> _SqlAlchemyConnection:
        """Open a connection for a control-plane read operation."""

        return self.new_session()

    def begin(self) -> _SqlAlchemyConnection:
        """Open a transaction for a control-plane write operation."""

        return _SqlAlchemyConnection(self._engine.begin())

    def renew(self) -> None:
        """Discard pooled connections so later operations establish fresh database sessions."""

        self._engine.dispose()

    def health(self) -> ConnectionHealth:
        """Verify that the configured database accepts a minimal command."""

        try:
            with self.new_session() as connection:
                connection.execute("SELECT 1", {})
        except Exception as error:
            return ConnectionHealth(
                connector_type=type(self).__name__,
                status=ConnectionHealthStatus.UNHEALTHY,
                detail=type(error).__name__,
            )
        return ConnectionHealth(
            connector_type=type(self).__name__,
            status=ConnectionHealthStatus.HEALTHY,
        )

    def close(self) -> None:
        """Release the SQLAlchemy connection pool owned by this connector."""

        self.renew()


class _SqlAlchemyConnection:
    """Adapts SQLAlchemy's executable statements to the manager's parameterized SQL strings."""

    def __init__(self, context: Any) -> None:
        self._context = context
        self._connection: Any | None = None

    def __enter__(self) -> _SqlAlchemyConnection:
        self._connection = self._context.__enter__()
        return self

    def __exit__(self, *arguments: object) -> bool | None:
        return self._context.__exit__(*arguments)

    def execute(self, statement: str, parameters: SqlParameters) -> Any:
        """Execute a parameterized statement through the active SQLAlchemy connection."""

        from sqlalchemy import text

        if self._connection is None:
            raise RuntimeError("connection must be used as a context manager")
        return self._connection.execute(text(statement), dict(parameters))


__all__ = ["FabricSqlDatabase", "SqlParameters", "SqlRow"]