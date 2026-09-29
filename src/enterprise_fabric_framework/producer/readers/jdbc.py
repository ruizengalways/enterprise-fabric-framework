"""JDBC connector implementations."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from enterprise_fabric_framework.connector.connectors import (
    ConnectionHealth,
    ConnectionHealthStatus,
    Connector,
)

DataFrame = Any
SparkSession = Any


class JdbcConnector(Connector):
    """Reads JDBC tables using credentials and options resolved at runtime.

    Args:
        spark: Active Spark session with the JDBC data source available.
        url: JDBC connection URL, such as ``jdbc:sqlserver://database.example.com:1433``.
        username: JDBC username resolved at runtime. Do not persist it in framework metadata.
        password: JDBC password resolved at runtime. Do not persist it in framework metadata.
        connection_options: Non-secret JDBC options, such as ``driver``.

    Example:
        >>> connector = JdbcConnector(
        ...     spark,
        ...     "jdbc:sqlserver://database.example.com:1433;databaseName=crm",
        ...     username="${JDBC_USERNAME}",
        ...     password="${JDBC_PASSWORD}",
        ...     connection_options={"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"},
        ... )
        >>> dataframe = connector.read("dbo.customer")
    """

    def __init__(
        self,
        spark: SparkSession,
        url: str,
        username: str,
        password: str,
        connection_options: Mapping[str, str],
        *,
        health_check_resource: str | None = None,
    ) -> None:
        """Create a connector for one JDBC data source.

        Args:
            spark: Active Spark session used to load JDBC tables.
            url: JDBC connection URL.
            username: JDBC username resolved at runtime.
            password: JDBC password resolved at runtime.
            connection_options: Non-secret JDBC options forwarded to Spark's JDBC data source.
            health_check_resource: Optional table used by ``health`` to prove JDBC access.
        """

        self._spark = spark
        self._url = url
        self._connection_options = dict(connection_options)
        self._connection_options["user"] = username
        self._connection_options["password"] = password
        self._health_check_resource = health_check_resource.strip() if health_check_resource else None
        if health_check_resource is not None and self._health_check_resource is None:
            raise ValueError("health_check_resource must not be empty when supplied")

    def read(self, table_name: str) -> DataFrame:
        """Read all rows from one JDBC table.

        Args:
            table_name: Table name, such as ``dbo.customer``.

        Returns:
            A Spark DataFrame containing the complete JDBC table. Spark may push compatible
            filters down when ``read`` applies the connector-neutral predicates.
        """

        return (
            self._spark.read.format("jdbc")
            .option("url", self._url)
            .option("dbtable", table_name)
            .options(**self._connection_options)
            .load()
        )

    def health(self) -> ConnectionHealth:
        """Read at most one row from the configured table to prove JDBC access."""

        if self._health_check_resource is None:
            return ConnectionHealth(
                connector_type=type(self).__name__,
                status=ConnectionHealthStatus.NOT_CONFIGURED,
                detail="health_check_resource is not configured",
            )
        try:
            self.read(self._health_check_resource).limit(1).count()
        except Exception as error:
            return ConnectionHealth(
                connector_type=type(self).__name__,
                status=ConnectionHealthStatus.UNHEALTHY,
                resource_ref=self._health_check_resource,
                detail=type(error).__name__,
            )
        return ConnectionHealth(
            connector_type=type(self).__name__,
            status=ConnectionHealthStatus.HEALTHY,
            resource_ref=self._health_check_resource,
        )


__all__ = ["JdbcConnector"]
