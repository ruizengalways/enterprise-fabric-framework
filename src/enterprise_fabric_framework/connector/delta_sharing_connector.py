"""Delta Sharing source connector."""

from __future__ import annotations

from typing import Any

from enterprise_fabric_framework.connector.connectors import (
    ConnectionHealth,
    ConnectionHealthStatus,
    Connector,
)

DataFrame = Any
SparkSession = Any


class DeltaSharingConnector(Connector):
    """Reads Delta Sharing tables using a runtime-resolved profile file.

    Args:
        spark: Active Spark session with the Delta Sharing data source available.
        credentials: Runtime-resolved Delta Sharing profile path or credentials macro. It
            contains connection credentials and must not be stored in framework metadata.

    Example:
        >>> connector = DeltaSharingConnector(spark, credentials="${CREDENTIALS_MACRO}")
        >>> dataframe = connector.read("crm.customer")
        >>> dataframe.columns
        ['customer_id', 'modified_at']
    """

    def __init__(
        self,
        spark: SparkSession,
        credentials: str,
        *,
        health_check_resource: str | None = None,
    ) -> None:
        """Create a connector for one Delta Sharing profile.

        Args:
            spark: Active Spark session used to load shared Delta tables.
            credentials: Runtime-resolved Delta Sharing profile path or credentials macro.
            health_check_resource: Optional share table used by ``health`` to prove provider access.
        """

        if not credentials.strip():
            raise ValueError("credentials must not be empty")

        self._spark = spark
        self._credentials = credentials
        self._health_check_resource = _optional_resource(health_check_resource)

    def read(self, table_name: str) -> DataFrame:
        """Read all rows from one Delta Sharing table.

        Args:
            table_name: Shared object name, such as ``crm.customer``.

        Returns:
            A Spark DataFrame containing the complete shared object.
        """

        if not table_name.strip():
            raise ValueError("table_name must not be empty")

        return self._spark.read.format("deltaSharing").load(f"{self._credentials}#{table_name}")

    def health(self) -> ConnectionHealth:
        """Read at most one row from the configured resource to prove provider access."""

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


def _optional_resource(resource_ref: str | None) -> str | None:
    if resource_ref is None:
        return None
    if not resource_ref.strip():
        raise ValueError("health_check_resource must not be empty when supplied")
    return resource_ref


__all__ = ["DeltaSharingConnector"]
