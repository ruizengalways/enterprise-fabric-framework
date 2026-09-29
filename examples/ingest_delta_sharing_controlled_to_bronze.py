"""Run one control-plane-targeted Delta Sharing Bronze delivery.

Set ``CONTROL_PLANE_SQLALCHEMY_URL``, ``DELTA_SHARING_PROFILE``, ``BRONZE_RUN_ID``,
``BRONZE_DELIVERY_ID``, ``BRONZE_COMPLETION_STATE``, and ``BRONZE_PUBLISHED_ROWS`` before
running a targeted single-table delivery in a Spark environment. The Fabric SQL procedure supplies
the source object, capture mode, and Bronze target. Set ``SOURCE_VERSION`` for an optional
FULL-read source version, or ``LOWER_BOUND`` and ``UPPER_BOUND`` for a WATERMARK run.
"""

from __future__ import annotations

import os

from enterprise_fabric_framework.connector import DeltaSharingConnector, FabricSqlDatabase
from enterprise_fabric_framework.control_plane import BronzeControlPlaneManager
from enterprise_fabric_framework.producer.delta_sharing_job import (
    DeltaSharingBronzeDeliverySettings,
    ingest_delta_sharing_delivery,
)
from enterprise_fabric_framework.producer.readers import DeltaSharingReader
from enterprise_fabric_framework.quality import SparkSqlDataQualityEvaluator
from enterprise_fabric_framework.reconciliation import RowCountReconciliationEvaluator


def main() -> None:
    """Compose the Fabric SQL control plane with the Delta Sharing reader."""

    from pyspark.sql import SparkSession

    environment = os.environ
    settings = DeltaSharingBronzeDeliverySettings.from_environment(environment)
    spark = SparkSession.builder.getOrCreate()
    manager = BronzeControlPlaneManager(FabricSqlDatabase(settings.control_plane_connection_url))
    reader = DeltaSharingReader(DeltaSharingConnector(spark, credentials=settings.delta_sharing_profile))
    evidence = ingest_delta_sharing_delivery(
        manager,
        reader,
        settings,
        environment,
        quality_evaluator=SparkSqlDataQualityEvaluator(()),
        reconciliation_evaluator=RowCountReconciliationEvaluator(),
    )
    print(
        "Bronze publication recorded "
        f"for {evidence.source_object} using {evidence.capture_mode.value} capture."
    )
if __name__ == "__main__":
    main()