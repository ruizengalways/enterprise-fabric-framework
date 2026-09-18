"""Optional Spark-native FULL, watermark, CDC and Delta CDF source capture.

Registered ``sources`` read through supported streaming protocols or bounded extraction into Spark.
Independently registered ``writers`` append retained Bronze and publish delivery evidence. Use this
boundary only when Fabric Copy or Dataflow Gen2 cannot satisfy the source contract.
"""

__all__: tuple[str, ...] = ()
