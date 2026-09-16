"""Optional Spark-native FULL, watermark, CDC and Delta CDF source capture.

Registered ``sources`` read a frozen source boundary into Spark. Independently registered
``writers`` persist the declared Bronze representation and completed capture manifest. Use this
boundary only when Fabric Copy or Dataflow Gen2 cannot satisfy the source contract.
"""

__all__: tuple[str, ...] = ()
