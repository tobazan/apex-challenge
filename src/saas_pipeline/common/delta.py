from delta.tables import DeltaTable
from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession


def ensure_uc_schema(spark: SparkSession, cfg: DictConfig, target: str) -> None:
    """Ensures the catalog.schema namespace exists when running on Databricks Unity Catalog."""
    if cfg.runtime == "databricks" and "." in target:
        parts = target.split(".")
        if len(parts) == 3:
            catalog_schema = f"{parts[0]}.{parts[1]}"
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_schema}")


def delta_exists(spark: SparkSession, cfg: DictConfig, target: str) -> bool:
    """Checks if a Delta table exists in either filesystem or Unity Catalog."""
    if cfg.runtime == "databricks":
        return spark.catalog.tableExists(target)
    return DeltaTable.isDeltaTable(spark, target)


def get_delta_table(spark: SparkSession, cfg: DictConfig, target: str) -> DeltaTable:
    """Returns a DeltaTable instance for MERGE operations across either runtime."""
    if cfg.runtime == "databricks":
        return DeltaTable.forName(spark, target)
    return DeltaTable.forPath(spark, target)


def read_delta(spark: SparkSession, cfg: DictConfig, target: str) -> DataFrame:
    """Reads a Delta table by path (filesystem) or table name (databricks)."""
    if cfg.runtime == "databricks":
        return spark.read.table(target)
    return spark.read.format("delta").load(target)


def write_delta(
    df: DataFrame,
    cfg: DictConfig,
    target: str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
    merge_schema: bool = False,
) -> None:
    """Writes a DataFrame to Delta path (filesystem) or registered UC table (databricks)."""
    writer = df.write.format("delta").mode(mode)
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    if cfg.runtime == "databricks":
        # Ensure schema exists in Unity Catalog before saving table
        catalog_schema = ".".join(target.split(".")[:2])
        df.sparkSession.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_schema}")
        writer.saveAsTable(target)
    else:
        writer.save(target)


def write_stream_delta(
    stream_df: DataFrame,
    cfg: DictConfig,
    target: str,
    checkpoint_path: str,
    output_mode: str = "append",
    partition_by: list[str] | None = None,
    merge_schema: bool = True,
) -> None:
    """Executes an availableNow Structured Streaming write to a Delta path or UC table."""
    writer = stream_df.writeStream.format("delta").outputMode(output_mode).option("checkpointLocation", checkpoint_path)
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer = writer.trigger(availableNow=True)

    if cfg.runtime == "databricks":
        ensure_uc_schema(stream_df.sparkSession, cfg, target)
        query = writer.toTable(target)
    else:
        query = writer.start(target)

    query.awaitTermination()
