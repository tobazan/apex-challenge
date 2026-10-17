# inspect_bronze.py
import sys

from pyspark.sql import functions as F

from saas_pipeline.spark import get_spark_session

spark = get_spark_session("Bronze_Inspector")


def inspect(tenant="all"):
    print("\n" + "=" * 60)
    print(f"--- DELTA INSPECTION (tenant: {tenant}) ---")
    print("=" * 60)

    # 1. Inspect Materials Catalog
    try:
        mat_df = spark.read.format("delta").load("data/bronze/shared/materials_catalog")
        print(f"\n[materials_catalog] Total rows: {mat_df.count()}")
        print("Top 5 latest ingested materials:")
        mat_df.select(
            "material",
            "descripcion",
            "precio_base",
            "valid_from",
            "valid_to",
            "_ingestion_timestamp",
            "_batch_id",
        ).orderBy(F.col("_ingestion_timestamp").desc()).show(5, truncate=False)
    except Exception as e:
        print(f"\n[materials_catalog] Not found or empty ({e})")

    # 2. Inspect Deliveries
    tenants = ["sv", "hn", "ec", "jm", "gt", "pe"] if tenant == "all" else [tenant.lower()]
    for t in tenants:
        path = f"data/bronze/{t}/deliveries"
        try:
            df = spark.read.format("delta").load(path)
            total = df.count()
            print(f"\n[deliveries - {t.upper()}] Path: {path} | Total rows: {total}")

            # Show partitions ordered descending by date
            print(f"Partitions for {t.upper()} (latest fecha_proceso first):")
            df.groupBy("fecha_proceso").agg(F.count("*").alias("row_count")).orderBy(
                F.col("fecha_proceso").desc()
            ).show(truncate=False)

            # Show latest ingested sample rows
            print(f"Top 3 latest ingested records for {t.upper()}:")
            df.select(
                "tenant_id",
                "fecha_proceso",
                "material",
                "tipo_entrega",
                "precio",
                "cantidad",
                "unidad",
                "_ingestion_timestamp",
                "_batch_id",
            ).orderBy(F.col("_ingestion_timestamp").desc()).show(3, truncate=False)

        except Exception:
            print(f"\n[deliveries - {t.upper()}] Path {path} does not exist yet.")

    # 3. Inspect Quality Logs
    try:
        qdf = spark.read.format("delta").load("data/shared/quality_logs")
        print(f"\n[quality_logs] Total checks logged: {qdf.count()}")
        print("Top 10 latest executed checks:")
        qdf.select(
            "_run_id",
            "tenant_id",
            "table_name",
            "check_name",
            "records_checked",
            "records_failed",
            "check_passed",
            "executed_at",
        ).orderBy(F.col("executed_at").desc()).show(10, truncate=False)
    except Exception as e:
        print(f"\n[quality_logs] No logs found ({e})")


if __name__ == "__main__":
    t = sys.argv[1] if len(sys.argv) > 1 else "all"
    inspect(t)
