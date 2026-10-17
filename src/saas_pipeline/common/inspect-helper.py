import sys

from pyspark.sql import functions as F

from saas_pipeline.common.spark import get_spark_session

spark = get_spark_session("Medallion_Inspector")


def inspect(tenant="all"):
    print("\n" + "=" * 80)
    print(f"--- MEDALLION INSPECTION (tenant: {tenant}) ---")
    print("=" * 80)

    tenants = ["sv", "hn", "ec", "jm", "gt", "pe"] if tenant == "all" else [tenant.lower()]

    # SCD Type 2 Dim Inspection
    print("\n[SILVER: dim_materials]")
    sample_tenant = tenants[0]
    dim_path = "data/silver/shared/dim_materials"
    try:
        dim_df = spark.read.format("delta").load(dim_path)
        total_dim = dim_df.count()
        distinct_skus = dim_df.select("material").distinct().count()
        current_skus = dim_df.filter(F.col("is_current")).count()
        print(f"  Path: {dim_path}")
        print(f"  Total records: {total_dim} | Distinct SKUs: {distinct_skus} | is_current=True: {current_skus}")

        # Show multiple versions of SKUs that have price history
        skus_with_history = (
            dim_df.groupBy("material")
            .agg(F.count("*").alias("versions"))
            .filter(F.col("versions") > 1)
            .limit(2)
            .collect()
        )
        if skus_with_history:
            example_sku = skus_with_history[0]["material"]
            print(f"\n  Sample SKU with SCD2 history ({example_sku}):")
            dim_df.filter(F.col("material") == example_sku).select(
                "material", "descripcion", "precio_base", "valid_from", "valid_to", "is_current"
            ).orderBy("valid_from").show(truncate=False)
    except Exception as e:
        print(f"  Not found or error: {e}")

    # Fact Deliveries & Quarantine per Tenant
    print("\n[SILVER: fact_deliveries vs QUARANTINE]")
    total_silver_all = 0
    total_quarantine_all = 0

    for t in tenants:
        s_path = f"data/silver/{t}/fact_deliveries"
        q_path = f"data/silver_quarantine/{t}/fact_deliveries"

        s_count = 0
        q_count = 0
        try:
            s_df = spark.read.format("delta").load(s_path)
            s_count = s_df.count()
            total_silver_all += s_count
        except Exception:
            pass

        try:
            q_df = spark.read.format("delta").load(q_path)
            q_count = q_df.count()
            total_quarantine_all += q_count
        except Exception:
            pass

        print(f"  Tenant {t.upper():<4} | Silver Valid: {s_count:<5} | Quarantined: {q_count:<4}")

    print("-" * 60)
    print(f"  TOTALS:    Silver Valid: {total_silver_all:<5} | Quarantined: {total_quarantine_all:<4}")

    # Quarantine Reasons Breakdown
    print("\n[SILVER QUARANTINE: Failure Breakdown]")
    try:
        all_q_dfs = []
        for t in tenants:
            p = f"data/silver_quarantine/{t}/fact_deliveries"
            try:
                all_q_dfs.append(spark.read.format("delta").load(p))
            except Exception:
                pass
        if all_q_dfs:
            combined_q = all_q_dfs[0]
            for extra in all_q_dfs[1:]:
                combined_q = combined_q.unionByName(extra)

            combined_q.groupBy("_quarantine_reason").agg(F.count("*").alias("count")).orderBy(
                F.col("count").desc()
            ).show(truncate=False)
    except Exception as e:
        print(f"  No quarantine data: {e}")

    # Enriched Fact Sample
    print("\n[SILVER FACT SAMPLE (Enriched)]")
    try:
        sample_fact = spark.read.format("delta").load(f"data/silver/{sample_tenant}/fact_deliveries")
        sample_fact.select(
            "tenant_id",
            "fecha_proceso",
            "material",
            "material_descripcion",
            "material_precio_base",
            "cantidad_original",
            "unidad_original",
            "cantidad_normalizada_st",
            "unidad_normalizada",
            "is_routine_delivery",
            "is_bonus_delivery",
        ).show(3, truncate=False)
    except Exception as e:
        print(f"  Could not load sample fact: {e}")

    # Shared Quality Logs Summary
    print("\n[SHARED: quality_logs]")
    try:
        qlogs_df = spark.read.format("delta").load("data/shared/quality_logs")
        print(f"  Total logs registered: {qlogs_df.count()}")
        print("\n  Summary by layer, table, and check:")
        qlogs_df.groupBy("layer", "table_name", "check_name", "check_severity").agg(
            F.count("*").alias("total_runs"),
            F.sum("records_checked").alias("checked_records"),
            F.sum("records_failed").alias("failed_records"),
        ).orderBy("layer", "table_name", "check_name").show(truncate=False)
    except Exception as e:
        print(f"  No quality logs found: {e}")


if __name__ == "__main__":
    t = sys.argv[1] if len(sys.argv) > 1 else "all"
    inspect(t)
