from collections.abc import Sequence

from pyspark.sql import Column
from pyspark.sql import functions as F

from saas_pipeline.common.quality_log import QualityRule


def build_delivery_quarantine_reason() -> Column:
    """Returns the structural validation expression for raw delivery records."""
    reasons = F.array(
        F.when(F.col("_tenant_id").isNull() | (F.trim(F.col("_tenant_id")) == ""), F.lit("INVALID_TENANT")),
        F.when(F.col("_parsed_date").isNull(), F.lit("INVALID_FECHA_PROCESO")),
        F.when(
            F.col("_parsed_qty_decimal").isNull()
            | (F.col("_parsed_qty_decimal") <= 0)
            | (F.col("_parsed_qty_decimal") != F.floor(F.col("_parsed_qty_decimal"))),
            F.lit("INVALID_CANTIDAD"),
        ),
        F.when(
            F.col("_parsed_price").isNull() | (F.col("_parsed_price") <= 0),
            F.lit("INVALID_PRECIO"),
        ),
        F.when(F.col("transporte").cast("long").isNull(), F.lit("INVALID_TRANSPORTE")),
        F.when(F.col("ruta").cast("long").isNull(), F.lit("INVALID_RUTA")),
        F.when(
            F.col("unidad").isNull() | ~F.upper(F.trim(F.col("unidad"))).isin(["CS", "ST"]),
            F.lit("INVALID_UNIDAD"),
        ),
        F.when(
            F.col("_cat_sku").isNull() | (F.col("_parsed_date").isNotNull() & F.col("_dim_material").isNull()),
            F.lit("UNMAPPED_MATERIAL"),
        ),
    )
    return F.when(F.size(F.array_compact(reasons)) > 0, F.concat_ws("|", F.array_compact(reasons))).otherwise(
        F.lit(None)
    )


def build_silver_delivery_quality_rules(
    routine_delivery_types: Sequence[str],
    bonus_delivery_types: Sequence[str],
) -> list[QualityRule]:
    """Builds critical post-merge rules for the Silver delivery fact."""
    valid_delivery_types = list(routine_delivery_types) + list(bonus_delivery_types)
    return [
        QualityRule(
            name="silver_valid_fecha_proceso_not_null",
            severity="critical",
            valid_condition=F.col("fecha_proceso").isNotNull(),
        ),
        QualityRule(
            name="silver_required_delivery_fields_not_null",
            severity="critical",
            valid_condition=(
                F.col("tenant_id").isNotNull()
                & F.col("transporte").isNotNull()
                & F.col("ruta").isNotNull()
                & F.col("tipo_entrega").isNotNull()
                & F.col("material").isNotNull()
                & F.col("precio").isNotNull()
                & F.col("cantidad_original").isNotNull()
                & F.col("cantidad_normalizada_st").isNotNull()
                & F.col("unidad_original").isNotNull()
                & F.col("unidad_normalizada").isNotNull()
            ),
        ),
        QualityRule(
            name="silver_positive_normalized_quantity_st",
            severity="critical",
            valid_condition=F.col("cantidad_normalizada_st").isNotNull()
            & (F.col("cantidad_normalizada_st") > 0)
            & (F.col("unidad_normalizada") == "ST"),
        ),
        QualityRule(
            name="silver_scd2_catalog_referential_integrity",
            severity="critical",
            valid_condition=F.col("material_descripcion").isNotNull() & F.col("material_precio_base").isNotNull(),
        ),
        QualityRule(
            name="silver_valid_delivery_type_flags",
            severity="critical",
            valid_condition=F.col("tipo_entrega").isin(valid_delivery_types)
            & (F.col("is_routine_delivery") != F.col("is_bonus_delivery")),
        ),
    ]
