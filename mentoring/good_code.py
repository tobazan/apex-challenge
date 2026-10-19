import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

# esto sigue siendo hardcodeado, preferentemente se cargarian de un config file
VALID_DELIVERY_TYPES = ["ZPRE", "ZVE1"]
CS_TO_ST_FACTOR = 20
DEFAULT_OUTPUT_BASE_PATH = "/tmp/output"

# declaramos el schema antes de leer
# casteamos luego para los calculos
DELIVERIES_SCHEMA = StructType(
    [
        StructField("pais", StringType(), nullable=True),
        StructField("fecha_proceso", StringType(), nullable=True),
        StructField("transporte", StringType(), nullable=True),
        StructField("ruta", StringType(), nullable=True),
        StructField("tipo_entrega", StringType(), nullable=True),
        StructField("material", StringType(), nullable=True),
        StructField("precio", StringType(), nullable=True),
        StructField("cantidad", StringType(), nullable=True),
        StructField("unidad", StringType(), nullable=True),
    ]
)

OUTPUT_COLUMNS = ["pais", "fecha", "material", "cantidad_st", "total"]


def read_deliveries(spark: SparkSession, file_path: str) -> DataFrame:
    """Read the raw CSV in parallel using the predefined schema"""
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("enforceSchema", "false")
        .schema(DELIVERIES_SCHEMA)
        .load(file_path)
    )


def transform_deliveries(df: DataFrame, country: str) -> DataFrame:
    """Clean, filter, normalise quantities to ST and compute totals.
    Returns the candidate rows plus an `is_valid` flag; nothing is dropped here.
    """
    country_code = country.strip().upper()

    cleaned = df.select(
        # Normalise text so "gt ", "GT" and "Gt" are treated the same
        F.upper(F.trim("pais")).alias("pais"),
        F.trim("material").alias("material"),
        F.upper(F.trim("tipo_entrega")).alias("tipo_entrega"),
        F.upper(F.trim("unidad")).alias("unidad"),
        # Only parse dates that look like yyyyMMdd; anything else becomes NULL
        F.when(
            F.trim("fecha_proceso").rlike(r"^\d{8}$"),
            F.to_date(F.trim("fecha_proceso"), "yyyyMMdd"),
        ).alias("fecha"),
        F.trim("cantidad").cast(DecimalType(12, 2)).alias("cantidad"),
        F.trim("precio").cast(DecimalType(12, 2)).alias("precio"),
    )

    enriched = (cleaned.filter(
                    (F.col("pais") == country_code) & F.col("tipo_entrega").isin(VALID_DELIVERY_TYPES)
                )
                .withColumn(
                    "cantidad_st",
                    F.when(F.col("unidad") == "CS", F.col("cantidad") * F.lit(CS_TO_ST_FACTOR))
                    .otherwise(F.col("cantidad"))
                    .cast(DecimalType(12, 2)),
                )
                .withColumn("total", (F.col("cantidad_st") * F.col("precio")).cast(DecimalType(18, 2)))
                .withColumn(
                    "is_valid", # no descarta sino que flagea en caso que queramos escribir a quarentena
                    F.col("fecha").isNotNull() & F.col("cantidad").isNotNull() & F.col("precio").isNotNull(),
                )
    )
    return enriched


def write_valid_deliveries(output_base_path: str, country: str, df: DataFrame):
    """Filters valid rows and writes the parquet output"""
    output_path = f"{output_base_path.rstrip('/')}/{country.strip().upper()}"

    filtered_df = df.filter("is_valid").select(*OUTPUT_COLUMNS)
    filtered_df.write.mode("overwrite").parquet(output_path)
    
    return


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", required=True, help="Path to the deliveries CSV")
    parser.add_argument("--country", required=True, help="Country code, e.g. GT")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_BASE_PATH, help="Base output path")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    raw_df = read_deliveries(spark, args.file)
    enriched_df = transform_deliveries(raw_df, args.country)
    write_valid_deliveries(args.output, args.country, enriched_df)


if __name__ == "__main__":
    main()