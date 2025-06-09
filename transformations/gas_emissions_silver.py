import dlt
from pyspark.sql import functions as F # Ensure consistent alias
from pyspark.sql.window import Window

# Define the Bronze table for raw ingestion
@dlt.table(
    name="bronze_sensor_emissions",
    comment="Ingest raw gas emissions data from CSV files to bronze layer.",
    table_properties={"quality": "bronze"}
)
def bronze_sensor_emissions():
  return (
    spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true") # Consider inferring schema for bronze, or specify manually
      .load("/Volumes/harrison_chen_catalog/synthetic_energy/energy_volume/Gas_Emissions/sensor_emissions/*.csv")
  )

@dlt.table(
    name="bronze_asset_info",
    comment="Ingest raw asset data from CSV files to bronze layer.",
    table_properties={"quality": "bronze"}
)
def bronze_asset_info():
  return (
    spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true") # Or define schema explicitly if inference is an issue
      .load("/Volumes/harrison_chen_catalog/synthetic_energy/energy_volume/Gas_Emissions/asset/*.csv")
  )

@dlt.table(
    name="bronze_site_info",
    comment="Ingest raw site data from CSV files to bronze layer.",
    table_properties={"quality": "bronze"}
)

def bronze_site_info():
  return (
    spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true") # Or define schema explicitly if inference is an issue
      .load("/Volumes/harrison_chen_catalog/synthetic_energy/energy_volume/Gas_Emissions/site_info/*.csv")
  )

# Read and clean sensor emissions
@dlt.table(
    name="silver_sensor_emissions",
    comment="Transform gas emissions data from bronze to silver layer",
    table_properties={
        "quality": "silver",
        "pipelines.reset.allowed": "false"
    },
    # Note: 'path' argument is typically not used for DLT tables in Unity Catalog.
    # The table will be created in the 'target schema' defined in your DLT pipeline settings.
    # If you intend to save this as an external table to a specific path, you would set:
    # spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    # and then the path would be managed by DLT for the table itself.
    # For a demo, you often just rely on the target schema.
    # For now, I'll remove it to avoid potential path conflicts or managed/external table issues.
    # path="/Volumes/harrison_chen_catalog/synthetic_energy/energy_volume/Gas_Emissions_Silver/sensor_emissions"
)
def silver_sensor_emissions():
    # --- IMPORTANT FIX HERE ---
    # Read the bronze data using dlt.read() to ensure DLT manages dependencies
    bronze_df = dlt.read("bronze_sensor_emissions")
    # --- END FIX ---
    
    # Cast columns to appropriate types
    df = bronze_df.select(
        F.col("emission_id").cast("integer"),
        F.col("asset_id").cast("integer"),
        F.col("site_id").cast("integer"),
        F.col("timestamp").cast("timestamp"),
        F.col("methane_level").cast("double"),
        F.col("co2_level").cast("double"),
        F.col("nox_level").cast("double"),
        F.col("temperature").cast("double"),
        F.col("pressure").cast("double"),
        F.col("flow_rate").cast("double"),
        F.col("alert_status")
    )
    
    # Handle nulls:
    # 1. For numeric readings, use moving average of last 3 readings for the same asset
    window_spec = Window.partitionBy("asset_id").orderBy("timestamp").rowsBetween(-3, -1)
    
    df = df.withColumn(
        "methane_level",
        F.when(F.col("methane_level").isNull(),
               F.avg("methane_level").over(window_spec)
        ).otherwise(F.col("methane_level"))
    ).withColumn(
        "co2_level",
        F.when(F.col("co2_level").isNull(),
               F.avg("co2_level").over(window_spec)
        ).otherwise(F.col("co2_level"))
    ).withColumn(
        "nox_level",
        F.when(F.col("nox_level").isNull(),
               F.avg("nox_level").over(window_spec)
        ).otherwise(F.col("nox_level"))
    ).withColumn(
        "temperature",
        F.when(F.col("temperature").isNull(),
               F.avg("temperature").over(window_spec)
        ).otherwise(F.col("temperature"))
    ).withColumn(
        "pressure",
        F.when(F.col("pressure").isNull(),
               F.avg("pressure").over(window_spec)
        ).otherwise(F.col("pressure"))
    ).withColumn(
        "flow_rate",
        F.when(F.col("flow_rate").isNull(),
               F.avg("flow_rate").over(window_spec)
        ).otherwise(F.col("flow_rate"))
    )
    
    # 2. For alert_status, use "UNKNOWN" for nulls
    df = df.withColumn(
        "alert_status",
        F.when(F.col("alert_status").isNull(), "UNKNOWN")
         .otherwise(F.col("alert_status"))
    )
    
    # Add data quality expectations - these should be applied AFTER transformations
    # that might affect the validity of the conditions.
    # For example, if your moving average could result in a value outside 0-100,
    # you might want to adjust the expectation or handle those cases.
    dlt.expect(
        "valid_methane_level",
        "methane_level >= 0 AND methane_level <= 100"
    )
    
    dlt.expect(
        "valid_pressure",
        "pressure >= 0 AND pressure <= 1000"
    )
    
    dlt.expect(
        "valid_alert_status",
        "alert_status IN ('NORMAL', 'WARNING', 'CRITICAL', 'UNKNOWN')"
    )
    
    # Add metadata columns
    df = df.withColumn(
        "processed_timestamp",
        F.current_timestamp()
    ).withColumn(
        "data_quality_score",
        F.when(F.col("alert_status") == "NORMAL", 1.0)
         .when(F.col("alert_status") == "WARNING", 0.7)
         .when(F.col("alert_status") == "CRITICAL", 0.3)
         .otherwise(0.5)
    )
    
    return df
  
  # Create views joining with asset and site information
@dlt.view(
    name="enriched_sensor_emissions",
    comment="Sensor emissions enriched with asset and site information for use in gold layer transformations"
)
def enriched_sensor_emissions():
    # Read the silver sensor emissions
    emissions_df = dlt.read("silver_sensor_emissions")
    
    # Read the bronze asset data using dlt.read()
    asset_df = dlt.read("bronze_asset_info").select(
        F.col("asset_id").cast("integer"),
        "asset_name",
        "asset_type",
        "operational_status"
    )
    
    # Read the bronze site data using dlt.read()
    site_df = dlt.read("bronze_site_info").select(
        F.col("site_id").cast("integer"),
        "site_name",
        "site_type",
        "location"
    )
    
    # Join the dataframes
    return emissions_df.join(
        asset_df,
        on="asset_id",
        how="left"
    ).join(
        site_df,
        on="site_id",
        how="left"
    )