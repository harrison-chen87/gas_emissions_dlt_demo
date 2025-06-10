import dlt
from pyspark.sql import functions as F

# Base path for all data files
BASE_PATH = "/Volumes/harrison_chen_catalog/synthetic_energy/energy_volume/Gas_Emissions"

# Bronze asset information
@dlt.table(
    name="bronze_asset",
    comment="Raw asset information from CSV files",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false"
    }
)
@dlt.expect_or_fail("valid_asset_ids", "asset_id IS NOT NULL")
def bronze_asset():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{BASE_PATH}/asset/*.csv")
    )

# Bronze shift schedule
@dlt.table(
    name="bronze_shift_schedule",
    comment="Raw shift schedule information from CSV files",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false"
    }
)
@dlt.expect_or_fail("valid_shift_ids", "shift_id IS NOT NULL AND site_id IS NOT NULL")
def bronze_shift_schedule():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{BASE_PATH}/shift_schedule/*.csv")
    )

# Bronze daily weather - fact table
@dlt.table(
    name="bronze_daily_weather",
    comment="Raw daily weather measurements from CSV files",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false"
    }
)
@dlt.expect_or_fail("valid_weather_site_id", "site_id IS NOT NULL")
def bronze_daily_weather():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{BASE_PATH}/daily_weather/*.csv")
    )

# Bronze maintenance records - fact table
@dlt.table(
    name="bronze_maintenance_record",
    comment="Raw maintenance event records from CSV files",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false"
    }
)
@dlt.expect_or_fail("valid_maintenance_ids", "maintenance_id IS NOT NULL AND asset_id IS NOT NULL")
def bronze_maintenance_record():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{BASE_PATH}/maintenance_record/*.csv")
    )

# Bronze asset configuration changes - change feed table
@dlt.table(
    name="bronze_asset_config_changes",
    comment="Raw asset configuration change feed from CSV files",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_fail("valid_config_ids", "config_id IS NOT NULL AND asset_id IS NOT NULL")
def bronze_asset_config_changes():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{BASE_PATH}/asset_config_changes/*.csv")
    )

# Bronze valve compliance changes - change feed table
@dlt.table(
    name="bronze_valve_compliance_changes",
    comment="Raw valve compliance change feed from CSV files",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_fail("valid_valve_ids", "valve_id IS NOT NULL AND asset_id IS NOT NULL")
def bronze_valve_compliance_changes():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{BASE_PATH}/valve_compliance_changes/*.csv")
    )

# Bronze alert history - fact table
@dlt.table(
    name="bronze_alert_history",
    comment="Raw alert history records from CSV files",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false"
    }
)
@dlt.expect_or_fail("valid_alert_ids", "alert_id IS NOT NULL AND asset_id IS NOT NULL")
def bronze_alert_history():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{BASE_PATH}/alert_history/*.csv")
    )

# Bronze gas production - fact table
@dlt.table(
    name="bronze_gas_production",
    comment="Raw gas production measurements from CSV files",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false"
    }
)
@dlt.expect_or_fail("valid_production_ids", "production_id IS NOT NULL AND site_id IS NOT NULL")
def bronze_gas_production():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{BASE_PATH}/gas_production/*.csv")
    )

# Bronze inspectors - dimension table
@dlt.table(
    name="bronze_inspectors",
    comment="Raw inspector information from CSV files",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false"
    }
)
@dlt.expect_or_fail("valid_inspector_ids", "inspector_id IS NOT NULL")
def bronze_inspectors():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{BASE_PATH}/inspectors/*.csv")
    )


# Bronze calibration records - fact table
@dlt.table(
    name="bronze_calibration_records",
    comment="Raw calibration records from CSV files",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false"
    }
)
@dlt.expect_or_fail("valid_calibration_ids", "calibration_id IS NOT NULL AND asset_id IS NOT NULL")
def bronze_calibration_records():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{BASE_PATH}/calibration_records/*.csv")
    )

# Bronze compliance regulations - dimension table
@dlt.table(
    name="bronze_compliance_regulations",
    comment="Raw compliance regulation information from CSV files",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false"
    }
)
@dlt.expect_or_fail("valid_regulation_ids", "regulation_id IS NOT NULL")
def bronze_compliance_regulations():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{BASE_PATH}/compliance_regulations/*.csv")
    )