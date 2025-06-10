import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Daily emissions by asset
@dlt.table(
    name="gold_daily_asset_emissions",
    comment="Daily aggregated emissions metrics by asset",
    table_properties={
        "quality": "gold",
        "pipelines.reset.allowed": "false"
    }
)


def gold_daily_asset_emissions():
    # Read from the enriched view
    emissions_df = dlt.read("enriched_sensor_emissions")
    
    return (
        emissions_df.groupBy(
            F.date_trunc("day", "timestamp").alias("date"),
            "asset_id",
            "asset_name",
            "asset_type",
            "operational_status"
        ).agg(
            F.avg("methane_level").alias("avg_methane_level"),
            F.max("methane_level").alias("max_methane_level"),
            F.avg("co2_level").alias("avg_co2_level"),
            F.max("co2_level").alias("max_co2_level"),
            F.avg("nox_level").alias("avg_nox_level"),
            F.max("nox_level").alias("max_nox_level"),
            F.avg("data_quality_score").alias("avg_data_quality"),
            F.count("*").alias("measurement_count"),
            F.count(F.when(F.col("alert_status") == "CRITICAL", True)).alias("critical_alerts"),
            F.count(F.when(F.col("alert_status") == "WARNING", True)).alias("warning_alerts")
        )
    )

# Monthly site emissions summary
@dlt.table(
    name="gold_monthly_site_emissions",
    comment="Monthly emissions summary by site with compliance metrics",
    table_properties={
        "quality": "gold",
        "pipelines.reset.allowed": "false"
    }
)
def gold_monthly_site_emissions():
    emissions_df = dlt.read("enriched_sensor_emissions")
    
    return (
        emissions_df.groupBy(
            F.date_trunc("month", "timestamp").alias("month"),
            "site_id",
            "site_name",
            "site_type",
            "location"
        ).agg(
            # Emissions averages
            F.avg("methane_level").alias("avg_methane_level"),
            F.avg("co2_level").alias("avg_co2_level"),
            F.avg("nox_level").alias("avg_nox_level"),
            
            # Compliance metrics
            F.count("*").alias("total_measurements"),
            F.sum(F.when(F.col("data_quality_score") >= 0.7, 1).otherwise(0)).alias("high_quality_measurements"),
            F.count(F.when(F.col("alert_status").isin("CRITICAL", "WARNING"), True)).alias("total_alerts"),
            
            # Environmental conditions
            F.avg("temperature").alias("avg_temperature"),
            F.avg("pressure").alias("avg_pressure"),
            F.avg("flow_rate").alias("avg_flow_rate")
        ).withColumn(
            "data_quality_percentage",
            (F.col("high_quality_measurements") / F.col("total_measurements") * 100).cast("decimal(5,2)")
        )
    )

# Asset emissions trends
@dlt.table(
    name="gold_asset_emissions_trends",
    comment="Rolling averages and trend analysis for asset emissions",
    table_properties={
        "quality": "gold",
        "pipelines.reset.allowed": "true"
    }
)
def gold_asset_emissions_trends():
    emissions_df = dlt.read("enriched_sensor_emissions")
    
    # First, create the base dataframe with proper timestamp columns
    base_df = (
        emissions_df
        .withColumn(
            "timestamp_hour",
            F.date_trunc("hour", "timestamp")
        )
        .withColumn(
            "timestamp_day",
            F.date_trunc("day", "timestamp")
        )
        .withColumn(
            "unix_timestamp_hour",
            F.unix_timestamp("timestamp_hour")
        )
        .withColumn(
            "unix_timestamp_day",
            F.unix_timestamp("timestamp_day")
        )
    )
    
    # Create hourly aggregations
    hourly_df = (
        base_df
        .groupBy(
            "timestamp_hour",
            "unix_timestamp_hour",
            "timestamp_day",
            "asset_id",
            "asset_name",
            "asset_type"
        )
        .agg(
            F.avg("methane_level").alias("hourly_methane_level"),
            F.avg("co2_level").alias("hourly_co2_level"),
            F.avg("nox_level").alias("hourly_nox_level")
        )
    )
    
    # Create daily aggregations for weekly trends
    daily_df = (
        base_df
        .groupBy(
            "timestamp_day",
            "unix_timestamp_day",
            "asset_id"
        )
        .agg(
            F.avg("methane_level").alias("daily_methane_level"),
            F.avg("co2_level").alias("daily_co2_level")
        )
    )
    
    # Define windows for rolling averages
    # For hourly trends within a day (3 hours = 10800 seconds)
    hourly_window = (
        Window.partitionBy("asset_id", "timestamp_day")
        .orderBy("unix_timestamp_hour")
        .rangeBetween(-10800, 0)
    )
    
    # For daily trends within a week (7 days = 604800 seconds)
    daily_window = (
        Window.partitionBy("asset_id")
        .orderBy("unix_timestamp_day")
        .rangeBetween(-604800, 0)
    )
    
    # Calculate rolling averages for daily data
    daily_trends = (
        daily_df
        .withColumn(
            "weekly_avg_methane",
            F.avg("daily_methane_level").over(daily_window)
        )
        .withColumn(
            "weekly_avg_co2",
            F.avg("daily_co2_level").over(daily_window)
        )
    )
    
    # Join hourly and daily trends
    final_df = (
        hourly_df
        .join(
            daily_trends,
            on=["asset_id", "timestamp_day"],
            how="left"
        )
        # Calculate rolling averages for hourly data
        .withColumn(
            "rolling_3hr_methane",
            F.avg("hourly_methane_level").over(hourly_window)
        )
        .withColumn(
            "rolling_3hr_co2",
            F.avg("hourly_co2_level").over(hourly_window)
        )
        # Calculate trends
        .withColumn(
            "intraday_methane_trend",
            F.when(
                F.col("hourly_methane_level") > F.col("rolling_3hr_methane"),
                "INCREASING"
            ).when(
                F.col("hourly_methane_level") < F.col("rolling_3hr_methane"),
                "DECREASING"
            ).otherwise("STABLE")
        )
        .withColumn(
            "weekly_methane_trend",
            F.when(
                F.col("daily_methane_level") > F.col("weekly_avg_methane"),
                "INCREASING"
            ).when(
                F.col("daily_methane_level") < F.col("weekly_avg_methane"),
                "DECREASING"
            ).otherwise("STABLE")
        )
        # Add percentage changes
        .withColumn(
            "intraday_methane_change_pct",
            F.when(F.col("rolling_3hr_methane").isNotNull(),
                ((F.col("hourly_methane_level") - F.col("rolling_3hr_methane")) / F.col("rolling_3hr_methane") * 100)
            ).cast("decimal(5,2)")
        )
        .withColumn(
            "weekly_methane_change_pct",
            F.when(F.col("weekly_avg_methane").isNotNull(),
                ((F.col("daily_methane_level") - F.col("weekly_avg_methane")) / F.col("weekly_avg_methane") * 100)
            ).cast("decimal(5,2)")
        )
        # Drop the unix timestamp columns as they're no longer needed
        .drop("unix_timestamp_hour", "unix_timestamp_day")
    )
    
    return final_df 