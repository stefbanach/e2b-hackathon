import os 
import polars as pl
from icecream import ic
import json

os.system("clear")
# Configure polars to display up to 100 rows in terminal output
pl.Config.set_tbl_rows(100)

df = pl.read_csv("src/export_data/data/sleep_data.csv")
df = df.drop(["startDate", "endDate", "device", "creationDate"])
df = df.filter(pl.col("sourceName") != "AutoSleep")
df = df.drop(["sourceName"])

df = df.with_columns(
    pl.col("start_date").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S %z").alias("start_date"),
    pl.col("end_date").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S %z").alias("end_date"),
)

df = df.filter(pl.col("start_date").dt.date() == pl.datetime(2025, 3, 21, time_zone="UTC").dt.date())

df = df.filter(
    (pl.col("start_date") >= pl.datetime(2025, 3, 20, 12, 0, 0, time_zone="UTC"))
    & 
    (pl.col("start_date") <= pl.datetime(2025, 3, 21, 12, 0, 0, time_zone="UTC"))
    )

sleep_start_time = df.filter(pl.col("record_type") == "HKCategoryTypeIdentifierSleepAnalysis").select(pl.col("start_date")).min()[0]
sleep_end_time = df.filter(pl.col("record_type") == "HKCategoryTypeIdentifierSleepAnalysis").select(pl.col("end_date")).max()[0]

### HEART RATE (bpm) DATA
heart_rate_data = df.filter(
    (pl.col("record_type") == "HKQuantityTypeIdentifierHeartRate")
    &
    (pl.col("end_date") >= pl.lit(sleep_start_time))
    &
    (pl.col("start_date") <= pl.lit(sleep_end_time))
)
heart_rate_data = heart_rate_data.drop(["end_date", "type"]).rename({"start_date": "time_when_measured"})
heart_rate_data = heart_rate_data.with_columns(
    pl.lit('Heart Rate (bpm)').alias('record_type'), 
    pl.col("time_when_measured").dt.hour().alias("hour_when_measured"), 
    pl.col("value").cast(pl.Float64).alias("value")
    )

# Create a comprehensive heart rate analysis dataframe for LLM input
heart_rate_analysis = {
    # Basic statistics
    "basic_stats": heart_rate_data.select([
        pl.min("value").alias("min_heart_rate"),
        pl.max("value").alias("max_heart_rate"),
        pl.mean("value").alias("avg_heart_rate"),
        pl.median("value").alias("median_heart_rate"),
        pl.std("value").alias("std_heart_rate"),
        pl.count("value").alias("num_measurements")
    ]),
    
    # Percentiles for distribution analysis
    "percentiles": heart_rate_data.select([
        pl.col("value").quantile(0.05).alias("5th_percentile"),
        pl.col("value").quantile(0.25).alias("25th_percentile"),
        pl.col("value").quantile(0.5).alias("50th_percentile"),
        pl.col("value").quantile(0.75).alias("75th_percentile"),
        pl.col("value").quantile(0.95).alias("95th_percentile")
    ]),
    
    # Hourly distribution
    "hourly_distribution": heart_rate_data.group_by("hour_when_measured").agg([
        pl.count("value").alias("count"),
        pl.mean("value").alias("avg_hr"),
        pl.min("value").alias("min_hr"),
        pl.max("value").alias("max_hr"),
        pl.std("value").alias("std_hr")
    ]).sort("hour_when_measured"),
    
    # Time series data (for trend analysis)
    "time_series": heart_rate_data.select(["time_when_measured", "value"])
        .sort("time_when_measured"),
    
    "sleep_metadata": pl.DataFrame({
        "sleep_start_time": [sleep_start_time],
        "sleep_end_time": [sleep_end_time],
        # "total_sleep_duration_hours": [(sleep_end_time - sleep_start_time).dt.total_seconds() / 3600]
    })
}

# Calculate additional metrics
heart_rate_analysis["variability"] = pl.DataFrame({
    "heart_rate_range": [heart_rate_analysis["basic_stats"].item(0, "max_heart_rate") - 
                         heart_rate_analysis["basic_stats"].item(0, "min_heart_rate")],
    "coefficient_of_variation": [heart_rate_analysis["basic_stats"].item(0, "std_heart_rate") / 
                                heart_rate_analysis["basic_stats"].item(0, "avg_heart_rate") * 100]
})

# Calculate rate of change metrics
if len(heart_rate_analysis["time_series"]) > 1:
    time_series_df = heart_rate_analysis["time_series"]
    time_series_df = time_series_df.with_columns([
        pl.col("value").diff().alias("hr_change"),
        (pl.col("time_when_measured").diff().dt.total_seconds() / 60).alias("time_diff_minutes")
    ]).filter(pl.col("hr_change").is_not_null())
    
    # Calculate rate of change per minute
    time_series_df = time_series_df.with_columns([
        (pl.col("hr_change") / pl.col("time_diff_minutes")).alias("hr_change_per_minute")
    ])
    
    heart_rate_analysis["rate_of_change"] = time_series_df.select([
        pl.mean("hr_change_per_minute").alias("avg_hr_change_per_minute"),
        pl.max("hr_change_per_minute").alias("max_hr_increase_per_minute"),
        pl.min("hr_change_per_minute").alias("max_hr_decrease_per_minute")
    ])



### HRV DATA
hrv_data = df.filter(
    (pl.col("record_type") == "HKQuantityTypeIdentifierHeartRateVariabilitySDNN")
    &
    (pl.col("end_date") >= pl.lit(sleep_start_time))
    &
    (pl.col("start_date") <= pl.lit(sleep_end_time))
)
hrv_data = hrv_data.drop(["type"])
hrv_data = hrv_data.with_columns(pl.lit('Heart Rate Variability SDNN').alias('record_type'))

### Sleep Classification Data
sleep_classification_data = df.filter(pl.col("record_type") == "HKCategoryTypeIdentifierSleepAnalysis")

# Replace sleep classification values with more readable labels
sleep_classification_data = sleep_classification_data.with_columns(
    pl.when(pl.col("value") == "HKCategoryValueSleepAnalysisAsleepREM")
    .then(pl.lit("REM sleep"))
    .when(pl.col("value") == "HKCategoryValueSleepAnalysisAsleepDeep")
    .then(pl.lit("Deep sleep"))
    .when(pl.col("value") == "HKCategoryValueSleepAnalysisAsleepCore")
    .then(pl.lit("Asleep"))
    .when(pl.col("value") == "HKCategoryValueSleepAnalysisAwake")
    .then(pl.lit("Awake"))
    .otherwise(pl.col("value"))
    .alias("value")
)

# Calculate duration by subtracting end_date from start_date
sleep_classification_data = sleep_classification_data.with_columns(
    (pl.col("end_date").dt.timestamp() - pl.col("start_date").dt.timestamp()).alias("duration")
)

# Convert duration from seconds to minutes for better readability
sleep_classification_data = sleep_classification_data.with_columns(
    (pl.col("duration") / 60 / 60 / 1000 / 1000).alias("duration_hours")
)

# Group by sleep stage and sum the duration in minutes
sleep_duration_by_stage = sleep_classification_data.group_by("value").agg(
    pl.sum("duration_hours").alias("total_duration_hours")
).rename({"value": "sleep_stage"})

#########################
# Convert to a single dataframe for LLM input
heart_rate_llm_input = {
    "heart_rate_analysis": heart_rate_analysis,
    "analysis_date": sleep_start_time,
    "data_source": "Apple Health"
}

# Print summary of the analysis data
print("Heart Rate Analysis Data Structure Created for LLM Input")
print(f"Number of components: {len(heart_rate_analysis)}")
print(f"Basic stats shape: {heart_rate_analysis['basic_stats'].shape}")
print(f"Time series data points: {heart_rate_analysis['time_series'].shape[0]}")

# Export data to files for LLM processing
import os
from pathlib import Path

# Create data directory if it doesn't exist
data_dir = Path("src/analyze_data/data")
data_dir.mkdir(parents=True, exist_ok=True)

# Export heart rate data
heart_rate_file = data_dir / "sleep_data_heart_rate.json"
with open(heart_rate_file, "w") as f:
    json.dump(heart_rate_llm_input, f, default=str)
print(f"Heart rate data exported to {heart_rate_file}")

# Export sleep duration by stage data
sleep_duration_file = data_dir / "sleep_data_duration_by_stage.csv"
sleep_duration_by_stage.write_csv(sleep_duration_file)
print(f"Sleep duration data exported to {sleep_duration_file}")

# Export HRV data
hrv_file = data_dir / "sleep_data_hrv.csv"
hrv_data.write_csv(hrv_file)
print(f"HRV data exported to {hrv_file}")
