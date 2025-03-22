import os 
import polars as pl
import json
import numpy as np
from icecream import ic
from pathlib import Path
from datetime import datetime

os.system("clear")

# Define paths
ACTIVITIES_SUMMARY_PATH = 'src/export_data/data/activities_last_3_days.csv'
DETAILED_ACTIVITIES_DIR = 'src/export_data/data/detailed_activities'

# Function to convert datetime objects to strings in a format suitable for JSON
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

# Load the summary data
summary_df = pl.read_csv(ACTIVITIES_SUMMARY_PATH)
ic("Loaded summary data:", summary_df.shape)

# Function to parse datetime strings
def parse_datetime(dt_str):
    if dt_str:
        return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")
    return None

# Process summary statistics
activities_summary = {}

for activity in summary_df.to_dicts():
    activity_id = str(activity['id'])
    
    # Convert start date to datetime object
    activity['start_datetime'] = parse_datetime(activity['start_date'])
    
    # Calculate additional metrics
    if activity.get('distance') and activity.get('moving_time'):
        # Convert distance to km
        activity['distance_km'] = activity['distance'] / 1000
        
        # Convert moving time to hours
        activity['moving_time_hours'] = activity['moving_time'] / 3600
        
        # Calculate pace (min/km) for runs and swims
        if activity['sport_type'] in ['Run', 'Swim']:
            pace_s_per_km = activity['moving_time'] / activity['distance_km']
            activity['pace_min_per_km'] = pace_s_per_km / 60
            activity['pace_formatted'] = f"{int(pace_s_per_km // 60)}:{int(pace_s_per_km % 60):02d}"
    
    # Calculate zone percentages - FIX: Handle None values
    for zone_type in ['hr', 'power']:
        zone_keys = [k for k in activity.keys() if k.startswith(f"{zone_type}_zone") and not k.endswith("_minutes")]
        # Make sure to convert None values to 0
        total_time = sum(activity.get(k, 0) or 0 for k in zone_keys)
        
        if total_time > 0:
            for zone_key in zone_keys:
                pct_key = f"{zone_key}_percent"
                # Also handle None values here
                zone_value = activity.get(zone_key, 0) or 0
                activity[pct_key] = (zone_value / total_time) * 100
    
    # Store in our activities dictionary
    activities_summary[activity_id] = activity

print(f"Processed {len(activities_summary)} activities in summary data")

# Load and process detailed activity data
detailed_activities = {}
streams_sample_rate = {}
hr_zone_summaries = {}
power_zone_summaries = {}

# Create directories if they don't exist
os.makedirs('src/analyze_data/data', exist_ok=True)

# Check if detailed activities directory exists
if not os.path.exists(DETAILED_ACTIVITIES_DIR):
    print(f"Warning: Detailed activities directory not found: {DETAILED_ACTIVITIES_DIR}")
    detailed_files = []
else:
    detailed_files = list(Path(DETAILED_ACTIVITIES_DIR).glob("activity_*.json"))
    print(f"Found {len(detailed_files)} detailed activity files")

for file_path in detailed_files:
    filename = file_path.name
    
    # Skip stream files - we'll process them separately
    if "streams" in filename or "zones" in filename:
        continue
    
    # Extract activity ID from filename
    activity_id = filename.replace("activity_", "").replace(".json", "")
    
    # Load the detailed activity data
    with open(file_path, 'r') as f:
        activity_data = json.load(f)
    
    # Store the detailed data
    detailed_activities[activity_id] = activity_data
    
    # Look for corresponding stream data
    stream_file = file_path.parent / f"activity_{activity_id}_streams.csv"
    if stream_file.exists():
        # Load the stream data
        stream_df = pl.read_csv(stream_file)
        
        # Calculate stream statistics
        if 'time' in stream_df.columns:
            # Calculate sample rate
            time_diffs = stream_df['time'].diff().drop_nulls()
            avg_sample_rate = time_diffs.mean()
            streams_sample_rate[activity_id] = avg_sample_rate
            
            # Calculate additional metrics from streams
            stream_stats = {}
            
            # Heart rate variability (if heartrate data exists)
            if 'heartrate' in stream_df.columns:
                heartrate = stream_df['heartrate'].drop_nulls()
                if len(heartrate) > 0:
                    stream_stats['hr_min'] = heartrate.min()
                    stream_stats['hr_max'] = heartrate.max()
                    stream_stats['hr_median'] = heartrate.median()
                    stream_stats['hr_std'] = heartrate.std()
                    stream_stats['hr_percentiles'] = {
                        'p10': heartrate.quantile(0.1),
                        'p25': heartrate.quantile(0.25), 
                        'p75': heartrate.quantile(0.75),
                        'p90': heartrate.quantile(0.9)
                    }
            
            # Power statistics (for cycling)
            if 'watts' in stream_df.columns:
                watts = stream_df['watts'].drop_nulls()
                if len(watts) > 0:
                    stream_stats['power_min'] = watts.min()
                    stream_stats['power_max'] = watts.max()
                    stream_stats['power_median'] = watts.median()
                    stream_stats['power_std'] = watts.std()
                    stream_stats['power_percentiles'] = {
                        'p10': watts.quantile(0.1),
                        'p25': watts.quantile(0.25), 
                        'p75': watts.quantile(0.75),
                        'p90': watts.quantile(0.9)
                    }
                    
                    # Calculate normalized power using 30-second rolling average
                    if len(watts) >= 30:
                        # Fix the warning by using native operator instead of map_elements
                        rolling_power = watts.rolling_mean(window_size=30)
                        rolling_power = rolling_power.drop_nulls()
                        if len(rolling_power) > 0:
                            # Use native power operator instead of map_elements
                            fourth_power = rolling_power ** 4
                            avg_fourth_power = fourth_power.mean()
                            normalized_power = avg_fourth_power**(1/4)
                            stream_stats['normalized_power'] = normalized_power
            
            # Speed/pace variability
            if 'velocity_smooth' in stream_df.columns:
                velocity = stream_df['velocity_smooth'].drop_nulls()
                if len(velocity) > 0:
                    stream_stats['speed_min'] = velocity.min()
                    stream_stats['speed_max'] = velocity.max()
                    stream_stats['speed_median'] = velocity.median()
                    stream_stats['speed_std'] = velocity.std()
                    stream_stats['speed_percentiles'] = {
                        'p10': velocity.quantile(0.1),
                        'p25': velocity.quantile(0.25), 
                        'p75': velocity.quantile(0.75),
                        'p90': velocity.quantile(0.9)
                    }
            
            # Add stream stats to the activity
            if activity_id in activities_summary:
                activities_summary[activity_id]['stream_stats'] = stream_stats
    
    # Look for HR zone data
    hr_zones_file = file_path.parent / f"activity_{activity_id}_hr_zones.json"
    if hr_zones_file.exists():
        with open(hr_zones_file, 'r') as f:
            hr_zone_data = json.load(f)
            hr_zone_summaries[activity_id] = hr_zone_data
    
    # Look for power zone data
    power_zones_file = file_path.parent / f"activity_{activity_id}_power_zones.json"
    if power_zones_file.exists():
        with open(power_zones_file, 'r') as f:
            power_zone_data = json.load(f)
            power_zone_summaries[activity_id] = power_zone_data

print(f"Processed {len(detailed_activities)} detailed activities")
print(f"Found stream data for {len(streams_sample_rate)} activities")
print(f"Found HR zone data for {len(hr_zone_summaries)} activities")
print(f"Found power zone data for {len(power_zone_summaries)} activities")

# Create aggregated statistics by sport type
sport_type_stats = {}

for activity in activities_summary.values():
    sport_type = activity['sport_type']
    
    if sport_type not in sport_type_stats:
        sport_type_stats[sport_type] = {
            'count': 0,
            'total_distance': 0,
            'total_duration': 0,
            'total_elevation': 0,
            'hr_zones': {
                'Zone 1 (Recovery)': 0,
                'Zone 2 (Endurance)': 0,
                'Zone 3 (Tempo)': 0,
                'Zone 4 (Threshold)': 0,
                'Zone 5 (VO2 Max)': 0
            },
            'power_zones': {
                'Zone 1 (Active Recovery)': 0,
                'Zone 2 (Endurance)': 0,
                'Zone 3 (Tempo)': 0, 
                'Zone 4 (Threshold)': 0,
                'Zone 5 (VO2 Max)': 0,
                'Zone 6 (Anaerobic)': 0,
                'Zone 7 (Neuromuscular)': 0
            }
        }
    
    # Update basic stats - handle None values
    sport_type_stats[sport_type]['count'] += 1
    sport_type_stats[sport_type]['total_distance'] += activity.get('distance', 0) or 0
    sport_type_stats[sport_type]['total_duration'] += activity.get('moving_time', 0) or 0
    sport_type_stats[sport_type]['total_elevation'] += activity.get('total_elevation_gain', 0) or 0
    
    # Update HR zones
    activity_id = str(activity['id'])
    if activity_id in hr_zone_summaries:
        for zone, seconds in hr_zone_summaries[activity_id].items():
            if zone in sport_type_stats[sport_type]['hr_zones']:
                sport_type_stats[sport_type]['hr_zones'][zone] += seconds or 0
    
    # Update power zones (cycling only)
    if sport_type in ['Ride', 'VirtualRide'] and activity_id in power_zone_summaries:
        for zone, seconds in power_zone_summaries[activity_id].items():
            if zone in sport_type_stats[sport_type]['power_zones']:
                sport_type_stats[sport_type]['power_zones'][zone] += seconds or 0

# Calculate averages and percentages for each sport type
for sport_type, stats in sport_type_stats.items():
    if stats['count'] > 0:
        # Calculate averages
        stats['avg_distance'] = stats['total_distance'] / stats['count']
        stats['avg_duration'] = stats['total_duration'] / stats['count']
        stats['avg_elevation'] = stats['total_elevation'] / stats['count']
        
        # Calculate HR zone percentages
        total_hr_time = sum(stats['hr_zones'].values())
        if total_hr_time > 0:
            stats['hr_zone_percentages'] = {
                zone: (seconds / total_hr_time) * 100
                for zone, seconds in stats['hr_zones'].items()
            }
        
        # Calculate power zone percentages (for cycling)
        if sport_type in ['Ride', 'VirtualRide']:
            total_power_time = sum(stats['power_zones'].values())
            if total_power_time > 0:
                stats['power_zone_percentages'] = {
                    zone: (seconds / total_power_time) * 100
                    for zone, seconds in stats['power_zones'].items()
                }

# Build a final dataset for LLM analysis
llm_analysis_data = {
    "activity_summaries": activities_summary,
    "detailed_activities": detailed_activities,
    "sport_type_statistics": sport_type_stats,
    "hr_zone_data": hr_zone_summaries,
    "power_zone_data": power_zone_summaries,
    "metadata": {
        "analysis_time": datetime.now().isoformat(),
        "total_activities": len(activities_summary),
        "sport_types": list(sport_type_stats.keys()),
        "time_period": "Last 3 days"
    }
}

# Save the JSON structure for LLM analysis
output_file = 'src/analyze_data/data/strava_llm_analysis_data.json'
with open(output_file, 'w') as f:
    # Use the custom serializer to handle datetime objects
    json.dump(llm_analysis_data, f, indent=2, default=json_serial)

print(f"Saved comprehensive analysis data to {output_file}")

# Create a summary dataframe with key statistics
summary_rows = []

for activity_id, activity in activities_summary.items():
    row = {
        'id': activity_id,
        'name': activity.get('name'),
        'sport_type': activity.get('sport_type'),
        'date': activity.get('start_date'),
        'distance_km': activity.get('distance_km'),
        'duration_min': activity.get('moving_time') / 60 if activity.get('moving_time') else None,
        'avg_hr': activity.get('average_heartrate'),
        'max_hr': activity.get('max_heartrate')
    }
    
    # Add pace for runs and swims, speed for cycling
    if activity.get('sport_type') in ['Run', 'Swim']:
        row['pace_min_km'] = activity.get('pace_min_per_km')
    else:
        row['avg_speed_kmh'] = activity.get('average_speed') * 3.6 if activity.get('average_speed') else None
    
    # Add power data for cycling
    if activity.get('sport_type') in ['Ride', 'VirtualRide']:
        row['avg_power'] = activity.get('average_watts')
        row['weighted_power'] = activity.get('weighted_average_watts')
        if 'stream_stats' in activity and 'normalized_power' in activity['stream_stats']:
            row['normalized_power'] = activity['stream_stats']['normalized_power']
    
    # Add zone percentages
    for zone_type in ['hr', 'power']:
        zone_pct_keys = [k for k in activity.keys() if k.startswith(f"{zone_type}_zone") and k.endswith("_percent")]
        for key in zone_pct_keys:
            simple_key = key.replace('_percent', '').replace('hr_', 'hr_pct_').replace('power_', 'power_pct_')
            row[simple_key] = activity.get(key)
    
    summary_rows.append(row)

# Create a polars DataFrame from the summary rows
activities_df = pl.DataFrame(summary_rows)

# Save the summary DataFrame as CSV
summary_csv_path = 'src/analyze_data/data/activities_analysis_summary.csv'
activities_df.write_csv(summary_csv_path)
print(f"Saved activities summary to {summary_csv_path}")

# Print some key statistics
print("Summary statistics by sport type:")
for sport_type, stats in sport_type_stats.items():
    print(f"\n{sport_type}:")
    print(f"  Activities: {stats['count']}")
    print(f"  Total distance: {stats['total_distance']/1000:.2f} km")
    print(f"  Total duration: {stats['total_duration']/60:.2f} minutes")
    
    if 'hr_zone_percentages' in stats:
        print("  Heart Rate Zone Distribution:")
        for zone, pct in stats['hr_zone_percentages'].items():
            print(f"    {zone}: {pct:.1f}%")
    
    if sport_type in ['Ride', 'VirtualRide'] and 'power_zone_percentages' in stats:
        print("  Power Zone Distribution:")
        for zone, pct in stats['power_zone_percentages'].items():
            print(f"    {zone}: {pct:.1f}%")

print("\nProcess completed!")


