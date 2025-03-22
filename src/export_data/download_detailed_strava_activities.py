import requests
import json
import csv
import polars as pl
import os
from datetime import datetime, timedelta
import time
import numpy as np

# Get the tokens from file to connect to Strava
with open('strava_token.json') as json_file:
    strava_tokens = json.load(json_file)

# Check if access token has expired
current_time = datetime.now().timestamp()
if strava_tokens['expires_at'] < current_time:
    # Make Strava auth API call with refresh token
    response = requests.post(
        'https://www.strava.com/oauth/token',
        data={
            'client_id': strava_tokens['client_id'],
            'client_secret': strava_tokens['client_secret'],
            'grant_type': 'refresh_token',
            'refresh_token': strava_tokens['refresh_token']
        }
    )
    
    # Save response as json in new variable
    new_strava_tokens = response.json()
    
    # Save new tokens to file
    with open('strava_token.json', 'w') as outfile:
        json.dump(new_strava_tokens, outfile)
    
    # Use new access token
    access_token = new_strava_tokens['access_token']
else:
    # Use current access token
    access_token = strava_tokens['access_token']

# Calculate timestamp for 3 days ago
three_days_ago = int((datetime.now() - timedelta(days=3)).timestamp())

# Get activities after this timestamp
activities_url = "https://www.strava.com/api/v3/athlete/activities"
params = {
    'access_token': access_token,
    'after': three_days_ago,
    'per_page': 100  # Max 200, but 100 is more reliable
}

# Get activities
response = requests.get(activities_url, params=params)
activities = response.json()

print(f"Found {len(activities)} activities in the last 3 days")

# Create output directory if it doesn't exist
output_dir = 'src/export_data/data/detailed_activities'
os.makedirs(output_dir, exist_ok=True)

# Save basic activities data
with open('src/export_data/data/strava_data.json', 'w') as outfile:
    json.dump(activities, outfile)

print("Basic data saved to src/export_data/data/strava_data.json")

# Function to get detailed data for an activity
def get_activity_details(activity_id):
    """Get detailed information for a specific activity"""
    url = f"https://www.strava.com/api/v3/activities/{activity_id}"
    headers = {'Authorization': f'Bearer {access_token}'}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching activity {activity_id}: {response.status_code}")
        return None

# Function to get stream data for an activity
def get_activity_streams(activity_id):
    """Get detailed stream data for a specific activity"""
    url = f"https://www.strava.com/api/v3/activities/{activity_id}/streams"
    headers = {'Authorization': f'Bearer {access_token}'}
    
    params = {
        'keys': 'time,distance,latlng,altitude,velocity_smooth,heartrate,cadence,watts,temp,moving',
        'key_by_type': True
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching streams for activity {activity_id}: {response.status_code}")
        return None

# Define heart rate zones (adjust according to your personal zones)
def calculate_hr_zones(max_hr):
    """Calculate heart rate zones based on max heart rate"""
    return {
        'Zone 1 (Recovery)': (0, int(max_hr * 0.6)),
        'Zone 2 (Endurance)': (int(max_hr * 0.6), int(max_hr * 0.7)),
        'Zone 3 (Tempo)': (int(max_hr * 0.7), int(max_hr * 0.8)),
        'Zone 4 (Threshold)': (int(max_hr * 0.8), int(max_hr * 0.9)),
        'Zone 5 (VO2 Max)': (int(max_hr * 0.9), int(max_hr * 1.0))
    }

# Define power zones (adjust according to your FTP)
def calculate_power_zones(ftp):
    """Calculate power zones based on FTP"""
    return {
        'Zone 1 (Active Recovery)': (0, int(ftp * 0.55)),
        'Zone 2 (Endurance)': (int(ftp * 0.55), int(ftp * 0.75)),
        'Zone 3 (Tempo)': (int(ftp * 0.75), int(ftp * 0.9)),
        'Zone 4 (Threshold)': (int(ftp * 0.9), int(ftp * 1.05)),
        'Zone 5 (VO2 Max)': (int(ftp * 1.05), int(ftp * 1.2)),
        'Zone 6 (Anaerobic)': (int(ftp * 1.2), int(ftp * 1.5)),
        'Zone 7 (Neuromuscular)': (int(ftp * 1.5), float('inf'))
    }

# Calculate time in zones from stream data
def calculate_time_in_zones(stream_data, zones, metric):
    """Calculate time spent in each zone"""
    if metric not in stream_data or 'time' not in stream_data:
        return None
    
    # Get the data points and timestamps
    values = stream_data[metric]
    times = stream_data['time']
    
    # Calculate the time difference between data points
    time_diffs = []
    for i in range(1, len(times)):
        time_diffs.append(times[i] - times[i-1])
    # Add the last time interval (estimate based on average)
    if time_diffs:
        time_diffs.append(sum(time_diffs) / len(time_diffs))
    
    # Initialize time spent in each zone
    time_in_zones = {zone_name: 0 for zone_name in zones.keys()}
    
    # Classify each data point into a zone and add its duration
    for i, value in enumerate(values):
        if value is None:
            continue
        
        for zone_name, (zone_min, zone_max) in zones.items():
            if zone_min <= value < zone_max:
                time_in_zones[zone_name] += time_diffs[i]
                break
    
    return time_in_zones

# Process each activity
all_activities_data = []

# User's HR max and FTP (ideally these would be stored in a config or profile)
# Replace these with your personal values
USER_HR_MAX = 190 # Example maximum heart rate
USER_FTP = 330 # Example FTP in watts

# Calculate zones
hr_zones = calculate_hr_zones(USER_HR_MAX)
power_zones = calculate_power_zones(USER_FTP)

for activity in activities:
    activity_id = activity['id']
    print(f"Processing activity: {activity['name']} (ID: {activity_id})")
    
    # Get detailed data
    details = get_activity_details(activity_id)
    
    if details:
        # Save detailed activity data
        activity_file = os.path.join(output_dir, f"activity_{activity_id}.json")
        with open(activity_file, 'w') as f:
            json.dump(details, f)
        
        # Get streams data
        streams = get_activity_streams(activity_id)
        
        hr_zone_data = {}
        power_zone_data = {}
        
        if streams:
            # Save streams data
            streams_file = os.path.join(output_dir, f"activity_{activity_id}_streams.json")
            with open(streams_file, 'w') as f:
                json.dump(streams, f)
            
            # Process streams into DataFrame format
            stream_data = {}
            for stream_type, data in streams.items():
                if 'data' in data:
                    stream_data[stream_type] = data['data']
            
            # Calculate time in HR zones
            if 'heartrate' in stream_data:
                hr_zone_data = calculate_time_in_zones(stream_data, hr_zones, 'heartrate')
            
            # Calculate time in power zones (for cycling activities)
            is_cycling = details.get('sport_type') in ['Ride', 'VirtualRide']
            if is_cycling and 'watts' in stream_data:
                power_zone_data = calculate_time_in_zones(stream_data, power_zones, 'watts')
            
            # Only create DataFrame if we have time data
            if 'time' in stream_data:
                # Build dict with consistent length arrays, use None for missing data
                time_length = len(stream_data['time'])
                df_data = {}
                
                for key in ['time', 'distance', 'heartrate', 'watts', 'cadence', 
                           'velocity_smooth', 'altitude', 'temp']:
                    if key in stream_data:
                        df_data[key] = stream_data[key]
                    else:
                        df_data[key] = [None] * time_length
                
                # Handle latlng separately as it's an array of [lat, lng] pairs
                if 'latlng' in stream_data:
                    df_data['latitude'] = [coord[0] for coord in stream_data['latlng']]
                    df_data['longitude'] = [coord[1] for coord in stream_data['latlng']]
                else:
                    df_data['latitude'] = [None] * time_length
                    df_data['longitude'] = [None] * time_length
                
                # Create Polars DataFrame
                stream_df = pl.DataFrame(df_data)
                
                # Save as CSV
                csv_file = os.path.join(output_dir, f"activity_{activity_id}_streams.csv")
                stream_df.write_csv(csv_file)
                
                print(f"  Saved detailed stream data to {csv_file}")
        
        # Save zone analysis to separate files
        if hr_zone_data:
            hr_zones_file = os.path.join(output_dir, f"activity_{activity_id}_hr_zones.json")
            with open(hr_zones_file, 'w') as f:
                json.dump(hr_zone_data, f)
            print(f"  Saved heart rate zone analysis to {hr_zones_file}")
        
        if power_zone_data:
            power_zones_file = os.path.join(output_dir, f"activity_{activity_id}_power_zones.json")
            with open(power_zones_file, 'w') as f:
                json.dump(power_zone_data, f)
            print(f"  Saved power zone analysis to {power_zones_file}")
        
        # Format the zone data for our summary
        hr_zone_summary = {}
        for zone, seconds in hr_zone_data.items():
            hr_zone_summary[f"hr_{zone.replace(' ', '_').lower()}"] = seconds
            hr_zone_summary[f"hr_{zone.replace(' ', '_').lower()}_minutes"] = round(seconds / 60, 1)
        
        power_zone_summary = {}
        for zone, seconds in power_zone_data.items():
            power_zone_summary[f"power_{zone.replace(' ', '_').lower()}"] = seconds
            power_zone_summary[f"power_{zone.replace(' ', '_').lower()}_minutes"] = round(seconds / 60, 1)
        
        # Add summary to our all activities data
        activity_summary = {
            'id': activity_id,
            'name': details.get('name'),
            'sport_type': details.get('sport_type'),
            'start_date': details.get('start_date'),
            'distance': details.get('distance'),
            'moving_time': details.get('moving_time'),
            'elapsed_time': details.get('elapsed_time'),
            'total_elevation_gain': details.get('total_elevation_gain'),
            'average_speed': details.get('average_speed'),
            'max_speed': details.get('max_speed'),
            'average_watts': details.get('average_watts'),
            'weighted_average_watts': details.get('weighted_average_watts'),
            'kilojoules': details.get('kilojoules'),
            'average_heartrate': details.get('average_heartrate'),
            'max_heartrate': details.get('max_heartrate'),
            'suffer_score': details.get('suffer_score'),
            'average_cadence': details.get('average_cadence'),
            'average_temp': details.get('average_temp'),
            'has_streams': streams is not None,
            'has_hr_zones': bool(hr_zone_data),
            'has_power_zones': bool(power_zone_data),
            **hr_zone_summary,
            **power_zone_summary
        }
        
        all_activities_data.append(activity_summary)
    
    # Sleep to avoid API rate limits
    time.sleep(1)

# Create summary DataFrame
if all_activities_data:
    activities_df = pl.DataFrame(all_activities_data)
    activities_csv = 'src/export_data/data/activities_last_3_days.csv'
    activities_df.write_csv(activities_csv)
    print(f"\nSaved summary of all activities to {activities_csv}")
    print(f"Total activities processed: {len(all_activities_data)}")
    
    # Print some zone analysis summaries
    for activity in all_activities_data:
        print(f"\nActivity: {activity['name']} ({activity['sport_type']})")
        
        if activity['has_hr_zones']:
            print("  Heart Rate Zone Analysis (minutes):")
            for key in sorted([k for k in activity.keys() if k.startswith('hr_') and k.endswith('_minutes')]):
                zone_name = key.replace('hr_', '').replace('_minutes', '').replace('_', ' ')
                print(f"    {zone_name.title()}: {activity[key]}")
        
        if activity['has_power_zones']:
            print("  Power Zone Analysis (minutes):")
            for key in sorted([k for k in activity.keys() if k.startswith('power_') and k.endswith('_minutes')]):
                zone_name = key.replace('power_', '').replace('_minutes', '').replace('_', ' ')
                print(f"    {zone_name.title()}: {activity[key]}")
else:
    print("No activities found in the last 3 days")

print("\nProcess completed!")