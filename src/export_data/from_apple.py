import polars as pl
from icecream import ic
import xml.etree.ElementTree as ET
import os

# Parse the XML file
tree = ET.parse('src/export_data/data/export.xml')
root = tree.getroot()


# Extract sleep data
sleep_data = []
print("aa")
# Define the health record types we want to extract
health_record_types = [
    'HKCategoryTypeIdentifierSleepAnalysis',
    'HKQuantityTypeIdentifierHeartRateVariabilitySDNN',
    'HKQuantityTypeIdentifierHeartRate'
]

# Use one loop to extract all types of health data
for record_type in health_record_types:
    for index, record in enumerate(root.findall(f".//Record[@type='{record_type}']")):
        print(f"Processing {record_type} record {index}")
        sleep_data.append({
            'record_type': record_type,
            'start_date': record.attrib.get('startDate'),
            'end_date': record.attrib.get('endDate'),
            'value': record.attrib.get('value'),
            **record.attrib  # Append all attributes from the record
        })

print("aa")
sleep_df = pl.DataFrame(sleep_data)

# Export the sleep dataframe to a CSV file
sleep_df.write_csv('src/export_data/data/sleep_data.csv')

ic(sleep_df)