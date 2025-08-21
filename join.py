import pandas as pd
from datetime import datetime

# Load data
print("Loading data files...")
audits = pd.read_csv('search_audits.csv')
tewksbury = pd.read_csv('Tewksbury MA PD_Network_Audit_5_28_2025_6_18_2025.csv')

# Check the column names in both files
print("Columns in audits file:", audits.columns.tolist())
print("Columns in tewksbury file:", tewksbury.columns.tolist())

# Identify the correct column names
# Based on your data, the timestamp column in tewksbury might be named differently
# Let's find the column that contains timestamp data
timestamp_col = None
reason_col = None
name_col = None
org_col = None

for col in tewksbury.columns:
    sample_val = tewksbury[col].dropna().iloc[0] if not tewksbury[col].dropna().empty else ""
    if isinstance(sample_val, str) and ('/' in sample_val and ':' in sample_val and ('AM' in sample_val or 'PM' in sample_val)):
        timestamp_col = col
        print(f"Found timestamp column: {col}")
    elif isinstance(sample_val, str) and sample_val in ['INVESTIGATION', 'Inv', 'inv']:
        reason_col = col
        print(f"Found reason column: {col}")
    elif isinstance(sample_val, str) and len(sample_val) < 10 and any(c in sample_val for c in ['.', ' ']):
        name_col = col
        print(f"Found name column: {col}")
    elif isinstance(sample_val, str) and len(sample_val) > 10 and 'County' in sample_val:
        org_col = col
        print(f"Found org column: {col}")

# If we couldn't automatically detect, set manually based on your data structure
if not timestamp_col:
    timestamp_col = input("Enter the name of the timestamp column in tewksbury file: ")
if not reason_col:
    reason_col = input("Enter the name of the reason column in tewksbury file: ")
if not name_col:
    name_col = input("Enter the name of the name column in tewksbury file: ")
if not org_col:
    org_col = input("Enter the name of the org column in tewksbury file: ")

# Convert timestamps to a common format
def convert_tewksbury_time(ts):
    try:
        # Convert "6/19/2025, 10:08:20 AM UTC" to "2025-06-19 10:08:20"
        dt = datetime.strptime(ts, '%m/%d/%Y, %I:%M:%S %p UTC')
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        return None

print("Converting timestamps...")
tewksbury['converted_time'] = tewksbury[timestamp_col].apply(convert_tewksbury_time)

# For the audits table, remove milliseconds
audits['converted_time'] = audits['search_timestamp'].str.slice(0, 19)

# Merge data
print("Merging data...")
merged = pd.merge(audits, tewksbury, 
                 left_on=['converted_time', 'reason'], 
                 right_on=['converted_time', reason_col])

# Create mapping
print("Creating mapping...")
mapping = merged[['user_guid', name_col, 'search_guid', org_col]]
mapping.columns = ['user_uuid', 'user_name', 'search_uuid', 'org_name']

# Remove duplicates
mapping = mapping.drop_duplicates()

# Save to CSV
mapping.to_csv('uuid_name_mapping.csv', index=False)
print("Mapping saved to uuid_name_mapping.csv")

# Show sample of the mapping
print("\nSample of the mapping:")
print(mapping.head())
