import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
import sys

# Find the latest promo folder
output_dir = Path('/app/processed')

promo_folders = list(output_dir.glob('promo_*'))

if not promo_folders:
    print("ERROR: No promo folders found in /app/processed")
    sys.exit(1)

# Sort by modification time (most recent first)
promo_folders.sort(key=lambda x: x.stat().st_mtime, reverse=True)

latest_folder = promo_folders[0]
print(f"Found latest promo folder: {latest_folder}")

# Find the Excel file in the latest folder
excel_files = list(latest_folder.glob('final_analysis_*.xlsx'))

if not excel_files:
    print(f"ERROR: No Excel files found in {latest_folder}")
    sys.exit(1)

# If multiple files, get the most recent one
excel_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
input_file = excel_files[0]

print(f"Using Excel file: {input_file}")

# Read the Excel file
df = pd.read_excel(input_file)

# Convert date columns to datetime, handling errors
df['Start_Date'] = pd.to_datetime(df['Start_Date'], errors='coerce')
df['End_Date'] = pd.to_datetime(df['End_Date'], errors='coerce')

# For 'Ongoing' or invalid end dates, set to a far future date (1 year from start)
df['End_Date'] = df.apply(
    lambda row: row['Start_Date'] + timedelta(days=365) if pd.isna(row['End_Date']) else row['End_Date'],
    axis=1
)

# Remove rows where Start_Date is invalid
df = df.dropna(subset=['Start_Date'])

# Dictionary to hold all events by date
events_by_date = defaultdict(list)

# Process each row
for idx, row in df.iterrows():
    resort = row['Resort']
    deal = row['Deals']
    start_date = row['Start_Date']
    end_date = row['End_Date']
    
    # Create event string
    event_text = f"{resort} - {deal}"
    
    # Add event to all dates in the range
    current_date = start_date
    while current_date <= end_date:
        date_key = current_date.strftime('%d-%m-%Y')
        events_by_date[date_key].append(event_text)
        current_date += timedelta(days=1)

# Sort dates
sorted_dates = sorted(events_by_date.keys(), key=lambda x: datetime.strptime(x, '%d-%m-%Y'))

# Generate the calendar text
output_lines = []
for date_str in sorted_dates:
    # Add date header
    output_lines.append(date_str)
    output_lines.append("=" * len(date_str))
    
    # Add events (remove duplicates for the same date)
    unique_events = list(set(events_by_date[date_str]))
    unique_events.sort()  # Sort alphabetically
    
    for event in unique_events:
        output_lines.append(f"- {event}")
    
    output_lines.append("")  # Empty line between dates

# Write to file (save in the same promo folder)
output_file = 'app/calendar' / 'calendar.txt'
with open(output_file, 'w', encoding='utf-8') as f:
    f.write('\n'.join(output_lines))

print(f"\n{'='*60}")
print(f"Calendar generated successfully!")
print(f"Output file: {output_file}")
print(f"Total dates with events: {len(sorted_dates)}")
print(f"{'='*60}")
print(f"\nFirst few entries:")
print('\n'.join(output_lines[:30]))