import argparse
import datetime

# Set up argument parser
parser = argparse.ArgumentParser(description='Process stock data with specified parameters.')

# Add arguments
parser.add_argument('--Raw_Start_Date', type=str, required=True, 
                    help='Start date and time for raw data (format: YYYY-MM-DDTHH:mm:ss)')
parser.add_argument('--Raw_End_Date', type=str, required=True, 
                    help='End date and time for raw data (format: YYYY-MM-DDTHH:mm:ss)')
parser.add_argument('--Start_Index_1min', type=int, required=True, 
                    help='Start index for 1-minute data')
parser.add_argument('--Stop_Index_1min', type=int, required=True, 
                    help='Stop index for 1-minute data')
parser.add_argument('--Execution_Type', type=str, choices=['Hard_Start', 'Schedule_Start'], required=True, 
                    help='Execution type')

parser.add_argument('--Hibernation_Req', choices=['Yes', 'No'], help='Hibernation Required')  # Add this line

# Parse arguments
args = parser.parse_args()

# Extract and parse datetime arguments
try:
    raw_start_date = datetime.datetime.fromisoformat(args.Raw_Start_Date)
    raw_end_date = datetime.datetime.fromisoformat(args.Raw_End_Date)
except ValueError as e:
    raise ValueError(f"Invalid date format. Ensure the format is YYYY-MM-DDTHH:mm:ss. Error: {e}")

# Extract other arguments
start_index_1min = args.Start_Index_1min
stop_index_1min = args.Stop_Index_1min
execution_type = args.Execution_Type

# Debug prints (optional)
print(f"Raw Start Date: {raw_start_date} (type: {type(raw_start_date)})")
print(f"Raw End Date: {raw_end_date} (type: {type(raw_end_date)})")
print(f"Start Index: {start_index_1min}")
print(f"Stop Index: {stop_index_1min}")
print(f"Execution Type: {execution_type}")
print(f"Hibernation Required: {args.Hibernation_Req}")  # Print or use the argument

# Main logic
if raw_start_date >= raw_end_date:
    raise ValueError("Raw_Start_Date must be earlier than Raw_End_Date.")

# Example processing logic
print("Processing data...")
print(f"Time Range: {raw_start_date} to {raw_end_date}")
print(f"Indexes: {start_index_1min} to {stop_index_1min}")
print(f"Execution type: {execution_type}")

# Add further processing logic here as needed...
