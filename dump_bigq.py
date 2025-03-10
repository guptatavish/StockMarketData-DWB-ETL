import os
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import json

# Load environment variables
load_dotenv()

# Set up Google Cloud authentication using service account key
service_account_key = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if service_account_key:
    # Write service account JSON to temporary file
    key_path = "temp_key.json"
    with open(key_path, 'w') as key_file:
        key_file.write(service_account_key)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
else:
    # Alternatively, look for path to credentials file
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not key_path:
        raise ValueError("No Google Cloud credentials found in environment variables")

# Project, dataset, and table information
PROJECT_ID = "marine-lodge-453310-g5"
DATASET_ID = "StockMktData"
TABLE_ID = "StockData"

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)
print(f"BigQuery client initialized for project: {PROJECT_ID}")

# Create or get dataset
dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
try:
    dataset = client.get_dataset(dataset_id)
    print(f"Dataset {DATASET_ID} already exists.")
except NotFound:
    print(f"Dataset {DATASET_ID} not found. Creating now...")
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset = client.create_dataset(dataset, timeout=30)
    print(f"Dataset {DATASET_ID} created.")

# Define schema based on the CSV structure
schema = [
    bigquery.SchemaField("stock_name", "STRING", description="Name of the stock"),
    bigquery.SchemaField("Date", "DATE", description="Date of the stock data"),
    bigquery.SchemaField("Price", "FLOAT", description="Closing price"),
    bigquery.SchemaField("Open", "FLOAT", description="Opening price"),
    bigquery.SchemaField("High", "FLOAT", description="High price"),
    bigquery.SchemaField("Low", "FLOAT", description="Low price"),
    bigquery.SchemaField("Vol", "FLOAT", description="Trading volume"),
    bigquery.SchemaField("Change", "FLOAT", description="Percentage change")
]

# Create or get table
table_id = f"{dataset_id}.{TABLE_ID}"
try:
    table = client.get_table(table_id)
    print(f"Table {TABLE_ID} already exists.")
except NotFound:
    print(f"Table {TABLE_ID} not found. Creating now...")
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(f"Table {TABLE_ID} created.")

# Function to convert volume strings to float values
def convert_to_float(x):
    if pd.isna(x) or x == '' or x == 'nan':
        return None
    
    try:
        x = str(x).strip()
        if x.endswith('K'):
            return float(x[:-1]) * 1e3
        elif x.endswith('M'):
            return float(x[:-1]) * 1e6
        elif x.endswith('B'):
            return float(x[:-1]) * 1e9
        else:
            return float(x.replace(',', ''))
    except (ValueError, TypeError):
        print(f"Warning: Could not convert '{x}' to float")
        return None

# Read the CSV file
try:
    print("Reading CSV file...")
    data = pd.read_csv("stock_data.csv")
    print(f"CSV loaded successfully with {len(data)} rows and columns: {', '.join(data.columns)}")
    
    # Process data - handle different column naming conventions
    
    # Process Date column
    if 'Date' in data.columns:
        data['Date'] = pd.to_datetime(data['Date']).dt.date
        print("Date column converted to date format")
    
    # Process numeric columns by removing commas and converting to float
    numeric_columns = ['Price', 'Open', 'High', 'Low']
    for col in numeric_columns:
        if col in data.columns:
            try:
                data[col] = data[col].astype(str).str.replace(',', '').apply(
                    lambda x: float(x) if x and x != 'nan' else None
                )
                print(f"Converted {col} to numeric format")
            except Exception as e:
                print(f"Warning: Error processing {col} column: {e}")
    
    # Process volume column - handle different naming conventions
    vol_column = next((c for c in data.columns if c in ['Vol', 'Vol.', 'Volume', 'VOL']), None)
    if vol_column:
        data.rename(columns={vol_column: 'Vol'}, inplace=True)
        data['Vol'] = data['Vol'].astype(str).apply(convert_to_float)
        print(f"Processed volume column '{vol_column}' and renamed to 'Vol'")
    
    # Process Change column
    if 'Change %' in data.columns:
        data.rename(columns={'Change %': 'Change'}, inplace=True)
    
    if 'Change' in data.columns:
        data['Change'] = data['Change'].astype(str).str.rstrip('%').apply(
            lambda x: float(x) if x and x != 'nan' else None
        )
        print("Change column processed")
    
    # Print data sample
    print("\nData sample (first 3 rows):")
    print(data.head(3))
    
    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    )
    
    # Start the load job
    print(f"\nLoading data to BigQuery table {DATASET_ID}.{TABLE_ID}...")
    job = client.load_table_from_dataframe(data, table_id, job_config=job_config)
    
    # Wait for the job to complete
    job.result()
    
    # Get table info and confirm row count
    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows into {DATASET_ID}.{TABLE_ID}")
    
    # Clean up temp key file if created
    if os.path.exists("temp_key.json"):
        os.remove("temp_key.json")
        print("Temporary credentials file cleaned up")

except Exception as e:
    print(f"Error: {e}")
    # Clean up temp key file if created
    if os.path.exists("temp_key.json"):
        os.remove("temp_key.json")
    raise