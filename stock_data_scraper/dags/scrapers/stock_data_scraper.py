import requests
from bs4 import BeautifulSoup as bs
import re
import csv
import time
import os
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

class StockDataScraper:
    def __init__(self, project_id, dataset_id, table_id):
        self.base_url = "https://www.investing.com"
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.headers = []
        self.total_data = []
        self.csv_path = "stock_data.csv"
        
        # Set up headers for requests
        self.request_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        
        # Initialize BigQuery client
        self._setup_bigquery_auth()
        self.client = bigquery.Client(project=self.project_id)
        
    def _setup_bigquery_auth(self):
        """Set up Google Cloud authentication"""
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
    
    def get_soup(self, url, max_retries=3):
        """Get BeautifulSoup object with retry mechanism"""
        for i in range(max_retries):
            try:
                response = requests.get(url, headers=self.request_headers, timeout=10)
                response.raise_for_status()
                return bs(response.text, features="lxml")
            except Exception as e:
                if i == max_retries - 1:  # Last attempt
                    print(f"Failed to fetch {url}: {e}")
                    raise
                print(f"Attempt {i+1} failed. Retrying in 3 seconds...")
                time.sleep(3)

    def get_stock_link(self, row):
        """Construct stock links with improved URL handling"""
        link_element = row.find("a")
        href = link_element.get('href', '')
        
        # Check if the href is a full URL or a relative path
        if href.startswith('http'):
            # It's already a full URL
            link = f"{href}-historical-data"
        elif href.startswith('/'):
            # It's a relative URL starting with /
            link = f"{self.base_url}{href}-historical-data"
        else:
            # It's a relative URL without starting /
            link = f"{self.base_url}/{href}-historical-data"
        
        stock_name = link_element.text.strip()
        return {
            "stock_name": stock_name,
            "link": link
        }

    def fetch_data(self):
        """Scrape stock data from investing.com"""
        print(f"Fetching main page from {self.base_url}...")
        soup = self.get_soup(self.base_url)

        regex = re.compile('.*datatable.*')
        tbody = soup.find("tbody", class_=regex)
        
        if not tbody:
            print("Could not find data table on main page.")
            return False
        
        rows = tbody.find_all("tr")
        print(f"Found {len(rows)} rows in the table.")
        
        historical_data_links = [self.get_stock_link(row) for row in rows]
        print(f"Identified {len(historical_data_links)} stock links.")
        
        for i, stock in enumerate(historical_data_links):
            link = stock['link']
            stock_name = stock['stock_name']
            print(f"Fetching data for {stock_name} ({i+1}/{len(historical_data_links)}): {link}")
            
            try:
                soup = self.get_soup(link)
                regex = re.compile('.*freeze-column.*')
                table = soup.find("table", class_=regex)
                
                if not table:
                    print(f"No table found for {stock_name}. Skipping...")
                    continue
                    
                if not len(self.headers):
                    thead = table.find("thead")
                    if thead:
                        header_cols = thead.find_all("th")
                        self.headers = [ele.text.strip() for ele in header_cols]
                        print(f"Found headers: {self.headers}")
                
                tbody = table.find("tbody")
                if not tbody:
                    print(f"No tbody found for {stock_name}. Skipping...")
                    continue
                    
                rows = tbody.find_all("tr")
                stock_data_count = 0
                
                for row in rows:
                    cols = row.find_all("td")
                    cols = [ele.text.strip() for ele in cols]
                    
                    if len(cols) == len(self.headers):
                        data = dict(zip(self.headers, cols))
                        data['stock_name'] = stock_name
                        self.total_data.append(data)
                        stock_data_count += 1
                
                print(f"Fetched {stock_data_count} data points for {stock_name}")
                
                # Add a small delay to avoid hitting rate limits
                time.sleep(2)
                
            except Exception as e:
                print(f"Error processing {stock_name}: {e}")
                continue
        
        if not self.total_data:
            print("No data collected.")
            return False
            
        print(f"Successfully collected {len(self.total_data)} total records")
        return True

    def save_to_csv(self):
        """Save scraped data to CSV file"""
        if not self.total_data:
            print("No data to write to CSV.")
            return False
            
        keys = self.total_data[0].keys()
        with open(self.csv_path, 'w', newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.total_data)
            print(f"Data written to {self.csv_path}")
        return True

    def create_bigquery_dataset_table(self):
        """Create or get BigQuery dataset and table"""
        dataset_id = f"{self.project_id}.{self.dataset_id}"
        try:
            dataset = self.client.get_dataset(dataset_id)
            print(f"Dataset {self.dataset_id} already exists.")
        except NotFound:
            print(f"Dataset {self.dataset_id} not found. Creating now...")
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            dataset = self.client.create_dataset(dataset, timeout=30)
            print(f"Dataset {self.dataset_id} created.")

        schema = [
            bigquery.SchemaField("stock_name", "STRING", description="Name of the stock"),
            bigquery.SchemaField("Date", "DATE", description="Date of the stock data"),
            bigquery.SchemaField("Open", "FLOAT", description="Opening price"),
            bigquery.SchemaField("High", "FLOAT", description="Highest price"),
            bigquery.SchemaField("Low", "FLOAT", description="Lowest price"),
            bigquery.SchemaField("Price", "FLOAT", description="Closing price"),
            bigquery.SchemaField("Vol", "FLOAT", description="Trading volume"),
            bigquery.SchemaField("Change", "FLOAT", description="Percentage change")
        ]

        table_id = f"{dataset_id}.{self.table_id}"
        try:
            table = self.client.get_table(table_id)
            print(f"Table {self.table_id} already exists.")
        except NotFound:
            print(f"Table {self.table_id} not found. Creating now...")
            table = bigquery.Table(table_id, schema=schema)
            table = self.client.create_table(table)
            print(f"Table {self.table_id} created.")
        
        return table_id

    def convert_to_float(self, x):
        """Convert string values to float with special handling for K, M, B suffixes"""
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

    def process_data(self):
        """Process data for BigQuery upload"""
        # Check if we have data in memory, otherwise load from CSV
        if not self.total_data and os.path.exists(self.csv_path):
            print(f"Loading data from {self.csv_path}...")
            self.total_data = pd.read_csv(self.csv_path).to_dict('records')
            
        if not self.total_data:
            print("No data to process.")
            return False
            
        data = pd.DataFrame(self.total_data)
        
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
            data['Vol'] = data['Vol'].astype(str).apply(self.convert_to_float)
            print(f"Processed volume column '{vol_column}' and renamed to 'Vol'")
        
        # Process Change column
        change_column = next((c for c in data.columns if c in ['Change %', 'Change', '% Change']), None)
        if change_column:
            data.rename(columns={change_column: 'Change'}, inplace=True)
            data['Change'] = data['Change'].astype(str).str.rstrip('%').apply(
                lambda x: float(x) if x and x != 'nan' else None
            )
            print("Change column processed")
        
        self.processed_data = data
        print(f"Processed {len(data)} rows of data")
        print("\nData sample (first 3 rows):")
        print(data.head(3))
        return True

    def load_to_bigquery(self):
        """Load processed data to BigQuery"""
        if not hasattr(self, 'processed_data') or self.processed_data.empty:
            print("No processed data available to load.")
            return False
            
        # Make sure dataset and table exist
        table_id = self.create_bigquery_dataset_table()
        
        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        )
        
        print(f"\nLoading data to BigQuery table {self.dataset_id}.{self.table_id}...")
        job = self.client.load_table_from_dataframe(
            self.processed_data, table_id, job_config=job_config
        )
        
        # Wait for the job to complete
        job.result()
        
        # Get table info and confirm row count
        table = self.client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows into {self.dataset_id}.{self.table_id}")
        
        # Clean up temp key file if created
        if os.path.exists("temp_key.json"):
            os.remove("temp_key.json")
            print("Temporary credentials file cleaned up")
            
        return True

    def run_pipeline(self):
        """Run the full ETL pipeline"""
        print("Starting stock data ETL pipeline...")
        
        # Extract
        if not self.fetch_data():
            print("Data extraction failed. Pipeline stopped.")
            return False
            
        # Save to CSV (optional intermediate step)
        self.save_to_csv()
        
        # Transform
        if not self.process_data():
            print("Data processing failed. Pipeline stopped.")
            return False
            
        # Load
        if not self.load_to_bigquery():
            print("Data loading failed. Pipeline stopped.")
            return False
            
        print("ETL pipeline completed successfully!")
        return True


# Stand-alone execution
if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables or use defaults
    project_id = os.getenv("PROJECT_ID", "marine-lodge-453310-g5")
    dataset_id = os.getenv("DATASET_ID", "StockMktData")
    table_id = os.getenv("TABLE_ID", "StockData")
    
    # Create and run the scraper
    scraper = StockDataScraper(project_id, dataset_id, table_id)
    scraper.run_pipeline()