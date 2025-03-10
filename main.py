from requests import get
from bs4 import BeautifulSoup as bs
import re
import csv
import time

def get_soup(url, max_retries=3):
    """Get BeautifulSoup object with retry mechanism and proper headers"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    
    for i in range(max_retries):
        try:
            response = get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return bs(response.text, features="lxml")
        except Exception as e:
            if i == max_retries - 1:  # Last attempt
                print(f"Failed to fetch {url}: {e}")
                raise
            print(f"Attempt {i+1} failed. Retrying in 3 seconds...")
            time.sleep(3)

def get_stock_link(row, base_url):
    """Fix URL construction to handle relative and absolute URLs properly"""
    link_element = row.find("a")
    href = link_element.get('href', '')
    
    # Check if the href is a full URL or a relative path
    if href.startswith('http'):
        # It's already a full URL
        link = f"{href}-historical-data"
    elif href.startswith('/'):
        # It's a relative URL starting with /
        link = f"{base_url}{href}-historical-data"
    else:
        # It's a relative URL without starting /
        link = f"{base_url}/{href}-historical-data"
    
    stock_name = link_element.text.strip()
    return {
        "stock_name": stock_name,
        "link": link
    }

def main():
    base_url = "https://www.investing.com"
    print(f"Fetching main page from {base_url}...")
    soup = get_soup(base_url)

    regex = re.compile('.*datatable.*')
    tbody = soup.find("tbody", class_=regex)
    
    if not tbody:
        print("Could not find data table on main page.")
        return
    
    rows = tbody.find_all("tr")
    print(f"Found {len(rows)} rows in the table.")
    
    historical_data_links = [get_stock_link(row, base_url) for row in rows]
    print(f"Identified {len(historical_data_links)} stock links.")

    total_data = []
    headers = []
    
    for i, stock in enumerate(historical_data_links):
        link = stock['link']
        stock_name = stock['stock_name']
        print(f"Fetching data for {stock_name} ({i+1}/{len(historical_data_links)}): {link}")
        
        try:
            soup = get_soup(link)
            regex = re.compile('.*freeze-column.*')
            table = soup.find("table", class_=regex)
            
            if not table:
                print(f"No table found for {stock_name}. Skipping...")
                continue
                
            if not len(headers):
                thead = table.find("thead")
                if thead:
                    header_cols = thead.find_all("th")
                    headers = [ele.text.strip() for ele in header_cols]
                    print(f"Found headers: {headers}")
            
            tbody = table.find("tbody")
            if not tbody:
                print(f"No tbody found for {stock_name}. Skipping...")
                continue
                
            rows = tbody.find_all("tr")
            stock_data_count = 0
            
            for row in rows:
                cols = row.find_all("td")
                cols = [ele.text.strip() for ele in cols]
                
                if len(cols) == len(headers):
                    data = dict(zip(headers, cols))
                    data['stock_name'] = stock_name
                    total_data.append(data)
                    stock_data_count += 1
            
            print(f"Fetched {stock_data_count} data points for {stock_name}")
            
            # Add a small delay to avoid hitting rate limits
            time.sleep(2)
            
        except Exception as e:
            print(f"Error processing {stock_name}: {e}")
            continue

    if total_data:
        write_to_csv(total_data, "stock_data.csv")
        print(f"Successfully wrote {len(total_data)} records to stock_data.csv")
    else:
        print("No data collected. CSV file not written.")

def write_to_csv(data, filename):
    if not data:
        print("No data to write.")
        return
        
    keys = data[0].keys()
    with open(filename, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)
        print(f"Data written to {filename}")

if __name__ == "__main__":
    main()  