from requests import get
from bs4 import BeautifulSoup as bs

def get_soup(url):
    response = get(url)
    soup = bs(response.text)
    return soup

url = "https://www.investing.com"
soup = get_soup(url)

import re
regex = re.compile('.*datatable.*')
tbody = soup.find("tbody", class_=regex)

rows = tbody.find_all("tr")

def get_stock_link(row):
    link_element = row.find("a")
    link = f"{url}{link_element.get('href')}-historical-data"
    stock_name = link_element.text

    return {
        "stock_name": stock_name,
        "link": link
    }

historical_data_links = [get_stock_link(row) for row in tbody.find_all("tr")]
print(historical_data_links)