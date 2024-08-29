import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

def fetch_sitemap(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def parse_sitemap(xml_content):
    soup = BeautifulSoup(xml_content, 'xml')
    urls = [loc.text for loc in soup.find_all('loc')]
    return urls

def save_urls_to_csv(urls, filename):
    df = pd.DataFrame(urls, columns=['url'])
    # Ensure the directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    df.to_csv(filename, index=False, header=True)

def main():
    sitemap_url = 'https://www.glamira.com/media/sitemap/glus/category_provider.xml'  # Replace with the actual sitemap URL
    sitemap_content = fetch_sitemap(sitemap_url)
    product_urls = parse_sitemap(sitemap_content)
    
    path = os.path.join(os.getcwd(), 'data', 'entire_urls.csv')
    save_urls_to_csv(product_urls, path)
    print(f"Product URLs saved to {path}")

if __name__ == "__main__":
    main()
