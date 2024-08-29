import concurrent.futures
import time
import requests
import os
import logging
import pandas as pd
import random
import sys
from typing import List, Dict
from urllib.parse import urljoin
from bs4 import BeautifulSoup

class GlamiraCrawler:
    def __init__(self, full_urls_csv: str, checklist_csv: str, log_file: str = 'crawler.log', max_workers: int = 5):
        # Initialize instance variables
        self.full_urls_csv = full_urls_csv
        self.checklist_csv = checklist_csv
        self.log_file = log_file
        self.max_workers = max_workers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148'
        })

        # Initialize logging
        self.setup_logging()

        # Initialize DataFrame for tracking products and checklist
        self.checklist_df = self.create_checklist()
        self.unique_products = pd.DataFrame(columns=['url'])

    def setup_logging(self):
        # Remove the old log file if it exists
        if os.path.exists(self.log_file):
            os.remove(self.log_file)
        
        # Set up logging configuration
        logging.basicConfig(filename=self.log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger()
        self.logger.info("Logging setup complete.")

    def create_checklist(self):
        try:
            # Remove old checklist if it exists
            if os.path.exists(self.checklist_csv):
                os.remove(self.checklist_csv)
            
            # Create a new checklist from the full URLs CSV
            urls_df = pd.read_csv(self.full_urls_csv)
            urls_df['status'] = 'NOT COMPLETE'
            urls_df.to_csv(self.checklist_csv, index=False)
            self.logger.info("Checklist created successfully.")
            return pd.read_csv(self.checklist_csv)  # Return the newly created checklist
        except Exception as e:
            self.logger.error(f"Error creating or loading checklist: {e}")
            sys.exit(1)  # Exit the program if there's an error

    def make_request(self, url: str) -> requests.Response:
        try:
            # Random sleep to avoid hitting the server too frequently
            time.sleep(random.uniform(1, 3))
            response = self.session.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors
            self.logger.info(f"Successfully retrieved page content from {url}")
            return response
        except requests.RequestException as e:
            self.logger.error(f"Error making request to {url}: {str(e)}")
            return None

    def get_totalpage_totalproducts(self, url):
        # Retrieve page content and parse the total number of pages and products
        response = self.make_request(url)
        if not response:
            return 0, 0
        
        soup = BeautifulSoup(response.content, 'html.parser')
        pagination_element = soup.select_one('ol.products.list.items.product-items li')

        if pagination_element:
            total_page = int(pagination_element.get('data-lastpage', 0))
            total_product = int(pagination_element.get('data-total-items', 0))
        else:
            total_page, total_product = 0, 0

        return total_page, total_product

    def fetch_product_data(self, url: str) -> List[Dict[str, str]]:
        details_list = []
        response = self.make_request(url)
        if response:
            soup = BeautifulSoup(response.content, 'html.parser')
            product_items = soup.find("ol", class_="products list items product-items")
            
            if product_items:
                for item in product_items.find_all('li'):
                    details = {}
                    link_tag = item.find('a', class_='product-link')
                    product_url = urljoin(url, link_tag['href']) if link_tag else 'N/A'
                    
                    # Skip already processed product URLs
                    if product_url in self.unique_products['url'].values:
                        continue
                    
                    # Add new product URL to the list
                    new_row = pd.DataFrame({'url': [product_url]})
                    self.unique_products = pd.concat([self.unique_products, new_row], ignore_index=True)
                    
                    # Extract product details
                    details['name'] = item.find('h2', class_='product-item-details product-name').get_text(strip=True) if item.find('h2', class_='product-item-details product-name') else 'N/A'
                    details['url'] = product_url
                    details['price'] = item.find('span', class_='price').get_text(strip=True) if item.find('span', class_='price') else 'N/A'
                    details['description'] = item.find('span', class_='short-description').get_text(strip=True) if item.find('span', class_='short-description') else 'N/A'
                    image_tags = item.find_all('img', class_='product-image-photo')
                    image_urls = [urljoin(url, img['src']) for img in image_tags if img.get('src')]
                    
                    details['image_urls'] = ','.join(image_urls)
                    
                    # Append details to list if they are valid
                    if all(value != 'N/A' for value in details.values()):
                        details_list.append(details)

        return details_list

    def save_details_to_csv(self, details_list: List[Dict[str, str]], category_name: str):
        try:
            # Define the file path for saving product details
            file_path = os.path.join(os.getcwd(), 'data', f'{category_name}.csv')
            df = pd.DataFrame(details_list)
            df.to_csv(file_path, mode='a', header=not os.path.isfile(file_path), index=False)
            self.logger.info(f"Successfully saved {len(details_list)} product details to {file_path}")
        except Exception as e:
            self.logger.error(f"Error saving to CSV: {str(e)}")

    def crawl_all_products(self, url: str, category_name: str, index: int):
        # Update status to 'IN PROGRESS' before starting to scrape the URL
        self.update_status(index, 'IN PROGRESS')
        self.logger.info(f"START SCRAPING URL: {url}")
        total_page, total_product = self.get_totalpage_totalproducts(url)
        print(total_page)
        print(total_product)
        
        if total_page and total_product:
            # Create URLs for each page
            urls = [url + f'?p={i}' if i > 1 else url for i in range(1, total_page + 1)]
            all_data = []
            
            # Use ThreadPoolExecutor to process multiple pages in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self.fetch_product_data, page_url) for page_url in urls]
                for future in concurrent.futures.as_completed(futures):
                    data = future.result()
                    all_data.extend(data)
            
            # Save all product details to CSV
            self.save_details_to_csv(all_data, category_name)
            self.logger.info(f"Scraping Complete {total_product} products from URL: {url}")
            return index
        return index

    def update_status(self, index: int, status: str):
        try:
            # Update the status of a URL in the checklist
            self.checklist_df.at[index, 'status'] = status
            self.checklist_df.to_csv(self.checklist_csv, index=False)
            self.logger.info(f"Updated status for index {index} to {status}")
        except Exception as e:
            self.logger.error(f"Error updating status for index {index}: {str(e)}")

    def run(self):
        self.logger.info("Starting Crawl Session")
        # Use ThreadPoolExecutor to run the crawling tasks in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for index, row in self.checklist_df.iterrows():
                url = row['url']
                category_name = url.split('/')[-2]  # Extract category name from URL
                status = self.checklist_df.loc[self.checklist_df['url'] == url, 'status'].values[0]
                if status == 'NOT COMPLETE':
                    futures.append(executor.submit(self.crawl_all_products, url, category_name, index))
                    time.sleep(2)  # Ensure some delay between tasks
            for future in concurrent.futures.as_completed(futures):
                index = future.result()
                self.update_status(index, 'COMPLETE')

if __name__ == "__main__":
    # Define file paths for the full URLs CSV and checklist CSV
    full_urls_csv = 'urls_test.csv'
    checklist_csv = 'checklist.csv'
    
    # Create an instance of GlamiraCrawler and run the crawler
    crawler = GlamiraCrawler(full_urls_csv, checklist_csv)
    crawler.run()
