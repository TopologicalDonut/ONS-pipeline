import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
from pathlib import Path
import zipfile
import time
import random
from tqdm import tqdm
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging

from src.logger import setup_logger

logger = setup_logger(__name__, logging.INFO)

BASE_URL = 'https://www.ons.gov.uk'
TARGET_URL = f'{BASE_URL}/economy/inflationandpriceindices/datasets/consumerpriceindicescpiandretailpricesindexrpiitemindicesandpricequotes'
FILE_TYPES = [".csv", ".xlsx", ".zip",]
SEARCH_TERMS = ["upload-itemindices", "/itemindices"]

DATA_DIR = Path(__file__).parent.parent / 'data'

class RateLimiter:
    def __init__(self, requests_per_period: int, period_seconds: int):

        self.delay = period_seconds / requests_per_period
        self.last_request_time = 0

    def wait(self):

        now = time.time()
        time_since_last_request = now - self.last_request_time

        if time_since_last_request < self.delay:
            time.sleep(self.delay - time_since_last_request)

        self.last_request_time = time.time()

class WebScraper:
    def __init__(self, BASE_URL: str, requests_per_period: int, period_seconds: int, logger: logging.Logger):

        self.BASE_URL = BASE_URL
        self.session = requests.Session()
        self.rate_limiter = RateLimiter(requests_per_period, period_seconds)
        self.logger = logger   

    def _make_request(self, url: str, max_retries: int = 5) -> requests.Response | None:
        backoff_time = 1
        for attempt in range(max_retries):
            self.rate_limiter.wait()
            try:
                response = self.session.get(url)
                response.raise_for_status()
                return response
            except RequestException as e:
                if response.status_code == 429:
                    self.logger.warning(f"Rate limit exceeded. Backing off for {backoff_time} seconds before retrying.")
                    time.sleep(backoff_time)
                    backoff_time *= 2  # exponential backoff
                else:
                    self.logger.warning(f"Error accessing {url}: {str(e)}. Attempt {attempt + 1} of {max_retries}")
                    if attempt == max_retries - 1:
                        self.logger.error(f"Failed to access {url} after {max_retries} attempts.")
                        return None
        return None

    def get_web_data(self, url: str) -> str | None:

        response = self._make_request(url)

        return response.text if response else None

    def get_data_links(self, html: str, FILE_TYPES: list[str], search_term: list[str]) -> list[str]:
        """
        Get data links from the HTML content.

        Notes
        -----
        The search terms are passed through the any() function.
        """
        soup = BeautifulSoup(html, 'html.parser')

        links = [
            link.get('href') for link in soup.find_all('a')
            if link.get('href') and any(term in link.get('href') for term in search_term)
            and link.get('href').endswith(tuple(FILE_TYPES))
        ]
        
        self.logger.info(f"Found {len(links)} matching data links")
        self.logger.debug(f"Data links: {links}")

        return links
    
    def download_file(self, url: str, path: Path) -> bool:

        response = self._make_request(url)

        if response:
            path.write_bytes(response.content)
            return True
        
        return False
        
    def process_files(self, data_links: list[str], download_dir: Path) -> None:

        self.logger.info(f"Starting to process {len(data_links)} files")

        download_dir.mkdir(parents = True, exist_ok = True)
        extract_dir = download_dir / 'extracted_files'
        extract_dir.mkdir(exist_ok = bool)
        
        processed_log = extract_dir / 'processed_zips.txt'
        processed = set(processed_log.read_text().splitlines()) if processed_log.exists() else set()

        stats = {
            'zips_processed': 0,
            'zips_skipped': 0,
            'files_downloaded': 0,
            'files_skipped': 0
        }

        for link in tqdm(data_links, desc = 'Processing files'):
            filename = Path(link).name
            file_path = download_dir / filename

            if filename.endswith('.zip'):
                if filename in processed:
                    self.logger.debug(f"Skipping already processed zip: {filename}")
                    stats['zips_skipped'] += 1
                    continue

                if self.download_file(self.BASE_URL + link, file_path):
                    self.logger.info(f"Extracting zip file: {filename}")

                    with zipfile.ZipFile(file_path) as zf:
                        zf.extractall(extract_dir)
                    
                    processed.add(filename)
                    processed_log.write_text('\n'.join(sorted(processed)))

                    file_path.unlink()
                    stats['zips_processed'] += 1
            else:
                if file_path.exists():
                    self.logger.debug(f"Skipping existing file: {filename}")
                    stats['files_skipped'] += 1
                    continue
                
                if self.download_file(self.BASE_URL + link, file_path):
                    stats['files_downloaded'] += 1
                
        self.logger.info(
            f"Processing complete -- "
            f"Processed: {stats['zips_processed']} zips, {stats['files_downloaded']} files. "
            f"Skipped: {stats['zips_skipped']} zips, {stats['files_skipped']} files."
        )
        
        return None

def main():

    scraper = WebScraper(BASE_URL, requests_per_period = 5, period_seconds = 10, logger = logger)
    
    if html := scraper.get_web_data(TARGET_URL):
        data_links = scraper.get_data_links(html, FILE_TYPES, SEARCH_TERMS)
        scraper.process_files(data_links, DATA_DIR)

if __name__ == "__main__":
    main()