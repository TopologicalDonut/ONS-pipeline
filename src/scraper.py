import requests
from bs4 import BeautifulSoup
from pathlib import Path
import zipfile
import time
from tqdm import tqdm
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging

from logger import setup_logger

'''
We want to scrape the ONS page for CPI data. Use requests to open the page, then use BeautifulSoup to parse the HTML.
'''

@dataclass
class RateLimiter:
    requests_per_second: float
    last_request: datetime = datetime.now()

    def wait(self):

        now = datetime.now()
        elapsed = now - self.last_request
        required_gap = timedelta(seconds = 1 // self.requests_per_second)

        if elapsed < required_gap:
            time.sleep((required_gap - elapsed.total_seconds()))
        self.last_request = datetime.now()

class WebScraper:
    def __init__(self, BASE_URL: str, rate_limit: float, log_file: str | None = None):

        self.BASE_URL = BASE_URL
        self.session = requests.Session()
        self.rate_limiter = RateLimiter(rate_limit)
        self.logger = setup_logger(__name__)
        self.logger.info(f"Initializing WebScraper with base URL: {BASE_URL}, rate limit: {rate_limit} req/s")   

    def get_web_data(self, url: str) -> str | None:

        self.logger.debug(f"Fetching data from {url}")

        try:
            response = self.session.get(url)
            response.raise_for_status()
            self.logger.debug(f"Successfully fetched data from {url}")

            return response.text
        
        except requests.RequestException as e:
            self.logger.error(f"Error fetching {url}: {str(e)}")

            return None

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

        self.logger.debug(f"Downloading file from {url} to {path}")
        self.rate_limiter.wait()

        try:
            response = self.session.get(url)
            response.raise_for_status()
            path.write_bytes(response.content)

            self.logger.debug(f"Successfully downloaded file to {path}")

            return True
        
        except requests.RequestException as e:
            self.logger.error(f"Error downloading {url}: {str(e)}")

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

    BASE_URL = 'https://www.ons.gov.uk'
    TARGET_URL = f'{BASE_URL}/economy/inflationandpriceindices/datasets/consumerpriceindicescpiandretailpricesindexrpiitemindicesandpricequotes'
    FILE_TYPES = [".csv", ".xlsx", ".zip",]
    SEARCH_TERMS = ["upload-itemindices", "/itemindices"]

    scraper = WebScraper(BASE_URL, rate_limit = 1.5)
    
    if html := scraper.get_web_data(TARGET_URL):
        data_links = scraper.get_data_links(html, FILE_TYPES, SEARCH_TERMS)
        scraper.process_files(data_links, Path('data'))

if __name__ == "__main__":
    main()