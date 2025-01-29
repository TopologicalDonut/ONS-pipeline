import logging
import time
from pathlib import Path
import zipfile
from dataclasses import dataclass, field
from typing import TypedDict
import requests
from requests.exceptions import RequestException, HTTPError
from bs4 import BeautifulSoup
from tqdm import tqdm
from functools import cache
import re
from io import BytesIO

from src.const import PATH_CONFIG

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class WebConfig:
    base_url: str = 'https://www.ons.gov.uk'
    target_url: str = (
        'https://www.ons.gov.uk/economy/inflationandpriceindices/'
        'datasets/consumerpriceindicescpiandretailpricesindexrpiitemindices'
        'andpricequotes'
    )
    file_types: set[str] = field(default_factory=lambda: {'.csv', '.xlsx', '.zip'})
    search_terms: set[str] = field(default_factory=lambda: {'upload-itemindices', '/itemindices'})
    previous_edition_terms: set[str] = field(default_factory=lambda: {'Previous versions'})
    requests_per_period: int = 5
    period_seconds: int = 10

ONS_WEB_CONFIG = WebConfig()

class ArchiveContents(TypedDict):
    quarterly: set[str]
    monthly: set[str]
    other: set[str]

class RequestManager:
    """
    Handles HTTP requests with backoff and dynamic rate limiting.
    """
    def __init__(self, requests_per_period: int, period_seconds: int):

        self.session = requests.Session()
        self.delay = period_seconds / requests_per_period
        self.last_request_time = 0.0

    def make_request(self, url: str) -> requests.Response:
        """
        Make an HTTP GET request, adjusting delay between requests depending on 429s.
        """

        while True:
            self._wait()

            try:
                response = self.session.get(url)
                response.raise_for_status()
                return response
            
            except HTTPError as e:
                if e.response.status_code == 429:
                    retry_after = e.response.headers.get('Retry-After')

                    if retry_after is None:
                        wait_time = 10
                    
                    else:
                        wait_time = int(retry_after)
                    
                    logger.warning(f"Rate limit exceeded. Backing off for {wait_time}s before retrying.")

                    time.sleep(wait_time)
                    self._reduce_rate()

                else:
                    raise
            
            except RequestException as e:
                raise
    
    def _wait(self) -> None:

        now = time.time()
        time_since_last_request = now - self.last_request_time

        if time_since_last_request < self.delay:
            time.sleep(self.delay - time_since_last_request)

        self.last_request_time = time.time()

    def _reduce_rate(self) -> None:

        self.delay *= 2

        logger.warning(f"Increasing delay between requests to {self.delay}")

    
def get_all_links(
    base_url: str, 
    target_url:str, 
    search_terms: set[str], 
    prev_edition_terms: set[str], 
    file_types: set[str],
    request_manager: RequestManager
) -> list[str]:
    """Gets all data file links from main page and previous pages, avoiding duplicates"""

    logger.info("Starting to collect data links")

    all_data_links = []
    stats = {
        'main_page_links': set(),
        'previous_version_links': set()
    }

    main_html = _get_page_content(target_url, request_manager)
    soup = BeautifulSoup(main_html, 'html.parser')
    main_page_links = _process_page_for_links(soup, search_terms, file_types)

    all_data_links.extend(main_page_links)
    stats['main_page_links'].update(main_page_links)

    logger.info(f"Found {len(main_page_links)} links on main page")

    prev_page_urls = [
        link.get('href') for link in soup.find_all('a')
        if any(term.lower() in link.text.lower() for term in prev_edition_terms)
        and any(term in link.get('href') for term in search_terms)
    ]

    logger.info(f"Found {len(prev_page_urls)} previous version pages")

    for url in tqdm(prev_page_urls, desc = 'Processing previous version pages'):
        logger.debug(f"Processing previous version page: {url}")

        prev_html = _get_page_content(base_url + url, request_manager)
        soup = BeautifulSoup(prev_html, 'html.parser')
        prev_links = _process_page_for_links(soup, search_terms, file_types)

        logger.debug(f"Found {len(prev_links)} links on previous version page")

        all_data_links.extend(prev_links)
        stats['previous_version_links'].update(prev_links)

    logger.info(f"Total unique data links found: {len(all_data_links)}")

    return all_data_links, stats

def _get_page_content(url: str, request_manager: RequestManager) -> str:

    response = request_manager.make_request(url)
    return response.text

def _process_page_for_links(soup: BeautifulSoup, search_terms: set[str], file_types: set[str]) -> list[str]:

    links = []
    normalized_names_seen = set()

    for href in (link.get('href') for link in soup.find_all('a')):
        if href and any(term in href for term in search_terms) and href.endswith(tuple(file_types)):

            normalized_name = _normalize_filename(Path(href).name)               
            if normalized_name not in normalized_names_seen:
                links.append(href)
                normalized_names_seen.add(normalized_name)

    return links

@cache
def _normalize_filename(filename: str) -> str:
    """Convert filename to standard format:
    - Quarterly returns: YYYYqN (e.g. 2023q1)
    - Monthly returns: YYYYMM (e.g. 202301)
    - Yearly returns: YYYY (e.g. 2023)
    - Others return: cleaned filename
    """
    
    basename = Path(filename).stem
    clean = ''.join(c.lower() for c in basename if c.isalnum())

    # Quarterly pattern
    if match := re.search(r'(\d{4})q([1-4])', clean):
        year, quarter = match.groups()
        if 1900 <= int(year) <= 2100:
            return f"{year}q{quarter}"

    # Monthly pattern
    if match := re.search(r'(\d{4})(0[1-9]|1[0-2])', clean):
        year, month = match.groups()
        if 1900 <= int(year) <= 2100:
            return f"{year}{month}"

    # Yearly archive pattern
    if match := re.search(r'itemindices(\d{4})', clean):
        year = match.group(1)
        if 1900 <= int(year) <= 2100:
            return year
        
    return clean

class FileHandler:
    def __init__(self, request_manager: RequestManager, download_dir: Path, extract_dir: Path):
        self.request_manager = request_manager
        self.download_dir = download_dir
        self.extract_dir = extract_dir
        
        self.existing_downloads = {f.name for f in download_dir.iterdir() if f.is_file()}
        self.stats = {
            'yearly_archive_contents': {}, # year -> contents
            'quarterly_archive_contents': {}, 
            'monthly_archive_contents': {},
            'unknown_archive_contents': {},
            'individual_files': set(),
            'skipped_files': set()
        }

        # Load previously processed files if any
        processed_log = extract_dir / 'processed_zips.txt'
        if processed_log.exists():
            self.existing_downloads.update(processed_log.read_text().splitlines())

    def process_file(self, url: str) -> None:
        """Process any file (zip or non-zip)"""

        filename = Path(url).name
        normalized_name = _normalize_filename(filename)
        
        # Skip if already processed
        if filename in self.existing_downloads:
            self.stats['skipped_files'].add(filename)
            return

        # Skip if in yearly archive
        if self._in_yearly_archive(filename):
            self.stats['skipped_files'].add(filename)
            return

        if filename.endswith('.zip'):
            self._process_zip(url, filename, normalized_name)
        else:
            self._process_regular_file(url, filename)

    def _process_zip(self, url: str, filename: str, normalized_name: str) -> None:

        try:
            response = self.request_manager.make_request(url)

            if len(normalized_name) == 4 and normalized_name.isdigit():
                category = 'yearly'
            elif 'q' in normalized_name and normalized_name[:4].isdigit():
                category = 'quarterly'
            elif len(normalized_name) == 6 and normalized_name.isdigit():
                category = 'monthly'
            else:
                category = 'unknown'
            
            with zipfile.ZipFile(BytesIO(response.content)) as zf:
                # Check contents
                contents = self._check_zip_contents(zf)
                
                self.stats[f'{category}_archive_contents'][normalized_name] = contents
                if category == 'unknown':
                    logger.warning(f"Uncategorized zip: {filename}")
                
                self._extract_zip(zf)
                
            self.existing_downloads.add(filename)
            self._update_processed_log()
            
        except Exception as e:
            logger.error(f"Error processing zip {filename}: {e}")

    def _process_regular_file(self, url: str, filename: str) -> None:

        target_path = self.download_dir / filename
        try:
            response = self.request_manager.make_request(url)
            target_path.write_bytes(response.content)
            self.existing_downloads.add(filename)
            self.stats['individual_files'].add(filename)
        except Exception as e:
            logger.warning(f"Failed to download {filename}: {e}")

    def _check_zip_contents(self, zf: zipfile.ZipFile) -> ArchiveContents:

        contents: ArchiveContents = {'quarterly': set(), 'monthly': set(), 'other': set()}
        
        for name in zf.namelist():
            if name.endswith('/'):
                continue
            inner_name = Path(name).name
            normalized_name = _normalize_filename(inner_name)
            if 'q' in normalized_name:
                contents['quarterly'].add(normalized_name)
            elif len(normalized_name) == 6 and normalized_name.isdigit():
                contents['monthly'].add(normalized_name)
            else:
                contents['other'].add(normalized_name)
        
        return contents

    def _extract_zip(self, zf: zipfile.ZipFile) -> None:

        for member in zf.namelist():
            filename = Path(member).name
            if not filename or filename in self.existing_downloads:
                continue
                
            target_path = self.extract_dir / filename
            if not target_path.exists():
                with zf.open(member) as source, open(target_path, 'wb') as target:
                    target.write(source.read())
                self.existing_downloads.add(filename)

    def _in_yearly_archive(self, filename: str) -> bool:

        normalized_name = _normalize_filename(filename)
        
        year = None
        if 'q' in normalized_name:  # e.g. 2023q1
            year = normalized_name[:4]
        elif len(normalized_name) == 6 and normalized_name.isdigit():  # e.g. 202301
            year = normalized_name[:4]
            
        if year and year in self.stats['yearly_archive_contents']:
            contents = self.stats['yearly_archive_contents'][year]
            if 'q' in normalized_name:
                return normalized_name in contents['quarterly']
            return normalized_name in contents['monthly']
        return False

    def _update_processed_log(self) -> None:

        log_path = self.extract_dir / 'processed_zips.txt'
        log_path.write_text('\n'.join(sorted(f for f in self.existing_downloads if f.endswith('.zip'))))

    def print_summary(self) -> None:

        def count_files(archive_dict):
            return sum(
                len(contents['quarterly']) + len(contents['monthly']) + len(contents['other'])
                for contents in archive_dict.values()
            )

        logger.info("-" * 50)
        logger.info("Processing Summary")
        logger.info("-" * 50)
        logger.info("Archive files processed:")
        logger.info(f"  Yearly ZIP archives: {len(self.stats['yearly_archive_contents'])} "
                f"(containing {count_files(self.stats['yearly_archive_contents'])} files)")
        logger.info(f"  Quarterly ZIP archives: {len(self.stats['quarterly_archive_contents'])} "
                f"(containing {count_files(self.stats['quarterly_archive_contents'])} files)")
        logger.info(f"  Monthly ZIP archives: {len(self.stats['monthly_archive_contents'])} "
                f"(containing {count_files(self.stats['monthly_archive_contents'])} files)")
        logger.info(f"  Uncategorized ZIP archives: {len(self.stats['unknown_archive_contents'])} "
                f"(containing {count_files(self.stats['unknown_archive_contents'])} files)")
        logger.info(f"  Individual non-archive files: {len(self.stats['individual_files'])}")
        logger.info(f"  Skipped files: {len(self.stats['skipped_files'])}")
        logger.info(f"\nTotal unique files processed: {len(self.existing_downloads)}")
        logger.info("-" * 50)

        if self.stats['unknown_archive_contents']:
            logger.info("\nUncategorized zip files:")
            for filename in sorted(self.stats['unknown_archive_contents'].keys()):
                logger.info(f"  - {filename}")

def main():

    request_manager = RequestManager(ONS_WEB_CONFIG.requests_per_period, ONS_WEB_CONFIG.period_seconds)

    download_dir = PATH_CONFIG.DATA_DIR
    extract_dir = download_dir / 'extracted_files'

    for dir in (download_dir, extract_dir):
        dir.mkdir(exist_ok=True)

    file_handler = FileHandler(request_manager, download_dir, extract_dir)

    logger.info("Collecting all data links")
    data_links, link_stats = get_all_links(
        ONS_WEB_CONFIG.base_url, 
        ONS_WEB_CONFIG.target_url, 
        ONS_WEB_CONFIG.search_terms,
        ONS_WEB_CONFIG.previous_edition_terms, 
        ONS_WEB_CONFIG.file_types,
        request_manager
    )

    logger.info(f"Processing main page links")
    main_links = [link for link in data_links if link in link_stats['main_page_links']]
    for link in tqdm(main_links, desc = 'Processing main page links'):
        file_handler.process_file(ONS_WEB_CONFIG.base_url + link)
    

    logger.info("Processing previous versions links")
    prev_links = [link for link in data_links if link in link_stats['previous_version_links']]
    for link in tqdm(prev_links, desc='Processing previous versions links'):
        file_handler.process_file(ONS_WEB_CONFIG.base_url + link)

    file_handler.print_summary()

if __name__ == "__main__":
    main()
