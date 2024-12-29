import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
from pathlib import Path
import zipfile
import time
from tqdm import tqdm
import logging
from dataclasses import dataclass, field

from src.const import PATH_CONFIG

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class WebConfig:
    base_url: str = 'https://www.ons.gov.uk'
    target_url: str = f'{base_url}/economy/inflationandpriceindices/datasets/consumerpriceindicescpiandretailpricesindexrpiitemindicesandpricequotes'
    file_types: list[str] = field(default_factory=lambda: ['.csv', '.xlsx', '.zip'])
    search_terms: list[str] = field(default_factory=lambda: ['upload-itemindices', '/itemindices'])
    previous_edition_terms: list[str] = field(default_factory=lambda: ['Previous versions'])
    requests_per_period: int = 5
    period_seconds: int = 10

ONS_WEB_CONFIG = WebConfig()

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
    def __init__(self, config: WebConfig):
        self.config = config
        self.session = requests.Session()
        self.rate_limiter = RateLimiter(config.requests_per_period, config.period_seconds)
        self.stats = {
            'main_page_links': set(),           
            'previous_version_links': set(), 
            'yearly_archive_contents': {},  # year -> set of files
            'quarterly_zip_contents': {},   # quarter -> set of files
            'individual_files': set(),
            'skipped_files': set()
        }

    def _make_request(self, url: str, max_retries: int = 5) -> requests.Response | None:
        """Make an HTTP request with rate limiting and retries."""
        backoff_time = 1

        self.rate_limiter.wait()

        for attempt in range(max_retries):
            try:
                response = self.session.get(url)
                response.raise_for_status()
                return response
            except RequestException as e:
                if response.status_code == 429:
                    logger.warning(f"Rate limit exceeded. Backing off for {backoff_time} seconds before retrying.")
                    time.sleep(backoff_time)
                    backoff_time *= 2  # exponential backoff
                else:
                    logger.warning(f"Error accessing {url}: {str(e)}. Attempt {attempt + 1} of {max_retries}")
                    if attempt == max_retries - 1:
                        logger.error(f"Failed to access {url} after {max_retries} attempts.")
                        return None
        return None
    
    def _normalize_filename(self, filename: str) -> str:
        """
        Normalize filename and identify if yearly, quarterly or monthly data.
        
        Examples
        --------
        - itemindices2005.zip -> y_2005       (yearly)
        - upload-itemindices2016q3.csv -> q_2016q3  (quarterly)
        - upload-202004itemindices.csv -> m_202404  (monthly)
        - itemindices202303.csv -> m_202303         (monthly)
        """

        basename = Path(filename).stem
        clean = ''.join(c.lower() for c in basename if c.isalnum())
        
        # Check for quarterly pattern
        q_pos = clean.find('q')
        if q_pos != -1 and q_pos + 1 < len(clean) and clean[q_pos + 1] in '1234':
            if q_pos >= 4:  # Need at least 4 chars before for year
                year = clean[q_pos - 4:q_pos]
                if year.isdigit() and 1900 <= int(year) <= 2100:
                    return f"q_{year}q{clean[q_pos + 1]}"
                
                # Look for YYYYMM pattern
        for i in range(len(clean) - 5):
            if clean[i:i+6].isdigit():
                year = clean[i:i+4]
                month = clean[i+4:i+6]
                if 1900 <= int(year) <= 2100 and 1 <= int(month) <= 12:
                    return f"m_{year}{month}"
        
        # Check for yearly archive pattern
        if 'itemindices' in clean:
            pos = clean.find('itemindices') + len('itemindices')
            if pos + 4 <= len(clean):
                year = clean[pos:pos + 4]
                if year.isdigit() and 1900 <= int(year) <= 2100:
                    return f"y_{year}"
        

        return f"other_{clean}"
    
    def _get_quarter_from_month(self, year: str, month: int) -> str:
        """Convert year and month to quarter string."""
        quarter = ((month - 1) // 3) + 1
        return f"{year}q{quarter}"

    def _months_in_quarter(self, year: str, quarter: str) -> list[str]:
        """Get all months in a quarter as YYYYMM strings."""
        quarter_num = int(quarter[1])
        start_month = (quarter_num - 1) * 3 + 1
        return [f"{year}{str(m).zfill(2)}" for m in range(start_month, start_month + 3)]

    def _check_zip_contents(self, zip_path: Path) -> dict[str, set[str]]:
        """
        Examine contents of a zip file and categorize the files inside.
        
        Returns
        -------
        dict with keys:
            'quarterly': set of quarters found (e.g., {'2005q1', '2005q2'})
            'monthly': set of months found (e.g., {'200501', '200502'})
            'other': set of other normalized filenames
        """
        contents = {'quarterly': set(), 'monthly': set(), 'other': set()}
        
        try:
            with zipfile.ZipFile(zip_path) as zf:
                for name in zf.namelist():
                    if name.endswith('/'):  # Skip directories
                        continue
                        
                    normalized = self._normalize_filename(Path(name).name)
                    if normalized.startswith('q_'):
                        contents['quarterly'].add(normalized[2:])  # Remove 'q_' prefix
                    elif normalized.startswith('m_'):
                        contents['monthly'].add(normalized[2:])  # Remove 'm_' prefix
                    else:
                        contents['other'].add(normalized)
                        
        except Exception as e:
            logger.warning(f"Couldn't read contents of {zip_path}: {e}")
            
        return contents
    
    def _extract_zip_with_flatten(self, zip_path: Path, extract_dir: Path) -> None:
        """Extract zip contents, flattening directory structure."""
        with zipfile.ZipFile(zip_path) as zf:
            for member in zf.namelist():
                filename = Path(member).name
                if not filename:  # Skip if it's a directory
                    continue
                source = zf.open(member)
                target = extract_dir / filename
                with open(target, 'wb') as f:
                    f.write(source.read())

    def get_web_data(self) -> str | None:
        """Get main webpage content."""
        response = self._make_request(self.config.target_url)
        return response.text if response else None

    def get_web_data_from_url(self, url: str) -> str | None:
        """Get data from a specific URL."""
        response = self._make_request(url)
        return response.text if response else None

    def download_file(self, url: str, path: Path) -> bool:
        """Download a file to specified path."""
        response = self._make_request(url)
        if response:
            path.write_bytes(response.content)
            return True
        return False
    
    def get_data_links(self, html: str) -> list[str]:
        soup = BeautifulSoup(html, 'html.parser')
        all_links = []
        seen_normalized = set()

        def process_page(page_soup, is_main_page=False):
            page_links = []
            for link in page_soup.find_all('a'):
                href = link.get('href')
                if href and any(term in href for term in self.config.search_terms) and href.endswith(tuple(self.config.file_types)):
                    filename = Path(href).name
                    normalized = self._normalize_filename(filename)
                    logger.debug(f"{'Main' if is_main_page else 'Previous'} page link: {href}, normalized: {normalized}")
                    if normalized not in seen_normalized or is_main_page:
                        page_links.append(href)
                        seen_normalized.add(normalized)
            return page_links

        # Process main page
        main_page_links = process_page(soup, is_main_page=True)
        self.stats['main_page_links'] = set(main_page_links)
        all_links.extend(main_page_links)
        logger.info(f"Found {len(main_page_links)} links on main page")

        # Find and process previous versions pages
        prev_editions_links = [
            link.get('href') 
            for link in soup.find_all('a') 
            if link.get('href') 
            and any(term.lower() in link.text.lower() for term in self.config.previous_edition_terms)
            and any(term in link.get('href') for term in self.config.search_terms)
        ]

        for prev_link in tqdm(prev_editions_links, desc='Checking previous versions'):
            self.rate_limiter.wait()
            full_url = self.config.base_url + prev_link
            logger.info(f"Checking previous versions page: {full_url}")

            if prev_html := self.get_web_data_from_url(full_url):
                prev_soup = BeautifulSoup(prev_html, 'html.parser')
                prev_page_links = process_page(prev_soup)

                self.stats['previous_version_links'].update(prev_page_links)
                all_links.extend(prev_page_links)
                logger.info(f"Found {len(prev_page_links)} relevant links on previous versions page")

        logger.info(f"Total links found: {len(all_links)}")
        logger.info(f"Unique normalized files: {len(seen_normalized)}")
        
        # Log all links for debugging
        for link in all_links:
            logger.debug(f"Final link: {link}")

        return all_links

    def process_files(self, data_links: list[str], download_dir: Path = None) -> None:
        """Process files with comprehensive tracking."""
        if download_dir is None:
            download_dir = Path('data')
            
        logger.info(f"Starting to process {len(data_links)} files")

        download_dir.mkdir(parents=True, exist_ok=True)
        extract_dir = download_dir / 'extracted_files'
        extract_dir.mkdir(exist_ok=True)
        temp_dir = download_dir / 'temp'
        temp_dir.mkdir(exist_ok=True)
        
        processed_log = extract_dir / 'processed_zips.txt'
        processed = set(processed_log.read_text().splitlines()) if processed_log.exists() else set()

        # First pass - analyze yearly archives
        for link in [l for l in data_links if self._normalize_filename(Path(l).name).startswith('y_')]:
            filename = Path(link).name
            year = self._normalize_filename(filename)[2:]  # Remove 'y_'
            
            temp_path = temp_dir / filename

            if self.download_file(self.config.base_url + link, temp_path):
                contents = self._check_zip_contents(temp_path)
                self.stats['yearly_archive_contents'][year] = contents
                temp_path.unlink()
                logger.info(f"Yearly archive {year} contains {len(contents['quarterly'])} quarterly and {len(contents['monthly'])} monthly files")

        # Process all files
        for link in tqdm(data_links, desc='Processing files'):
            filename = Path(link).name
            normalized = self._normalize_filename(filename)
            file_path = download_dir / filename

            logger.debug(f"Processing link: {link}")
            logger.debug(f"Filename: {filename}, Normalized: {normalized}")

            # Skip if content is in yearly archive
            year = normalized[2:6]  # Extract year from any normalized name
            if year in self.stats['yearly_archive_contents']:
                contents = self.stats['yearly_archive_contents'][year]
                if (normalized.startswith('m_') and normalized[2:] in contents['monthly']) or \
                   (normalized.startswith('q_') and normalized[2:] in contents['quarterly']):
                    self.stats['skipped_files'].add(filename)
                    logger.debug(f"Skipping {filename} - found in yearly archive {year}")
                    continue

            if normalized.startswith('y_'):
                if filename in processed:
                    self.stats['skipped_files'].add(filename)
                    continue
                
                if self.download_file(self.config.base_url + link, file_path):
                    logger.info(f"Extracting yearly archive: {filename}")
                    try:
                        self._extract_zip_with_flatten(file_path, extract_dir)
                        processed.add(filename)
                        processed_log.write_text('\n'.join(sorted(processed)))
                    except Exception as e:
                        logger.error(f"Error extracting {filename}: {e}")
                    finally:
                        file_path.unlink()
                        
            elif filename.endswith('.zip'):
                if filename in processed:
                    self.stats['skipped_files'].add(filename)
                    continue
                    
                if self.download_file(self.config.base_url + link, file_path):
                    logger.info(f"Extracting quarterly zip file: {filename}")
                    try:
                        quarter = normalized[2:] if normalized.startswith('q_') else None
                        if quarter:
                            zip_contents = self._check_zip_contents(file_path)
                            self.stats['quarterly_zip_contents'][quarter] = zip_contents
                            
                        self._extract_zip_with_flatten(file_path, extract_dir)
                        processed.add(filename)
                        processed_log.write_text('\n'.join(sorted(processed)))
                    except Exception as e:
                        logger.error(f"Error extracting {filename}: {e}")
                    finally:
                        file_path.unlink()
            else:
                if file_path.exists():
                    self.stats['skipped_files'].add(filename)
                    continue
                
                if self.download_file(self.config.base_url + link, file_path):
                    self.stats['individual_files'].add(filename)

        # Print comprehensive statistics
        yearly_files = sum(
            len(contents['monthly']) + len(contents['quarterly']) 
            for contents in self.stats['yearly_archive_contents'].values()
        )
        
        quarterly_files = sum(
            len(contents['monthly']) + len(contents['quarterly'])
            for contents in self.stats['quarterly_zip_contents'].values()
        )
        
        logger.info("\nProcessing Summary:")
        logger.info(f"Total links found: {len(self.stats['main_page_links']) + len(self.stats['previous_version_links'])}")
        logger.info(f"- From main page: {len(self.stats['main_page_links'])}")
        logger.info(f"- From previous versions: {len(self.stats['previous_version_links'])}")
        logger.info("\nFiles processed:")
        logger.info(f"- Files extracted from yearly archives: {yearly_files}")
        logger.info(f"- Files extracted from quarterly zips: {quarterly_files}")
        logger.info(f"- Individual files downloaded: {len(self.stats['individual_files'])}")
        logger.info(f"- Files skipped (duplicates/already processed): {len(self.stats['skipped_files'])}")
        logger.info(f"Total unique files processed: {yearly_files + quarterly_files + len(self.stats['individual_files'])}")
        
        # Cleanup
        temp_dir.rmdir()

def main():
    scraper = WebScraper(ONS_WEB_CONFIG)
    
    if html := scraper.get_web_data():
        data_links = scraper.get_data_links(html)
        scraper.process_files(data_links)

if __name__ == "__main__":
    main()