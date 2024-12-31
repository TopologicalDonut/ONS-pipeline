import logging
import time
from pathlib import Path
import zipfile
from dataclasses import dataclass, field
import requests
from requests.exceptions import RequestException, HTTPError
from bs4 import BeautifulSoup
from tqdm import tqdm
from functools import lru_cache

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

class RateLimiter:
    """
    Simple rate limiter to ensure we make only X requests per Y seconds.
    """
    def __init__(self, requests_per_period: int, period_seconds: int):

        self.delay = period_seconds / requests_per_period
        self.last_request_time = 0.0

    def wait(self) -> None:

        now = time.time()
        time_since_last_request = now - self.last_request_time
        if time_since_last_request < self.delay:
            time.sleep(self.delay - time_since_last_request)
        self.last_request_time = time.time()

    def reduce_rate(self) -> None:
        """
        Reduce rate by 50% if we hit a 429.
        """

        self.delay *= 2

        logger.warning(f"Increasing delay between requests to {self.delay}")

class RequestManager:
    """
    Handles HTTP requests with backoff and dynamic rate limiting.
    """
    def __init__(self, config: WebConfig):

        self.config = config
        self.session = requests.Session()
        self.rate_limiter = RateLimiter(self.config.requests_per_period, self.config.period_seconds)

    def make_request(self, url: str) -> requests.Response | None:
        """
        Make an HTTP GET request, adjusting delay between requests depending on 429s.
        """

        while True:
            self.rate_limiter.wait()

            try:
                response = self.session.get(url)
                response.raise_for_status()
                return response
            
            except HTTPError as e:
                if e.response.status_code == 429:
                    retry_after = e.response.headers.get('Retry-After')
                    wait_time = int(retry_after)
                    
                    logger.warning(f"Rate limit exceeded. Backing off for {wait_time}s before retrying.")

                    time.sleep(wait_time)
                    self.rate_limiter.reduce_rate()

                else:
                    raise
            
            except RequestException as e:
                raise
    
class LinkScraper:
    """
    Responsible for parsing HTML to find the relevant data file links
    on both the main page and any "previous versions" pages. Uses the 
    'seen_normalized' set to skip duplicates on previous versions.
    """
    def __init__(self, config: WebConfig, normalize_func):

        self.config = config
        self.seen_normalized: set[str] = set()  # tracks normalized filenames we've seen
        self.normalize_func = normalize_func

    def scrape_main_page_links(self, html: str) -> list[str]:
        """
        Extract relevant file links from the main page HTML, 
        allowing duplicates if they appear. (is_main_page=True)
        """
        soup = BeautifulSoup(html, 'html.parser')
        return self._process_page(soup, is_main_page=True)

    def scrape_previous_versions_links(self, html: str) -> list[str]:
        """
        Finds and returns the links to any "previous versions" pages themselves.
        (We do *not* parse data file links here; we just find the page URLs.)
        """
        soup = BeautifulSoup(html, 'html.parser')
        return [
            link.get('href') for link in soup.find_all('a')
            if (href := link.get('href'))
                and any(term.lower() in link.text.lower()
                        for term in self.config.previous_edition_terms)
                and any(term in href for term in self.config.search_terms)
        ]

    def scrape_links_from_previous_page(self, html: str) -> list[str]:
        """
        Parse the actual data file links on a 'previous versions' page,
        skipping duplicates if we've already seen a normalized filename.
        (is_main_page=False)
        """
        soup = BeautifulSoup(html, 'html.parser')
        return self._process_page(soup, is_main_page=False)

    def _process_page(self, soup: BeautifulSoup, is_main_page: bool) -> list[str]:
        """
        Core logic that scans a page for relevant data file links, 
        applying your original 'seen_normalized' logic.
        """

        page_links = []

        for href in (link.get('href') for link in soup.find_all('a')):
            if self._is_relevant_link(href):
                normalized = self.normalize_func(Path(href).name)
                if is_main_page or normalized not in self.seen_normalized:
                    page_links.append(href)
                    self.seen_normalized.add(normalized)
        
        return page_links

    def _is_relevant_link(self, href: str | None) -> bool:
        """
        Checks if the link is relevant based on search terms + file extension.
        """
        return bool(href
                    and any(term in href for term in self.config.search_terms)
                    and href.endswith(tuple(self.config.file_types))
                )

class FileProcessor:
    """
    Handles:
    - Normalizing filenames (yearly, quarterly, monthly, etc.)
    - Downloading files
    - Checking zip contents
    - Extracting zip files
    """
    def __init__(self, config: WebConfig, request_manager: RequestManager):

        self.config = config
        self.request_manager = request_manager
        self.logger = logger

    @lru_cache(maxsize = 1000)
    def normalize_filename(self, filename: str) -> str:

        basename = Path(filename).stem
        clean = ''.join(c.lower() for c in basename if c.isalnum())

        # Check for quarterly pattern
        q_pos = clean.find('q')
        if q_pos != -1 and q_pos + 1 < len(clean) and clean[q_pos + 1] in '1234':
            if q_pos >= 4:  # Need at least 4 chars before for year
                year = clean[q_pos - 4:q_pos]
                if year.isdigit() and 1900 <= int(year) <= 2100:
                    return f"q_{year}q{clean[q_pos + 1]}"

        # Check for YYYYMM pattern
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

    def download_file(self, url: str, destination: Path) -> bool:
        """
        Download a file from a given URL to a local path, returning True if success.
        """
        response = self.request_manager.make_request(url)
        if response:
            destination.write_bytes(response.content)
            return True
        return False

    def check_zip_contents(self, zip_path: Path) -> dict[str, set[str]]:
        """
        Examine a zip file's contents, categorizing them by:
          - 'quarterly': set of q_YYYYqN
          - 'monthly': set of m_YYYYMM
          - 'other': set of everything else
        """
        contents: dict[str, set[str]] = {'quarterly': set(), 'monthly': set(), 'other': set()}
        try:
            with zipfile.ZipFile(zip_path) as zf:
                for name in zf.namelist():
                    if name.endswith('/'):
                        continue
                    inner_name = Path(name).name
                    normalized = self.normalize_filename(inner_name)
                    if normalized.startswith('q_'):
                        contents['quarterly'].add(normalized[2:])  # strip off 'q_'
                    elif normalized.startswith('m_'):
                        contents['monthly'].add(normalized[2:])   # strip off 'm_'
                    else:
                        contents['other'].add(normalized)
        except Exception as e:
            self.logger.warning(f"Couldn't read contents of {zip_path}: {e}")

        return contents

    def extract_zip_with_flatten(self, zip_path: Path, extract_dir: Path) -> None:
        """
        Extract zip contents into a single-level directory (flatten).
        """
        with zipfile.ZipFile(zip_path) as zf:
            for member in zf.namelist():
                filename = Path(member).name
                if not filename:  # skip directories
                    continue
                with zf.open(member) as source, open(extract_dir / filename, 'wb') as out_file:
                    out_file.write(source.read())

class WebScraperPipeline:
    """
    Orchestrates the entire flow:
      1. Get main page -> find data file links (main)
      2. Find "previous versions" pages -> parse their data file links (skip duplicates)
      3. Process files: 
         - first pass yearly archives -> store contents
         - second pass everything else
      4. Log stats
    """

    def __init__(self, config: WebConfig):
        self.config = config
        self.request_manager = RequestManager(config)
        self.file_processor = FileProcessor(config, self.request_manager)

        # The LinkScraper needs the normalization logic
        self.link_scraper = LinkScraper(
            config, normalize_func = self.file_processor.normalize_filename
        )

        self.stats = {
            'main_page_links': set(),
            'previous_version_links': set(),
            'yearly_archive_contents': {},  # { year: {'quarterly': set(), 'monthly': set(), 'other': set()} }
            'quarterly_zip_contents': {},   # { quarter: {'quarterly': set(), 'monthly': set(), 'other': set()} }
            'individual_files': set(),
            'skipped_files': set()
        }

    def run(self, download_dir: Path | None = None):
        if download_dir is None:
            download_dir = Path('data')

        download_dir.mkdir(parents=True, exist_ok=True)
        extract_dir = download_dir / 'extracted_files'
        extract_dir.mkdir(exist_ok=True)
        temp_dir = download_dir / 'temp'
        temp_dir.mkdir(exist_ok=True)

        # 1) Grab main page
        main_html = self._get_page_content(self.config.target_url)
        if not main_html:
            logger.error("Failed to retrieve main page HTML. Exiting.")
            return

        # 2a) Get data links from main page
        main_links = self.link_scraper.scrape_main_page_links(main_html)
        self.stats['main_page_links'] = set(main_links)
        logger.info(f"Found {len(main_links)} relevant links on main page")

        # 2b) Find links to "previous versions" pages
        prev_version_page_links = self.link_scraper.scrape_previous_versions_links(main_html)
        logger.info(f"Found {len(prev_version_page_links)} 'previous versions' pages")

        # For each "previous versions" page, parse its data links
        for link in tqdm(prev_version_page_links, desc='Checking previous versions'):
            full_url = self.config.base_url + link
            prev_html = self._get_page_content(full_url)
            if not prev_html:
                continue
            # scrape data file links from this "previous versions" page
            previous_links = self.link_scraper.scrape_links_from_previous_page(prev_html)
            self.stats['previous_version_links'].update(previous_links)

        # Combine them all
        all_links = list(self.stats['main_page_links']) + list(self.stats['previous_version_links'])
        logger.info(f"Total combined links: {len(all_links)}")

        # 3) Process all files
        self._process_files(all_links, download_dir, extract_dir, temp_dir)

        # Cleanup temp dir if empty
        try:
            temp_dir.rmdir()
        except OSError:
            pass

        # 4) Print summary
        self._print_summary()

    # -----------------------------------------------------------
    # Pipeline Sub-steps
    # -----------------------------------------------------------
    def _get_page_content(self, url: str) -> str | None:
        """
        Get the page text from a URL using RequestManager.
        """
        response = self.request_manager.make_request(url)
        return response.text if response else None

    def _process_files(
        self, 
        data_links: list[str], 
        download_dir: Path, 
        extract_dir: Path, 
        temp_dir: Path
    ) -> None:
        """
        Process each link in 2 phases:
         - Phase 1: Find and parse yearly archives first (so we can skip duplicates)
         - Phase 2: Download and extract the rest (skipping those that are in yearly archives)
        """
        logger.info(f"Beginning file processing of {len(data_links)} total links.")

        processed_log_path = extract_dir / 'processed_zips.txt'
        if processed_log_path.exists():
            processed = set(processed_log_path.read_text().splitlines())
        else:
            processed = set()

        file_proc = self.file_processor

        # ------------ PHASE 1: Analyze Yearly Archives ------------
        # We do this first so that we can skip monthly/quarterly duplicates if they're inside a yearly archive
        yearly_links = [
            link for link in data_links 
            if file_proc.normalize_filename(Path(link).name).startswith('y_')
        ]

        for link in yearly_links:
            filename = Path(link).name
            normalized = file_proc.normalize_filename(filename)
            year = normalized[2:]  # remove 'y_'

            # Only analyze if not already processed
            if filename in processed:
                self.stats['skipped_files'].add(filename)
                logger.debug(f"Skipping yearly archive {filename} - already processed.")
                continue

            # Download to temp, read contents, remove temp
            temp_path = temp_dir / filename
            if file_proc.download_file(self.config.base_url + link, temp_path):
                zip_contents = file_proc.check_zip_contents(temp_path)
                self.stats['yearly_archive_contents'][year] = zip_contents
                temp_path.unlink()

                logger.info(
                    f"Yearly archive {year}: {len(zip_contents['quarterly'])} quarterly, "
                    f"{len(zip_contents['monthly'])} monthly, {len(zip_contents['other'])} others"
                )
            else:
                logger.warning(f"Failed to download yearly archive: {filename}")

        # ------------ PHASE 2: Process everything ------------
        for link in tqdm(data_links, desc='Processing files'):
            filename = Path(link).name
            normalized = file_proc.normalize_filename(filename)
            local_path = download_dir / filename

            # Skip if found in yearly archive
            # e.g., normalized is "m_202303" or "q_2023q1"
            if normalized.startswith(('m_', 'q_')):
                possible_year = normalized[2:6]  # e.g., '2023'
                if possible_year in self.stats['yearly_archive_contents']:
                    contents_dict = self.stats['yearly_archive_contents'][possible_year]
                    if (
                        normalized.startswith('m_')
                        and normalized[2:] in contents_dict['monthly']
                    ) or (
                        normalized.startswith('q_')
                        and normalized[2:] in contents_dict['quarterly']
                    ):
                        self.stats['skipped_files'].add(filename)
                        logger.debug(f"Skipping {filename}, it's already in yearly archive {possible_year}")
                        continue

            # Yearly ZIP file
            if normalized.startswith('y_'):
                if filename in processed:
                    self.stats['skipped_files'].add(filename)
                    continue

                if file_proc.download_file(self.config.base_url + link, local_path):
                    logger.info(f"Extracting yearly archive: {filename}")
                    try:
                        file_proc.extract_zip_with_flatten(local_path, extract_dir)
                        processed.add(filename)
                        processed_log_path.write_text('\n'.join(sorted(processed)))
                    except Exception as e:
                        logger.error(f"Error extracting {filename}: {e}")
                    finally:
                        local_path.unlink()

            # Quarterly ZIP (or any ZIP not matching 'y_')
            elif filename.endswith('.zip'):
                if filename in processed:
                    self.stats['skipped_files'].add(filename)
                    continue

                if file_proc.download_file(self.config.base_url + link, local_path):
                    logger.info(f"Extracting quarterly/other zip: {filename}")
                    try:
                        if normalized.startswith('q_'):
                            quarter = normalized[2:]
                            zip_contents = file_proc.check_zip_contents(local_path)
                            self.stats['quarterly_zip_contents'][quarter] = zip_contents
                        file_proc.extract_zip_with_flatten(local_path, extract_dir)

                        processed.add(filename)
                        processed_log_path.write_text('\n'.join(sorted(processed)))
                    except Exception as e:
                        logger.error(f"Error extracting {filename}: {e}")
                    finally:
                        local_path.unlink()

            # Individual file (.csv, .xlsx, etc.)
            else:
                # If local file exists, skip
                if local_path.exists():
                    self.stats['skipped_files'].add(filename)
                    continue

                if file_proc.download_file(self.config.base_url + link, local_path):
                    self.stats['individual_files'].add(filename)

        logger.info("File processing complete.")

    def _print_summary(self) -> None:
        """
        Print summary info about how many links we found, files we extracted, etc.
        """
        # Summaries from yearly archives
        yearly_files = sum(
            len(contents['monthly']) + len(contents['quarterly']) + len(contents['other'])
            for contents in self.stats['yearly_archive_contents'].values()
        )

        # Summaries from quarterly zips
        quarterly_files = sum(
            len(contents['monthly']) + len(contents['quarterly']) + len(contents['other'])
            for contents in self.stats['quarterly_zip_contents'].values()
        )

        logger.info("\nProcessing Summary:")
        total_links = len(self.stats['main_page_links']) + len(self.stats['previous_version_links'])
        logger.info(f"Total links found: {total_links}")
        logger.info(f"- From main page: {len(self.stats['main_page_links'])}")
        logger.info(f"- From previous versions: {len(self.stats['previous_version_links'])}")

        logger.info("\nFiles processed:")
        logger.info(f"- Files extracted from yearly archives: {yearly_files}")
        logger.info(f"- Files extracted from quarterly zips: {quarterly_files}")
        logger.info(f"- Individual files downloaded: {len(self.stats['individual_files'])}")
        logger.info(f"- Files skipped (duplicates/already processed): {len(self.stats['skipped_files'])}")

        total_unique = yearly_files + quarterly_files + len(self.stats['individual_files'])
        logger.info(f"Total unique files processed: {total_unique}")

def main():

    scraper_pipeline = WebScraperPipeline(config=ONS_WEB_CONFIG)
    scraper_pipeline.run(download_dir = PATH_CONFIG.DATA_DIR)

if __name__ == "__main__":
    main()
