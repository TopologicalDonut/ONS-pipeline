import logging
from pathlib import Path

from src.logger import setup_logger
from src.scraper import WebScraper
from src.reader import main as read_data
from src.processor import main as process_data
from src.database import main as load_database

logger = setup_logger(__name__, logging.INFO)

def run_pipeline(
    data_dir: Path = Path('data'),
    db_path: str = 'database/ons_cpi.db'
) -> bool:
    
    try:
        logger.info("Starting pipeline")
        
        logger.info("Step 1: Scraping data")
        BASE_URL = 'https://www.ons.gov.uk'
        TARGET_URL = f'{BASE_URL}/economy/inflationandpriceindices/datasets/consumerpriceindicescpiandretailpricesindexrpiitemindicesandpricequotes'
        FILE_TYPES = [".csv", ".xlsx", ".zip"]
        SEARCH_TERMS = ["upload-itemindices", "/itemindices"]
        
        scraper = WebScraper(BASE_URL, rate_limit=1.5, logger=logger)
        if html := scraper.get_web_data(TARGET_URL):
            data_links = scraper.get_data_links(html, FILE_TYPES, SEARCH_TERMS)
            scraper.process_files(data_links, data_dir)
        else:
            raise RuntimeError("Failed to fetch data from ONS website")
        
        logger.info("Step 2: Reading and combining data")
        raw_data = read_data()
        if raw_data is None:
            raise RuntimeError("Failed to read data")
        
        logger.info("Step 3: Processing and validating data")
        clean_data = process_data(raw_data)
        if clean_data is None:
            raise RuntimeError("Failed to process data")
        
        logger.info("Step 4: Loading database")
        success = load_database(clean_data, db_path)
        if not success:
            raise RuntimeError("Failed to load database")
        
        logger.info("Pipeline completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    run_pipeline()