import logging

from src.logger import setup_logger
from src.scraper import main as scrape_data
from src.reader import main as read_data
from src.processor import main as process_data
from src.database import main as load_database

logger = setup_logger(__name__, logging.INFO)

def run_pipeline() -> bool:
    try:
        logger.info("Starting pipeline")
        
        logger.info("Step 1: Scraping data")
        scrape_data()
        
        logger.info("Step 2: Reading and combining data")
        raw_data = read_data()
        if raw_data is None:
            raise RuntimeError("Failed to read data")
        
        logger.info("Step 3: Processing and validating data")
        clean_data = process_data(raw_data)
        if clean_data is None:
            raise RuntimeError("Failed to process data")
        
        logger.info("Step 4: Loading database")
        success = load_database(clean_data)
        if not success:
            raise RuntimeError("Failed to load database")
            
        logger.info("Pipeline completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    run_pipeline()