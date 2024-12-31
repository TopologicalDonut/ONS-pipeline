import logging

from src.const import PATH_CONFIG
from src.scraper import main as scrape_data
from src.processor import main as process_data
from src.database import main as load_database

def run_pipeline() -> bool:

    PATH_CONFIG.LOG_DIR.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(PATH_CONFIG.LOG_DIR / 'ons_cpi.log')
        ]
    )

    logger = logging.getLogger(__name__)

    try:
        logger.info("Starting pipeline")
        
        logger.info("Step 1: Scraping data")
        scrape_data()
        
        logger.info("Step 2: Processing and validating data")
        clean_data = process_data()
        if clean_data is None:
            raise RuntimeError("Failed to process data")
        
        logger.info("Step 3: Loading database")
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