from pathlib import Path
import polars as pl
from dataclasses import dataclass, field
from typing import Tuple
from datetime import datetime
import json
import logging

"""
This script is for processing the ONS data so that it's ready for storing in the database.

There's four main things to get from the data:
    1. Date
    2. Item ID
    3. Item Description
    4. Item Index (ALL_GM_INDEX)

Considerations:
    - Date is stored as YYYYMM, so need to convert to a proper format for the database.
    - There's some errors/extraneous symbols in the data that need to be dealt with (e.g. a '*' in the date column),
        so we should validate the data.
"""
# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('data_processing.log'),
        logging.StreamHandler()  # This will print to console too
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ValidationResults:
    total_rows: int = 0
    invalid_dates: int = 0
    invalid_ids: int = 0
    missing_descriptions: int = 0
    invalid_indices: int = 0
    rows_retained: int = 0
    problem_rows: list[dict] = field(default_factory=list)
    
    def add_problem(self,
        file: Path, 
        rows: pl.DataFrame, 
        reason: str
    ) -> None:
        """Add problematic rows to the tracking list"""
        for row in rows.to_dicts():
            self.problem_rows.append({
                "file": str(file),
                "row": row,
                "reason": reason
            })
    
    def save_problems(self, output_path: Path) -> None:
        """Save problem rows to a JSON file with timestamp"""
        if self.problem_rows:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = output_path / f"validation_problems_{timestamp}.json"
            
            logger.info(f"Saving validation problems to {output_file}")
            with open(output_file, 'w') as f:
                json.dump(self.problem_rows, f, indent=2)
    
    def print_summary(self) -> None:
        """Print a summary of the validation results"""
        logger.info("\nValidation Summary:")
        logger.info(f"Total rows processed: {self.total_rows}")
        logger.info(f"Invalid dates: {self.invalid_dates}")
        logger.info(f"Invalid IDs: {self.invalid_ids}")
        logger.info(f"Missing descriptions: {self.missing_descriptions}")
        logger.info(f"Invalid indices: {self.invalid_indices}")
        logger.info(f"Rows retained: {self.rows_retained}")
        if self.total_rows > 0:
            logger.info(f"Percentage retained: {(self.rows_retained/self.total_rows)*100:.2f}%")

def read_data(data_folder: Path) -> pl.DataFrame:
    """
    Read and combine all data files with case-insensitive column matching (because the ONS data is inconsistent).
    """
    data_frames = []

    required_cols = {
        'index_date': 'date',
        'item_id': 'item_id',
        'item_desc': 'item_desc',
        'all_gm_index': 'item_index'
    }
    
    for filepath in data_folder.rglob('*'):
        if filepath.suffix.lower() in ('.csv', '.xlsx'):
            logger.debug(f'Processing {filepath}')
            try:
                if filepath.suffix.lower() == '.csv':
                    df = pl.read_csv(filepath, ignore_errors = True) # random characters and words in some of data
                else:
                    df = pl.read_excel(filepath)
                
                col_mapping = {col.lower(): col for col in df.columns}

                # col_mapping = {lowername: originalname (either same or UPPERCASE)}
                # required_cols = {lowername: standardname}
                #
                # If required column in col_mapping, rename originalname to standardname (b/c col_mapping[req_col]
                # would give corresponding originalname).
                
                df = df.select(
                    [pl.col(col_mapping[req_col]).alias(std_name)
                     for req_col, std_name in required_cols.items()
                     if req_col in col_mapping]
                )
                
                # Need source file info to trace problems back to the original file
                df = df.with_columns(pl.lit(str(filepath)).alias('source_file'))
                data_frames.append(df)
                logger.debug(f"Successfully read {filepath}, shape: {df.shape}")
                
            except Exception as e:
                logger.error(f"Error reading {filepath}: {e}", exc_info=True)
                continue
    
    if not data_frames:
        raise ValueError("No valid data files found")
    
    return pl.concat(data_frames)

def validate_data(df: pl.DataFrame, file_path: Path) -> Tuple[pl.DataFrame, ValidationResults]:
    """
    Clean and validate all data files in the file path
    """
    logger.info(f"Starting validation with {df.height} rows")
    results = ValidationResults(total_rows=df.height)
    
    # Validate dates
    pre_date_count = df.height
    logger.debug("Validating dates")

    date_conditions = (
        pl.col('date').cast(pl.Utf8).str.contains(r'^\d{6}$') &
        (pl.col('date') % 100 <= 12) &
        (pl.col('date') % 100 > 0)
    )

    # Polars will automatically remove null dates 

    invalid_dates = df.filter(~date_conditions | pl.col('date').is_null())
    if invalid_dates.height > 0:
        logger.warning(f"Found {invalid_dates.height} invalid dates")
        results.invalid_dates += invalid_dates.height
        results.add_problem(
            file=file_path,
            rows=invalid_dates,
            reason="Invalid date format or value"
        )
    df = df.filter(date_conditions)
    logger.info(f"Date validation: {pre_date_count - df.height} rows removed")
    
    # Validate IDs
    pre_id_count = df.height
    logger.debug("Validating IDs")
    invalid_ids = df.filter(pl.col('item_id').is_null())
    if invalid_ids.height > 0:
        logger.warning(f"Found {invalid_ids.height} missing item IDs")
        results.invalid_ids = invalid_ids.height
        results.add_problem(
            file=file_path,
            rows=invalid_ids,
            reason="Missing item ID"
        )
    df = df.filter(pl.col('item_id').is_not_null())
    logger.info(f"ID validation: {pre_id_count - df.height} rows removed")

    # Clean and validate descriptions
    pre_desc_count = df.height
    logger.debug("Validating descriptions")
    df = df.with_columns(pl.col('item_desc').str.strip_chars().alias('item_desc'))
    
    invalid_descriptions = df.filter(
        pl.col('item_desc').is_null() | 
        (pl.col('item_desc').str.len_chars() == 0)
    )
    if invalid_descriptions.height > 0:
        logger.warning(f"Found {invalid_descriptions.height} missing or empty item descriptions")
        results.missing_descriptions = invalid_descriptions.height
        results.add_problem(
            file=file_path,
            rows=invalid_descriptions,
            reason="Missing or empty item description"
        )
    df = df.filter(
        pl.col('item_desc').is_not_null() & 
        (pl.col('item_desc').str.len_chars() > 0)
    )

    logger.info(f"Description validation: {pre_desc_count - df.height} rows removed")
    
    # Validate indices
    pre_indices_count = df.height
    logger.debug("Validating indices")
    invalid_indices = df.filter(
        pl.col('item_index').is_null() | 
        (pl.col('item_index') < 0)
    )
    if invalid_indices.height > 0:
        logger.warning(f"Found {invalid_indices.height} invalid item indices")
        results.invalid_indices = invalid_indices.height
        results.add_problem(
            file=file_path,
            rows=invalid_indices,
            reason="Invalid item index"
        )
    df = df.filter(
        pl.col('item_index').is_not_null() &
        (pl.col('item_index') >= 0)
    )
    
    logger.info(f"Index validation: {pre_indices_count - df.height} rows removed")

    results.rows_retained = df.height
    logger.info(f"Validation complete: retained {results.rows_retained} of {results.total_rows} rows")
    return df, results

def main():
    logger.info("Starting data processing")
    data_folder = Path('data')
    problems_folder = Path('data/validation_problems')
    problems_folder.mkdir(parents=True, exist_ok=True)
    
    try:  
        raw_data = read_data(data_folder)
        logger.info(f"Initial data shape: {raw_data.shape}")
        
        clean_data, validation_results = validate_data(
            raw_data, 
            data_folder,
        )

        # Save problem rows
        validation_results.save_problems(problems_folder)
        
        # Print summary
        validation_results.print_summary()

        logger.info("\nSample of cleaned data:")
        print(clean_data.head(10))
        
        logger.info("Data processing completed successfully")
        
        return clean_data
        
    except Exception as e:
        logger.error("Error processing data", exc_info=True)
        return None

if __name__ == '__main__':
    main()