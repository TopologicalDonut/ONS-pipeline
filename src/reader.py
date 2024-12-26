from pathlib import Path
import polars as pl
import logging
from src.logger import setup_logger

"""
This script is for reading all the ONS data from the target directory of the scraper.
There's four main things to get from the data:
    1. Date
    2. Item ID
    3. Item Description
    4. Item Index (ALL_GM_INDEX)
"""

type SourceName = str
type TargetName = str

logger = setup_logger(__name__, logging.INFO)

COL_MAPPINGS = {
    'INDEX_DATE': 'date',
    'ITEM_ID': 'item_id',
    'ITEM_DESC': 'item_desc',
    'ALL_GM_INDEX': 'item_index'
}

DATA_DIR = Path(__file__).parent.parent / 'data'

def _read_file(filepath: Path) -> pl.DataFrame | None:
    """
    Reads a single file into a polars dataframe. Takes either .csv or .xlsx.
    """
    try:
        logger.debug(f"Processing {filepath}")

        if filepath.suffix.lower() == '.csv':
            df = pl.read_csv(filepath, ignore_errors = True)
        else:
            df = pl.read_excel(filepath)

        logger.debug(f"Successfully read {filepath}, shape:{df.shape}")
        return df
    
    except Exception as e:
        logger.error(f"Error reading {filepath}: {e}", exc_info = True)
        return None

def _standardize_columns(df:pl.DataFrame, column_mapping: dict[SourceName, TargetName]) -> pl.DataFrame:
    """
    Standardizes column names for given columns with case-insensitive matching. Also selects
    the needed columns.
    """
    df_cols = {col.lower(): col for col in df.columns}

    # Check if any missing cols i.e. mistake in column_mapping
    missing_cols = [
        source_name for source_name in column_mapping.keys()
        if source_name.lower() not in df_cols
    ]

    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    standardized_cols = df.select(
        [
            pl.col(df_cols[source_name.lower()]).alias(target_name)
            for source_name, target_name in column_mapping.items()
            if source_name.lower() in df_cols
        ]
    )

    if standardized_cols.width == 0:
        raise ValueError(f"No required columns found. Looking for {list(column_mapping.keys())}")
    
    return standardized_cols

def read_data(column_mapping: dict[SourceName, TargetName], data_folder: Path) -> pl.DataFrame:

    data_frames = []
    
    for filepath in data_folder.rglob('*'):
        if filepath.suffix.lower() in ('.csv', '.xlsx'):

            df = _read_file(filepath)
            if df is not None:
                df = (
                    _standardize_columns(df, column_mapping)
                    # Want this to keep track of which files are problematic later.
                    .with_columns(pl.lit(str(filepath)).alias('source_file'))
                )
                data_frames.append(df)

    if not data_frames:

        raise ValueError("No valid data files found")
    
    return pl.concat(data_frames)

def main() -> pl.DataFrame:
    
    df = read_data(COL_MAPPINGS, DATA_DIR)

    logger.info(f"Read {df.height} rows from data")
    print(df.head(10))
    
    return df

if __name__ == '__main__':
    main()