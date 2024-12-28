from dataclasses import dataclass, field
from typing import Callable, Any
from datetime import datetime
import polars as pl
from pathlib import Path
import json 
import logging

from src.const import PATH_CONFIG

"""
This script is for processing ONS data so that it's ready for database storage.

To validate the data, should ensure:
    1. No null values
    2. Date in YYYYMM format and then converted into YYYY-MM-DD string (we don't use datetime b/c duckdb converts str to date).
    3. Item desc has no leading/trailing whitespace
    4. Item index is a valid index i.e. >= 0 float
    5. No duplicate date/item_id pairs

The implementation has some dataclasses defined to make it easy to create and use validation rules and keep track of any problematic data.
"""

logger = logging.getLogger(__name__)

type ValidationFunction = Callable[[pl.Expr], pl.Expr]
type ValidationRule = list[tuple[ValidationFunction, str]]

@dataclass(frozen = True)
class ValidationConfig:
    """
    Example
    --------
    columns={
        'date': [
            (lambda col: col.is_not_null(),
            "Missing date")
        ],
        'item_id': [
            (lambda col: col.is_not_null(),
            "Missing item ID")
        ]
    }
    """

    columns: dict[str, ValidationRule]
    duplicate_check_columns: list[str]

ONS_VALIDATION_CONFIG = ValidationConfig(
    columns={
        'date': [
            (lambda col: col.is_not_null(),
            "Missing date"),
            (lambda col: (col >= 100000) & (col <= 999999), 
            "Invalid date format"),
            (lambda col: (col % 100 <= 12) & (col % 100 > 0), 
            "Invalid month")
        ],
        'item_id': [
            (lambda col: col.is_not_null(),
            "Missing item ID")
        ],
        'item_desc': [
            (lambda col: col.is_not_null(), 
            "Missing or Empty description"),
            (lambda col: col.str.strip_chars().str.len_chars() > 0,
            "Empty description after trimming")
        ],
        'item_index': [
            (lambda col: col.is_not_null() & (col > 0), 
            "Invalid index value")
        ]
    },
    duplicate_check_columns=['date', 'item_id']
)

@dataclass
class ValidationResults:

    total_rows: int = 0
    invalid_rows: dict[str, int] = field(default_factory = dict)
    rows_retained: int = 0
    problem_rows: list[dict[str, Any]] = field(default_factory = list)

    def add_problem(self, rows: pl.DataFrame, reason: str) -> None:

        self.invalid_rows[reason] = self.invalid_rows.get(reason, 0) + rows.height

        for row in rows.to_dicts():
            self.problem_rows.append(
                {
                    'row': row,
                    'reason': reason
                }
            )
    
    def save_problem(self, output_path: Path | None) -> None:

        if self.problem_rows and output_path != None:

            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S') # Windows is dumb, can't use : for timestamp
            output_file = output_path / f"validation_problems_{timestamp}.json"

            logger.info(f"Saving validation problems to {output_file}")

            with open(output_file, 'w') as f:
                json.dump(self.problem_rows, f, indent = 2)

    def print_summary(self) -> None:

        logger.info("Validation summary:")
        logger.info(f"Total rows processed: {self.total_rows}")

        for reason, count in self.invalid_rows.items():
            logger.info(f"{reason}: {count}")

        logger.info(f"Rows retained: {self.rows_retained}")

def process_data(
    df: pl.DataFrame,
    config: ValidationConfig,
    problems_path: Path | None = PATH_CONFIG.VALIDATION_DIR
) -> tuple[pl.DataFrame, ValidationResults]:
    """
    Parameters
    ----------
    config
        A ValidationConfig that contains the columns and validation rules for processing the data.

    problems_path
        Path to where the json with info on the problem rows is saved.
    """ 

    if problems_path != None:
        Path.mkdir(problems_path, exist_ok = True) 

    logger.info(f"Starting data processing with {df.height} rows")

    val_results = ValidationResults(total_rows = df.height)

    for col_name, validations in config.columns.items():
        for validate_func, error_msg in validations:

            valid = validate_func(pl.col(col_name))
            invalid_rows = df.filter(~valid)

            if invalid_rows.height > 0:

                val_results.add_problem(invalid_rows, error_msg)
                df = df.filter(valid)

    if 'date' in df.columns and df['date'].dtype in [pl.Int32, pl.Int64]:
        df = df.with_columns([
            pl.col('date')
            .cast(pl.Utf8)
            .str.replace(r'(\d{4})(\d{2})', r'$1-$2-01')
            .cast(pl.Date)
            .alias('date')
        ])

    if config.duplicate_check_columns:
        
        duplicates = df.filter(~pl.struct(config.duplicate_check_columns).is_first_distinct())

        if duplicates.height > 0:

            val_results.add_problem(duplicates, "Duplicate entries")
            df = df.unique(subset = config.duplicate_check_columns, keep = 'first')

    val_results.rows_retained = df.height

    if problems_path:
        
        val_results.save_problem(problems_path)
    
    logger.info(f"Processing complete: retained {val_results.rows_retained} of {val_results.total_rows} rows")

    return df, val_results

def main(input_df: pl.DataFrame) -> pl.DataFrame:

    try:
        clean_data, validation_val_results = process_data(
            input_df, 
            ONS_VALIDATION_CONFIG
        )

        validation_val_results.print_summary()
        print(clean_data.head(10))

        return clean_data
        
    except Exception:
        
        logger.error("Error processing data", exc_info=True)

        return None

if __name__ == '__main__':
    main()

