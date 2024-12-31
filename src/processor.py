from dataclasses import dataclass, field
from collections import defaultdict
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

@dataclass(frozen=True)
class ProcessorConfig:

    column_mapping: dict[str, str]
    validation_rules: dict[str, ValidationRule]
    duplicate_check_columns: list[str]
    valid_extensions: frozenset[str] = frozenset({'.csv', '.xlsx'})

ONS_CONFIG = ProcessorConfig(
    column_mapping={
        'INDEX_DATE': 'date',
        'ITEM_ID': 'item_id',
        'ITEM_DESC': 'item_desc',
        'ALL_GM_INDEX': 'item_index'
    },
    validation_rules={
        'date': [
            (lambda col: col.is_not_null(), "Missing date"),
            (lambda col: (col >= 100000) & (col <= 999999), "Invalid date format"),
            (lambda col: (col % 100 <= 12) & (col % 100 > 0), "Invalid month")
        ],
        'item_id': [
            (lambda col: col.is_not_null(), "Missing item ID")
        ],
        'item_desc': [
            (lambda col: col.is_not_null(), "Missing or Empty description"),
            (lambda col: col.str.strip_chars().str.len_chars() > 0, "Empty description after trimming")
        ],
        'item_index': [
            (lambda col: col.is_not_null() & (col > 0), "Invalid index value")
        ]
    },
    duplicate_check_columns=['date', 'item_id']
)

@dataclass
class ProcessingResults:
    total_files: int = 0
    successful_files: int = 0
    failed_files: dict[str, str] = field(default_factory=dict)
    total_rows: int = 0
    rows_retained: int = 0
    problem_rows: defaultdict[str, list[dict[str, Any]]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def add_file_error(self, filepath: str, error: str) -> None:
        self.failed_files[filepath] = error

    def add_problem(self, rows: pl.DataFrame, reason: str) -> None:
        self.problem_rows[reason].extend(rows.to_dicts())
    
    @property
    def invalid_rows(self) -> dict[str, int]:
        return {reason: len(rows) for reason, rows in self.problem_rows.items()}
    
    def save_results(self, output_dir: Path) -> None:
        if not output_dir.exists():
            output_dir.mkdir(parents=True)

        timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')
        
        if self.failed_files:
            error_file = output_dir / f"file_errors_{timestamp}.json"
            with error_file.open('w') as f:
                json.dump(self.failed_files, f, indent=2)
            logger.info(f"Saved file errors to {error_file}")

        if self.problem_rows:
            problems_file = output_dir / f"validation_problems_{timestamp}.json"
            problems_list = [
                {"row": row, "reason": reason}
                for reason, rows in self.problem_rows.items()
                for row in rows
            ]
            with problems_file.open('w') as f:
                json.dump(problems_list, f, indent=2)
            logger.info(f"Saved validation problems to {problems_file}")

    def print_summary(self) -> None:
        logger.info("\nProcessing Summary:")
        logger.info(f"Files processed: {self.total_files}")
        logger.info(f"Files succeeded: {self.successful_files}")
        logger.info(f"Files failed: {len(self.failed_files)}")
        
        if self.failed_files:
            logger.info("File Errors:")
            for filepath, error in self.failed_files.items():
                logger.info(f"{filepath}: {error}")

        logger.info(f"Total rows processed: {self.total_rows}")
        
        if self.problem_rows:
            logger.info("\nValidation Issues:")
            for reason, count in self.invalid_rows.items():
                logger.info(f"{reason}: {count}")
        
        logger.info(f"Rows retained: {self.rows_retained}")

class Processor:
    def __init__(self, config: ProcessorConfig):
        self.config = config
    
    def _read_file(self, filepath: Path) -> pl.LazyFrame:
        logger.debug(f"Processing {filepath}")
        
        if filepath.suffix.lower() == '.csv':
            return pl.scan_csv(filepath, ignore_errors = True)
        else:
            return pl.read_excel(filepath).lazy()

    def _standardize_columns(self, df: pl.LazyFrame) -> pl.LazyFrame:
        df_cols = {col.lower(): col for col in df.collect_schema().names()}
        
        missing_cols = [
            source_name for source_name in self.config.column_mapping.keys()
            if source_name.lower() not in df_cols
        ]
        
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        return df.select([
            pl.col(df_cols[source_name.lower()]).alias(target_name)
            for source_name, target_name in self.config.column_mapping.items()
        ])

    def process_file(self, filepath: Path, results: ProcessingResults) -> pl.LazyFrame | None:
        try:
            df = self._read_file(filepath)
            df = self._standardize_columns(df)
            df = df.with_columns(pl.lit(str(filepath)).alias('source_file'))
            return df
        except Exception as e:
            results.add_file_error(str(filepath), str(e))
            return None

    def validate_data(self, df: pl.LazyFrame, results: ProcessingResults) -> pl.DataFrame:
        for col_name, validations in self.config.validation_rules.items():
            for validate_func, error_msg in validations:
                valid = validate_func(pl.col(col_name))
                invalid_rows = df.filter(~valid).collect()
                
                if invalid_rows.height > 0:
                    results.add_problem(invalid_rows, error_msg)
                    df = df.filter(valid)

        # Convert YYYYMM date format to YYYY-MM-DD
        if 'date' in df.collect_schema().names():
            df = df.with_columns([
                pl.col('date')
                .cast(pl.Utf8)
                .str.replace(r'(\d{4})(\d{2})', r'$1-$2-01')
                .alias('date')
            ])

        if self.config.duplicate_check_columns:
            duplicates = df.filter(
                ~pl.struct(self.config.duplicate_check_columns).is_first_distinct()
            ).collect()
            
            if duplicates.height > 0:
                results.add_problem(duplicates, "Duplicate entries")
                df = df.unique(
                    subset=self.config.duplicate_check_columns,
                    keep='first'
                )

        return df.collect()

    def process_directory(self, data_dir: Path, output_dir: Path | None = None) -> pl.DataFrame:

        results = ProcessingResults()
        dataframes = []
        
        for filepath in data_dir.rglob('*'):
            if filepath.suffix.lower() in self.config.valid_extensions:
                results.total_files += 1
                df = self.process_file(filepath, results)
                if df is not None:
                    results.successful_files += 1
                    dataframes.append(df)

        if not dataframes:
            raise ValueError("No valid data files found")
        
        combined_df = pl.concat(dataframes)
        results.total_rows = combined_df.collect().height
        
        final_df = self.validate_data(combined_df, results)
        results.rows_retained = final_df.height
        
        if output_dir:
            results.save_results(output_dir)
        
        results.print_summary()
        return final_df

def create_ons_config() -> ProcessorConfig:
    return ProcessorConfig(
        column_mapping={
            'INDEX_DATE': 'date',
            'ITEM_ID': 'item_id',
            'ITEM_DESC': 'item_desc',
            'ALL_GM_INDEX': 'item_index'
        },
        validation_rules={
            'date': [
                (lambda col: col.is_not_null(), "Missing date"),
                (lambda col: (col >= 100000) & (col <= 999999), "Invalid date format"),
                (lambda col: (col % 100 <= 12) & (col % 100 > 0), "Invalid month")
            ],
            'item_id': [
                (lambda col: col.is_not_null(), "Missing item ID")
            ],
            'item_desc': [
                (lambda col: col.is_not_null(), "Missing or Empty description"),
                (lambda col: col.str.strip_chars().str.len_chars() > 0, "Empty description after trimming")
            ],
            'item_index': [
                (lambda col: col.is_not_null() & (col >= 0), "Invalid index value")
            ]
        },
        duplicate_check_columns=['date', 'item_id']
    )

def main() -> pl.DataFrame | None:
    try:
        processor = Processor(ONS_CONFIG)
        df = processor.process_directory(PATH_CONFIG.DATA_DIR, PATH_CONFIG.VALIDATION_DIR)
        print(df.head(10))

        return df
    except Exception:
        logger.error("Processing failed", exc_info=True)

        return None

if __name__ == "__main__":
    main()