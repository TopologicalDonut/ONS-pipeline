import duckdb
import polars as pl
from dataclasses import dataclass
import logging
from pathlib import Path

from src.logger import setup_logger

logger = setup_logger(__name__, logging.INFO)

type SQLType = str

@dataclass(frozen=True)
class TableConfig:
    id_column: str
    date_column: str
    entity_columns: dict[str, SQLType]    
    measurement_columns: dict[str, SQLType]
    entity_table: str = "items"
    data_table: str = "cpi_data"

class DuckDBManager:
    def __init__(self, db_path: str, config: TableConfig, logger: logging.Logger):

        self.conn = duckdb.connect(db_path)
        self.config = config
        self.logger = logger

    def setup_schema(self) -> None:

        self.logger.info("Creating database schema")
        
        entity_cols = [f'{self.config.id_column} VARCHAR PRIMARY KEY'] + [f'{name} {dtype}' for name, dtype in self.config.entity_columns.items()]
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.config.entity_table} (
                {', '.join(entity_cols)}
            )
        """)

        data_cols = [
            f'{self.config.date_column} DATE',
            f'{self.config.id_column} VARCHAR',
            *[f'{name} {dtype}' for name, dtype in self.config.measurement_columns.items()]
        ]
        constraints = [
            f'PRIMARY KEY ({self.config.date_column}, {self.config.id_column})',
            f'FOREIGN KEY ({self.config.id_column}) REFERENCES {self.config.entity_table}({self.config.id_column})'
        ]
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.config.data_table} (
                {', '.join(data_cols)},
                {', '.join(constraints)}
            )
        """)
        
        self.logger.info(f"Created tables: {self.config.entity_table} and {self.config.data_table}")

    def insert_data(self, df: pl.DataFrame) -> None:

        self.logger.info("Starting data insertion")

        entity_cols = [self.config.id_column] + list(self.config.entity_columns.keys())
        entities_df = df.select(entity_cols).unique(subset=[self.config.id_column])

        self.logger.debug(f"Inserting/updating {len(entities_df)} entities")
        self.conn.execute(f"INSERT OR REPLACE INTO {self.config.entity_table} SELECT * FROM entities_df")

        measurement_cols = [self.config.date_column, self.config.id_column] + list(self.config.measurement_columns.keys())
        measurements_df = df.select(measurement_cols).unique(subset=[self.config.id_column, self.config.date_column])

        self.logger.debug(f"Inserting/updating {len(measurements_df)} measurements")
        self.conn.execute(f"INSERT OR REPLACE INTO {self.config.data_table} SELECT * FROM measurements_df")

        self.logger.info("Data insertion complete")

    def close(self) -> None:

        self.conn.close()

def main(
    input_df: pl.DataFrame,
    db_path_str: str = 'ons_cpi.db'
) -> bool:

    try:
        db_path = Path(db_path_str)
        db_path.parent.mkdir(parents = True, exist_ok = True)

        config = TableConfig(
            id_column="item_id",
            date_column="date",
            entity_columns={"item_desc": "VARCHAR"},
            measurement_columns={"item_index": "FLOAT"}
        )
        
        db_manager = DuckDBManager(str(db_path), config, logger)
        db_manager.setup_schema()
        db_manager.insert_data(input_df)
        logger.info(f"Successfully loaded data into {db_path}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to load database: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    main()