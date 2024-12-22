import duckdb
import polars as pl
from dataclasses import dataclass
from datetime import date
from typing import Protocol

from src.logger import setup_logger

logger = setup_logger(__name__)

# Custom type to make classes and functions more self-evident.
type SQLType = str
type QueryResult = pl.DataFrame

# This isn't strictly needed for the pipeline, but here for possible future changes where a different db is used.
class DatabaseHandler(Protocol):
    def execute(self, query: str) -> QueryResult: ...
    def close(self) -> None: ...

@dataclass(frozen = True)
class TableConfig:
    """Configuration for database table structure and column definitions.
    
    Entity and measurement columns are for naming and declaring variable types aside from the 
    primary keys.
    """
    id_column: str
    date_column: str
    entity_columns: dict[str, SQLType]    
    measurement_columns: dict[str, SQLType]
    
    entity_table: str = "items"
    data_table: str = "cpi_data"

class DuckDBHandler:
    def __init__(self, db_path: str):
        self.conn = duckdb.connect(db_path)

    def execute(self, query: str) -> QueryResult:
        return self.conn.execute(query).pl()
    
    def close(self) -> None:
        self.conn.close()

class DataManager:
    def __init__(self, db: DatabaseHandler, config: TableConfig):
        self.db = db
        self.config = config

    def setup_schema(self) -> None:
        """Creates or verifies the required database tables."""
        entity_cols = [
            f'{self.config.id_column} VARCHAR PRIMARY KEY',
            *[f'{col} {dtype}' for col, dtype in self.config.entity_columns.items()]
        ]

        self.db.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.config.entity_table} (
                {', '.join(entity_cols)}
            )
        """)

        data_cols = [
            f'{self.config.date_column} DATE',
            f'{self.config.id_column} VARCHAR',
            *[f'{col} {dtype}' for col, dtype in self.config.measurement_columns.items()]
        ]

        self.db.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.config.data_table} (
                {', '.join(data_cols)},
                PRIMARY KEY ({self.config.date_column}, {self.config.id_column}),
                FOREIGN KEY ({self.config.id_column})
                    REFERENCES {self.config.entity_table}({self.config.id_column})
            )
        """)

    def insert_data(self, df: pl.DataFrame) -> tuple[int, int]:
        """Updates database with new data, returning counts of affected rows.
        
        Returns
        -------
        entities_updated
            Number of entity records inserted or replaced
        measurements_updated
            Number of measurement records inserted or replaced
        """

        entity_cols = [self.config.id_column] + list[self.config.entity_columns.keys()]
        entities_df = df.select(entity_cols).unique(subset = self.config.id_column)

def setup_database(conn: duckdb.DuckDBPyConnection, config: TableConfig) -> None:
    """Create database tables based on provided configuration.
    
    Creates two tables:
    1. An entity table with static information about each item
    2. A measurements table with time series data linked to entities
    
    Parameters
    ----------
    conn
        DuckDB database connection
    config
        Configuration specifying table structure
    
    Raises
    ------
    duckdb.Error
        If table creation fails
    """
    # Create entity table
    entity_cols = [
        f"{config.id_column} VARCHAR PRIMARY KEY",
        *[f"{col} {dtype}" for col, dtype in config.entity_columns.items()]
    ]
    
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {config.entity_table} (
            {', '.join(entity_cols)}
        )
    """)
    
    data_cols = [
        f"{config.date_column} DATE", 
        f"{config.id_column} VARCHAR",
        *[f"{col} {dtype}" for col, dtype in config.measurement_columns.items()]
    ]
    
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {config.data_table} (
            {', '.join(data_cols)},
            PRIMARY KEY ({config.date_column}, {config.id_column}),
            FOREIGN KEY ({config.id_column}) REFERENCES {config.entity_table}({config.id_column})
        )
    """)
    
    logger.info(f"Created/verified tables: {config.entity_table} and {config.data_table}")

def insert_data(
    conn: duckdb.DuckDBPyConnection, 
    config: TableConfig,
    df: pl.DataFrame
) -> tuple[int, int]:
    """Insert or update data in both entity and measurements tables.
    
    Parameters
    ----------
    conn
        DuckDB database connection
    config
        Configuration specifying table structure
    df
        Polars DataFrame containing both entity and measurement data
    
    Returns
    -------
    entities_updated : int
        Number of entity records updated
    measurements_updated : int
        Number of measurement records updated
    
    Raises
    ------
    ValueError
        If required columns are missing from the DataFrame
    """
    logger.info("Starting data insertion")
    
    # Update entities table
    entity_cols = [config.id_column] + list(config.entity_columns.keys())
    entities_df = df.select(entity_cols).unique(subset=[config.id_column])
    
    entities_before = conn.execute(f"SELECT COUNT(*) FROM {config.entity_table}").fetchone()[0]
    
    conn.execute(f"""
        INSERT OR REPLACE INTO {config.entity_table} 
        SELECT {', '.join(entity_cols)} FROM entities_df
    """)
    
    entities_after = conn.execute(f"SELECT COUNT(*) FROM {config.entity_table}").fetchone()[0]
    entities_updated = entities_after - entities_before
    
    # Update measurements table
    measurement_cols = [config.date_column, config.id_column] + list(config.measurement_columns.keys())
    measurements_df = df.select(measurement_cols)
    
    measurements_before = conn.execute(f"SELECT COUNT(*) FROM {config.data_table}").fetchone()[0]
    
    conn.execute(f"""
        INSERT OR REPLACE INTO {config.data_table}
        SELECT {', '.join(measurement_cols)} FROM measurements_df
    """)
    
    measurements_after = conn.execute(f"SELECT COUNT(*) FROM {config.data_table}").fetchone()[0]
    measurements_updated = measurements_after - measurements_before
    
    logger.info(f"Updated {entities_updated} entities and {measurements_updated} measurements")
    return entities_updated, measurements_updated