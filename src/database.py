import duckdb
import polars as pl
from dataclasses import dataclass
import logging
from pathlib import Path

from src.const import PATH_CONFIG

logger = logging.getLogger(__name__)

type SQLType = str

DB_NAME = 'ons_cpi.db'

@dataclass(frozen=True)
class TableConfig:
    id_column: str
    date_column: str
    entity_columns: dict[str, SQLType]    
    measurement_columns: dict[str, SQLType]
    entity_table: str = "items"
    data_table: str = "cpi_data"
    float_tolerance: float = 1e-3 

    def get_comparison(self, col: str, dtype: str, table1: str, table2: str) -> str:
        """This is to compare whether values of two tables are equal or not with a tolerance for floats"""

        if dtype.upper().startswith('FLOAT'):

            return f"ABS({table1}.{col} - {table2}.{col}) > {self.float_tolerance}"
        
        return f"{table1}.{col} != {table2}.{col}"

TABLE_CONFIG = TableConfig(
    id_column = "item_id",
    date_column = "date",
    entity_columns = {"item_desc": "VARCHAR"},
    measurement_columns = {"item_index": "FLOAT"}
)

class DuckDBManager:
    def __init__(self, db_path: str, config: TableConfig, logger: logging.Logger):

        self.conn = duckdb.connect(db_path)
        self.config = config
        self.logger = logger
        
        # Check which tables exist before creation
        tables_before = set(self.conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name IN (?, ?)
        """, [self.config.entity_table, self.config.data_table]).fetchall())

        tables_before = {t[0] for t in tables_before}

    def setup_schema(self, force_recreate: bool = False) -> None:
        """
        Set up the database schema.
        
        Parameters
        ----------
        force_recreate : bool
            If True, drops and recreates tables. If False, only creates if they don't exist.
        """

        self.logger.info("Creating database schema")
        
        # Check which tables exist before creation
        tables_before = set(self.conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name IN (?, ?)
        """, [self.config.entity_table, self.config.data_table]).fetchall())

        tables_before = {t[0] for t in tables_before}       

        if force_recreate:
            self.logger.warning("Force recreating tables - all existing data will be lost")
            self.conn.execute(f"DROP TABLE IF EXISTS {self.config.data_table}")
            self.conn.execute(f"DROP TABLE IF EXISTS {self.config.entity_table}")
        
        entity_cols = [
            f'{self.config.id_column} VARCHAR PRIMARY KEY'
        ] + [
            f'{name} {dtype}' for name, dtype in self.config.entity_columns.items()
        ]
        
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
        
        # Check which tables exist after creation
        tables_after = set(self.conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name IN (?, ?)
        """, [self.config.entity_table, self.config.data_table]).fetchall())
        
        tables_after = {t[0] for t in tables_after}
        
        new_tables = tables_after - tables_before
        if new_tables:
            self.logger.info(f"Created new tables: {', '.join(new_tables)}")
        else:
            self.logger.info("No new tables created - all tables already exist")
    
    def insert_data(self, df: pl.DataFrame) -> tuple[int, int]:
        """Insert data and return counts of inserted/updated records."""

        self.logger.info("Starting data insertion")
        
        # Insert entities
        entity_cols = [self.config.id_column] + list(self.config.entity_columns.keys())
        entities_df = df.select(entity_cols).unique(subset=[self.config.id_column])
        
        value_comparisons = [
            self.config.get_comparison(col, dtype, 'e', 'n') for col, dtype in self.config.entity_columns.items()
        ]

        replaced_entities = self.conn.execute(f"""
            SELECT COUNT(*) FROM {self.config.entity_table} e
            INNER JOIN entities_df n ON e.{self.config.id_column} = n.{self.config.id_column}
            WHERE { ' OR '.join(value_comparisons) }
        """).fetchone()[0]

        entities_before = self.conn.execute(f"SELECT COUNT(*) FROM {self.config.entity_table}").fetchone()[0]
        self.conn.execute(f"INSERT OR REPLACE INTO {self.config.entity_table} SELECT * FROM entities_df")
        entities_after = self.conn.execute(f"SELECT COUNT(*) FROM {self.config.entity_table}").fetchone()[0]
        
        # Insert measurements
        measurement_cols = [self.config.date_column, self.config.id_column] + list(self.config.measurement_columns.keys())
        measurements_df = df.select(measurement_cols).unique(subset=[self.config.id_column, self.config.date_column])
        
        value_comparisons = [
            self.config.get_comparison(col, dtype, 'd', 'n') for col, dtype in self.config.measurement_columns.items()
        ]

        replaced_measurements = self.conn.execute(f"""
            SELECT COUNT(*) FROM {self.config.data_table} d
            INNER JOIN measurements_df n 
            ON d.{self.config.date_column} = n.{self.config.date_column}
            AND d.{self.config.id_column} = n.{self.config.id_column}
            WHERE {' OR '.join(value_comparisons)}
        """).fetchone()[0]

        measurements_before = self.conn.execute(f"SELECT COUNT(*) FROM {self.config.data_table}").fetchone()[0]
        self.conn.execute(f"INSERT OR REPLACE INTO {self.config.data_table} SELECT * FROM measurements_df")
        measurements_after = self.conn.execute(f"SELECT COUNT(*) FROM {self.config.data_table}").fetchone()[0]
        
        self.logger.info(
            f"Processed: {entities_after - entities_before} new entities ({replaced_entities} updated), "
            f"{measurements_after - measurements_before} new measurements ({replaced_measurements} updated)"
        )

        return None
    
    def preview_tables(self, limit: int = 5) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Preview both tables."""
        entity_preview = self.conn.execute(
            f"SELECT * FROM {self.config.entity_table} LIMIT {limit}"
        ).pl()
        
        data_preview = self.conn.execute(
            f"SELECT * FROM {self.config.data_table} LIMIT {limit}"
        ).pl()
        
        return entity_preview, data_preview

    def get_table_stats(self) -> dict:
        """Get statistics about the tables."""

        stats = {}
        
        # Get row counts
        stats['entity_count'] = self.conn.execute(
            f"SELECT COUNT(*) FROM {self.config.entity_table}"
        ).fetchone()[0]
        
        stats['measurement_count'] = self.conn.execute(
            f"SELECT COUNT(*) FROM {self.config.data_table}"
        ).fetchone()[0]
        
        # Get date range
        if stats['measurement_count'] > 0:
            date_range = self.conn.execute(f"""
                SELECT 
                    MIN({self.config.date_column}) as min_date,
                    MAX({self.config.date_column}) as max_date
                FROM {self.config.data_table}
            """).fetchone()
            
            stats['date_range'] = (date_range[0], date_range[1])
        
        return stats
    
    def close(self) -> None:

        self.conn.close()


def main(input_df: pl.DataFrame, db_dir: Path = PATH_CONFIG.DB_DIR, db_name: str = DB_NAME) -> bool:

    try:
        db_path = db_dir / db_name
        db_dir.mkdir(parents=True, exist_ok=True)
        
        db_manager = DuckDBManager(str(db_path), TABLE_CONFIG, logger)
        db_manager.setup_schema()
        
        db_manager.insert_data(input_df)
        
        entity_preview, data_preview = db_manager.preview_tables()
        stats = db_manager.get_table_stats()
        
        logger.info("Table Previews:")
        logger.info("\nEntities Table:")
        logger.info(entity_preview)
        logger.info("\nMeasurements Table:")
        logger.info(data_preview)
        
        logger.info("\nDatabase Statistics:")
        logger.info(f"Total entities: {stats['entity_count']}")
        logger.info(f"Total measurements: {stats['measurement_count']}")
        if 'date_range' in stats:
            logger.info(f"Date range: {stats['date_range'][0]} to {stats['date_range'][1]}")
        
        db_manager.close()
        logger.info(f"Successfully loaded data into {db_path}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to load database: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    main()