import duckdb
import polars as pl
from pathlib import Path

from src.database import TableConfig, setup_database, insert_data
"""
Despite its name, this script does not use pytest. It's just a basic script to make and insert a sample database in the same format as the ONS data to ensure that the database.py script works 
as intended.
"""

def create_test_data():

    data = {
        'item_id': ['A001', 'A001', 'A001', 'A001',
                   'B002', 'B002', 'B002', 'B002',
                   'C003', 'C003', 'C003', 'C003'],
        'item_desc': ['Apples', 'Apples', 'Apples', 'Apples',
                     'Bananas', 'Bananas', 'Bananas', 'Bananas',
                     'Carrots', 'Carrots', 'Carrots', 'Carrots'],
        'date': [202301, 202302, 202303, 202304] * 3,
        'item_index': [100.0, 102.5, 103.8, 105.2,
                      100.0, 101.8, 103.2, 104.5,
                      100.0, 100.9, 101.5, 102.8]
    }
    
    return pl.DataFrame(data)

def main():
    """Creates a fresh test database each time it's run.
    
    The script overwrites any existing test.duckdb file without warning.
    """
    config = TableConfig(
        id_column="item_id",
        date_column="date",
        entity_columns={"item_desc": "VARCHAR NOT NULL"},
        measurement_columns={"item_index": "DOUBLE NOT NULL"}
    )
    
    db_path = Path('test.duckdb')
    conn = duckdb.connect(str(db_path))
    
    try:
        setup_database(conn, config)
        
        test_df = create_test_data()
        entities, measurements = insert_data(conn, config, test_df)
        
        print(f"\nInserted {entities} entities and {measurements} measurements")
        
        print("\nEntities (items):")
        print(conn.execute("""
            SELECT * FROM items
            ORDER BY item_id
        """).pl())
        
        print("\nMeasurements (by date):")
        print(conn.execute("""
            SELECT 
                date,
                item_id,
                item_index
            FROM cpi_data
            ORDER BY date, item_id
        """).pl())
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()