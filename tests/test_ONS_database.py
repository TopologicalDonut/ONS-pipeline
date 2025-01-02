import pytest
import polars as pl
from src.database import DuckDBManager, TableConfig
import logging
from datetime import date

@pytest.fixture
def table_config():
    return TableConfig(
        id_column="item_id",
        date_column="date",
        entity_columns={"name": "VARCHAR", "category": "VARCHAR"},
        measurement_columns={"price": "FLOAT", "quantity": "INTEGER"}
    )

@pytest.fixture
def sample_data():
    return pl.DataFrame({
        "item_id": ["A001", "A002", "A001", "A002"],
        "date": ["2023-01-01", "2023-01-01", "2023-01-02", "2023-01-02"],
        "name": ["Apple", "Banana", "Apple", "Banana"],
        "category": ["Fruit", "Fruit", "Fruit", "Fruit"],
        "price": [1.0, 0.5, 1.1, 0.6],
        "quantity": [100, 150, 90, 140]
    })

@pytest.fixture
def db_manager(table_config):
    manager = DuckDBManager(":memory:", table_config)
    manager.setup_schema()
    yield manager
    manager.close()

def test_setup_schema(db_manager):
    result = db_manager.conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
    table_names = [row[0] for row in result]
    assert "items" in table_names
    assert "cpi_data" in table_names

def test_insert_data(db_manager, sample_data):
    db_manager.insert_data(sample_data)
    
    items = db_manager.conn.execute("SELECT * FROM items ORDER BY item_id").fetchall()
    assert len(items) == 2
    assert dict(zip(['item_id', 'name', 'category'], items[0])) == {'item_id': 'A001', 'name': 'Apple', 'category': 'Fruit'}
    assert dict(zip(['item_id', 'name', 'category'], items[1])) == {'item_id': 'A002', 'name': 'Banana', 'category': 'Fruit'}

    cpi_data = db_manager.conn.execute("SELECT * FROM cpi_data ORDER BY date, item_id").fetchall()
    assert len(cpi_data) == 4
    
    expected_cpi_data = [
        {'date': date(2023, 1, 1), 'item_id': 'A001', 'price': 1.0, 'quantity': 100},
        {'date': date(2023, 1, 1), 'item_id': 'A002', 'price': 0.5, 'quantity': 150},
        {'date': date(2023, 1, 2), 'item_id': 'A001', 'price': 1.1, 'quantity': 90},
        {'date': date(2023, 1, 2), 'item_id': 'A002', 'price': 0.6, 'quantity': 140}
    ]
    
    for actual, expected in zip(cpi_data, expected_cpi_data):
        actual_dict = dict(zip(['date', 'item_id', 'price', 'quantity'], actual))
        assert actual_dict['date'] == expected['date']
        assert actual_dict['item_id'] == expected['item_id']
        assert actual_dict['price'] == pytest.approx(expected['price'], rel=1e-5)
        assert actual_dict['quantity'] == expected['quantity']

def test_unique_constraint(db_manager, sample_data):
    db_manager.insert_data(sample_data)
    
    duplicate_data = pl.DataFrame({
        "item_id": ["A001"],
        "date": ["2023-01-01"],
        "name": ["Apple"],
        "category": ["Fruit"],
        "price": [2.0],
        "quantity": [200]
    })
    db_manager.insert_data(duplicate_data)

    cpi_data = db_manager.conn.execute("SELECT * FROM cpi_data WHERE item_id='A001' AND date='2023-01-01'").fetchall()
    assert len(cpi_data) == 1
    actual_dict = dict(zip(['date', 'item_id', 'price', 'quantity'], cpi_data[0]))
    assert actual_dict['date'] == date(2023, 1, 1)
    assert actual_dict['item_id'] == 'A001'
    assert actual_dict['price'] == pytest.approx(2.0, rel=1e-5)
    assert actual_dict['quantity'] == 200

def test_foreign_key_constraint(db_manager):
    invalid_data = pl.DataFrame({
        "item_id": ["A003"],
        "date": ["2023-01-01"],
        "price": [1.0],
        "quantity": [100]
    })
    with pytest.raises(Exception):
        db_manager.insert_data(invalid_data)