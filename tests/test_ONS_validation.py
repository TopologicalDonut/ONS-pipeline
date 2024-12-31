import pytest
import polars as pl
from pathlib import Path
from typing import Generator

from src.processor import Processor, ProcessorConfig, ProcessingResults

@pytest.fixture
def test_config() -> ProcessorConfig:
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
                (lambda col: col.is_not_null() & (col > 0), "Invalid index value")
            ]
        },
        duplicate_check_columns=['date', 'item_id']
    )

@pytest.fixture
def processor(test_config: ProcessorConfig) -> Processor:
    return Processor(test_config)

@pytest.fixture
def test_dir(tmp_path: Path) -> Generator[Path, None, None]:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    yield data_dir

def test_date_validation(processor: Processor):
    """Test invalid date format detection"""
    df = pl.DataFrame({
        'INDEX_DATE': [20230, 202313, None],
        'ITEM_ID': [1, 2, 3],
        'ITEM_DESC': ['Valid Item A', 'Valid Item B', 'Valid Item C'],
        'ALL_GM_INDEX': [100.0, 200.0, 300.0]
    }).lazy()
    
    df = processor._standardize_columns(df)
    results = ProcessingResults()
    final_df = processor.validate_data(df, results)
    assert final_df.height == 0  # All rows should be invalid

def test_item_id_validation(processor: Processor):
    """Test null item_id detection"""
    df = pl.DataFrame({
        'INDEX_DATE': [202301],
        'ITEM_ID': [None],
        'ITEM_DESC': ['Valid Item'],
        'ALL_GM_INDEX': [100.0]
    }).lazy()
    
    df = processor._standardize_columns(df)
    results = ProcessingResults()
    final_df = processor.validate_data(df, results)
    assert final_df.height == 0  # All rows should be invalid

def test_empty_description_validation(processor: Processor):
    """Test empty description detection"""
    df = pl.DataFrame({
        'INDEX_DATE': [202301, 202301, 202301],
        'ITEM_ID': [1, 2, 3],
        'ITEM_DESC': ['', '  ', None],
        'ALL_GM_INDEX': [100.0, 100.0, 100.0]
    }).lazy()
    
    df = processor._standardize_columns(df)
    results = ProcessingResults()
    final_df = processor.validate_data(df, results)
    assert final_df.height == 0  # All rows should be invalid

def test_invalid_index_validation(processor: Processor):
    """Test invalid index detection"""
    test_cases = [
        (0.0, "Non-positive value"),
        (None, "Null value")
    ]
    
    for idx, case in test_cases:
        df = pl.DataFrame({
            'INDEX_DATE': [202301],
            'ITEM_ID': [1],
            'ITEM_DESC': ['Valid Item'],
            'ALL_GM_INDEX': [idx]
        }).lazy()
        
        df = processor._standardize_columns(df)
        results = ProcessingResults()
        final_df = processor.validate_data(df, results)
        assert final_df.height == 0, f"Failed for case: {case}"

def test_valid_data(processor: Processor):
    """Test that valid data passes all validations"""
    df = pl.DataFrame({
        'INDEX_DATE': [202301],
        'ITEM_ID': [1],
        'ITEM_DESC': ['Valid Item'],
        'ALL_GM_INDEX': [100.0]
    }).lazy()
    
    df = processor._standardize_columns(df)
    results = ProcessingResults()
    final_df = processor.validate_data(df, results)
    assert final_df.height == 1
    assert isinstance(final_df, pl.DataFrame)  # Should return an eager DataFrame

def test_duplicate_handling(processor: Processor):
    """Test duplicate row handling"""
    df = pl.DataFrame({
        'INDEX_DATE': [202301, 202301, 202301, 202301],
        'ITEM_ID': [1, 1, 2, 2],  # Two sets of duplicates
        'ITEM_DESC': ['Item A', 'Item A dupe', 'Item B', 'Item B dupe'],
        'ALL_GM_INDEX': [100.0, 101.0, 200.0, 201.0]
    }).lazy()
    
    df = processor._standardize_columns(df)
    results = ProcessingResults()
    final_df = processor.validate_data(df, results)
    assert final_df.height == 2  # Should keep one of each unique date-item combo
    
    # Verify we kept the first occurrence of each duplicate set
    retained_rows = final_df.sort(['date', 'item_id'])
    assert retained_rows['item_desc'][0] == 'Item A'
    assert retained_rows['item_index'][0] == 100.0
    assert retained_rows['item_desc'][1] == 'Item B'
    assert retained_rows['item_index'][1] == 200.0