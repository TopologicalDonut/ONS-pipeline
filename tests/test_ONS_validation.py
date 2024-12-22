import pytest
import polars as pl

from src.processor import process_data, ValidationConfig

@pytest.fixture
def ons_validations():
    return ValidationConfig(
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
                 "Missing description"),
                (lambda col: col.str.strip_chars().str.len_chars() > 0,
                 "Empty description")
            ],
            'item_index': [
                (lambda col: col.is_not_null() & (col >= 0),
                 "Invalid index value")
            ]
        },
        duplicate_check_columns=['date', 'item_id']
    )

def test_date_validation(ons_validations):
    """Test invalid date format detection"""
    df = pl.DataFrame({
        'date': [20230, 202313, None],
        'item_id': [1, 2, 3],
        'item_desc': ['Valid Item A', 'Valid Item B', 'Valid Item C'],
        'item_index': [100.0, 200.0, 300.0]
    })
    clean_df, results = process_data(df, ons_validations, None)
    assert "Missing date" in results.invalid_rows
    assert "Invalid date format" in results.invalid_rows
    assert "Invalid month" in results.invalid_rows
    assert clean_df.height == 0

def test_item_id_validation(ons_validations):
    """Test null item_id detection"""
    df = pl.DataFrame({
        'date': [202301],
        'item_id': [None],
        'item_desc': ['Valid Item'],
        'item_index': [100.0]
    })
    clean_df, results = process_data(df, ons_validations, None)
    assert results.invalid_rows["Missing item ID"] == 1
    assert clean_df.height == 0

def test_empty_description_validation(ons_validations):

        df = pl.DataFrame({
            'date': [202301, 202301, 202301],
            'item_id': [1, 1, 1],
            'item_desc': ['', '  ', None],
            'item_index': [100.0, 100.0, 100.0]
        })
        clean_df, results = process_data(df, ons_validations, None)

        assert results.invalid_rows["Missing description"] == 1
        assert results.invalid_rows["Empty description"] == 2
        assert clean_df.height == 0

def test_invalid_index_validation(ons_validations):
    """Test invalid index detection"""
    test_cases = [
        (-1.0, "Negative value"),
        (None, "Null value")
    ]
    
    for idx, case in test_cases:
        df = pl.DataFrame({
            'date': [202301],
            'item_id': [1],
            'item_desc': ['Valid Item'],
            'item_index': [idx]
        })
        clean_df, results = process_data(df, ons_validations, None)
        assert results.invalid_rows["Invalid index value"] == 1, f"Failed for case: {case}"
        assert clean_df.height == 0

def test_valid_data(ons_validations):
    """Test that valid data passes all validations"""
    df = pl.DataFrame({
        'date': [202301],
        'item_id': [1],
        'item_desc': ['Valid Item'],
        'item_index': [100.0]
    })
    clean_df, results = process_data(df, ons_validations, None)
    assert results.total_rows == 1
    assert results.rows_retained == 1
    assert len(results.invalid_rows) == 0
    assert clean_df.height == 1

def test_duplicate_handling(ons_validations):
    """Test duplicate row handling"""
    duplicate_df = pl.DataFrame({
        'date': [202301, 202301, 202301, 202301],
        'item_id': [1, 1, 2, 2],  # Two sets of duplicates
        'item_desc': ['Item A', 'Item A dupe', 'Item B', 'Item B dupe'],
        'item_index': [100.0, 101.0, 200.0, 201.0]
    })
    clean_df, results = process_data(duplicate_df, ons_validations, None)
    
    assert clean_df.height == 2  # Should keep one of each unique date-item combo
    assert results.invalid_rows["Duplicate entries"] == 2
    
    # Verify we kept the first occurrence of each duplicate set
    retained_rows = clean_df.sort(['date', 'item_id'])
    assert retained_rows['item_desc'][0] == 'Item A'
    assert retained_rows['item_index'][0] == 100.0
    assert retained_rows['item_desc'][1] == 'Item B'
    assert retained_rows['item_index'][1] == 200.0