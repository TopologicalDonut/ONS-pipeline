import zipfile
from pathlib import Path
import polars as pl
from typing import Optional
import datetime

"""
This script is for processing the ONS data so that it's ready for storing in the database.

There's four main things to get from the data:
    1. Date
    2. Item ID
    3. Item Description
    4. Item Index (ALL_GM_INDEX)

Considerations:
    - Need to extract data from the zip files without redundant extraction.
    - Processing data:
        - Date is stored as YYYYMM, so need to convert to a proper format for the database.
        - There's some errors/extraneous symbols in the data that need to be dealt with (e.g. a '*' in the date column).
"""

def extract_zip_files(
    data_folder: Path, 
    target_folder: Path = Path("data/extracted_files"),
) -> bool:
    '''
    Extracts the zip files in the data folder. 
    
    Notes
    -----
    Maintains a record of previously extracted files to avoid re-extraction of
    unchanged data. Re-extracts entire zip if any contained file has changed.
    '''
    target_folder.mkdir(parents=True, exist_ok=True)
    extracted_files = _load_extracted_files_list(target_folder)
    
    try:
        for zip_path in data_folder.glob('*.zip'):
            _process_single_zip(zip_path, target_folder, extracted_files)
        _save_extracted_files_list(extracted_files, target_folder)        
        return True
    except Exception as e:
        print(f'Error extracting zip files: {e}')
        return False

def _load_extracted_files_list(target_folder: Path) -> set[str]:
    """
    Load the list of extracted files from the extracted_files.txt file.
    """
    extracted_files_path = target_folder / 'extracted_files.txt'
    if extracted_files_path.exists():
        return set(extracted_files_path.read_text().splitlines())
    return set()

def _save_extracted_files_list(extracted_files: set[str], target_folder: Path) -> None:
    """
    Save the list of extracted files to the extracted_files.txt file.
    """
    extracted_files_path = target_folder / 'extracted_files.txt'
    extracted_files_path.write_text('\n'.join(sorted(extracted_files)))

def _process_single_zip(
    zip_path: Path, 
    target_folder: Path,
    extracted_files: set[str],
) -> None:
    """
    Extract single zip file if it has new content. Updates the extracted_files set for each extracted zip.
    """
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_contents = set(zip_ref.namelist())
        if not zip_contents.issubset(extracted_files):
            print(f'Extracting {zip_path}')
            zip_ref.extractall(target_folder)
            extracted_files.update(zip_contents)
        else:
            print(f'No new files in {zip_path}')
        
def process_all_files(data_folder: Path) -> pl.DataFrame:
    '''
    Reads all the data from the data folder to create one cleaned dataframe.
    
    Notes
    -----
    The data is read from the csv and xlsx files in the data folder.
    '''
    data = pl.DataFrame()
    
    for filepath in data_folder.rglob('*'):
        if filepath.suffix.lower() in ('.csv', '.xlsx'):
            print(f'Reading {filepath}')
            new_data = _read_data(filepath)
            if new_data is not None:
                data = data.vstack(new_data)

    return data

def _read_data(file_path: Path) -> Optional[pl.DataFrame]:
    """
    Read a single ONS data file into a DataFrame.
    """
    try:
        if file_path.suffix.lower() == ".csv":
            return pl.read_csv(file_path)
        return pl.read_excel(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def _process_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans the ONS data by extracting the columns we want and ensuring they are in
    the right format.
    """
    return(df
        .select([
             'INDEX_DATE',
             'ITEM_ID',
             'ITEM_DESC',
             'ALL_GM_INDEX'
        ])
        .with_columns([
            pl.col('INDEX_DATE')
                .str.replace('[^0-9]', '')
                .map_elements(lambda x: 
                    datetime.strptime(x, '%Y%m').date()
                    if len(x) == 6 and 1 <= int(x[4:]) <= 12
                    else None
                )
                .alias('date'),

            pl.col('ITEM_ID')
                .str.strip()
                .map_elements()
        ])
    )


def main():

    data_folder = Path('data')
    
    if extract_zip_files(data_folder):
        print('Zip files extracted successfully')
    else:
        print('Error extracting zip files')

    print((pl.read_csv('data/upload-202005itemindices.csv', n_rows=5).schema))

if __name__ == '__main__':
    main()