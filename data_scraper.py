import requests
from bs4 import BeautifulSoup
from pathlib import Path
import zipfile
import time
from tqdm import tqdm

'''
We want to scrape the ONS page for CPI data. Use requests to open the page, then use BeautifulSoup to parse the HTML.
'''

def get_web_data(url: str) -> str | None:
    """
    Fetch HTML content from given URL.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:", errh)
        return None
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:", errc)
        return None
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:", errt)
        return None
    except requests.exceptions.RequestException as err:
        print ("Oops: Something Else", err)
        return None
    
    html = response.text
    return html

def get_data_links(
    html: str, 
    file_types: list[str] = ['.csv', '.xlsx', '.zip'], 
    search_term: list[str] = ['upload-itemindices', '/itemindices']
) -> list[str]:
    """
    Get data links from the HTML content.

    Notes
    -----
    The search terms are passed through the any() function.
    """
    soup = BeautifulSoup(html, 'html.parser')
    links = soup.find_all('a')

    # Since the links we want are the datasets that contain 'upload-itemindices' or '/itemindices', we can filter as such:
    data_links = [link.get('href') for link in links 
                  if any(term in link.get('href') for term in search_term)
                  and link.get('href').endswith(tuple(file_types))] 

    return data_links

def download_and_extract_data(
    data_links: list[str], 
    base_url: str,
    folder: Path = Path('data'), 
) -> None:
    """
    Download data from given links to the specified folder, extract any zips,
    and clean up the zip files while maintaining extraction history.
    """
    download_folder = folder
    extract_folder = download_folder / 'extracted_files'
    download_folder.mkdir(exist_ok=True)
    extract_folder.mkdir(exist_ok=True)
    
    processed_zips_log = extract_folder / 'processed_zips.txt'
    if processed_zips_log.exists():
        processed_zips = set(processed_zips_log.read_text().splitlines())
    else:
        processed_zips = set()

    # Track what happened to print summary at the end
    stats = {
        'zips_skipped': 0,
        'zips_processed': 0,
        'files_skipped': 0,
        'files_downloaded': 0
    }

    for data_link in tqdm(data_links, desc="Processing files"):
        filename = Path(data_link).name
        file_path = download_folder / filename
        
        if filename.endswith('.zip'):
            if filename in processed_zips:
                stats['zips_skipped'] += 1
                continue
                
            time.sleep(2)
            response = requests.get(base_url + data_link)
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            with zipfile.ZipFile(file_path) as zf:
                zf.extractall(extract_folder)
            
            processed_zips.add(filename)
            processed_zips_log.write_text('\n'.join(sorted(processed_zips)))
            file_path.unlink()
            stats['zips_processed'] += 1
            
        else:
            if file_path.exists():
                stats['files_skipped'] += 1
                continue
                
            time.sleep(2)
            response = requests.get(base_url + data_link)
            with open(file_path, 'wb') as f:
                f.write(response.content)
            stats['files_downloaded'] += 1

    # Show summary at the end
    print("\nDownload Summary:")
    print(f"Zip files: {stats['zips_processed']} processed, {stats['zips_skipped']} skipped")
    print(f"Other files: {stats['files_downloaded']} downloaded, {stats['files_skipped']} skipped")

def main():
    url = "https://www.ons.gov.uk/economy/inflationandpriceindices/datasets/consumerpriceindicescpiandretailpricesindexrpiitemindicesandpricequotes"

    html = get_web_data(url)
    if html == None:
        print("Failed to get webpage data, exiting.")
        return
    
    data_links = get_data_links(html)

    folder = Path('data')
    base_url = "https://www.ons.gov.uk"

    download_and_extract_data(data_links, base_url, folder)

if __name__ == "__main__":
    main()