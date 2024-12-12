import requests
from bs4 import BeautifulSoup
import os
import time
from tqdm import tqdm

'''
We want to scrape the ONS page for CPI data. Use requests to open the page, then use BeautifulSoup to parse the HTML.
'''

def get_web_data(url: str) -> str:
    """
    Fetch HTML content from given URL.

    Parameters
    ----------
    url : str
        The URL to fetch data from.

    Returns
    -------
    str or None
        HTML content if successful, None if any error occurs.
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

def get_data_links(html, file_types = ['.csv', '.xlsx', '.zip'], search_term = ['upload-itemindices', '/itemindices']):
    """
    Get data links from the HTML content.

    Parameters
    ----------
    html : str
        The HTML content to parse.

    file_types : list of str, optional
        List of file extensions to filter links.

    search_term : list of str, optional
        List of search terms to filter. This is in case the naming convention ever changes.

    Returns
    -------
    list of str
        List of data links found in the HTML content.

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

def download_data(data_links, folder, base_url):
    """
    Download data from given links to the specified folder.

    Parameters
    ----------
    data_links: list of str
        List of relative paths that are appended to the base_url to download the data.

    folder: str
        The folder to save the downloaded data.

    base_url: str
        The base URL to append the relative paths to.
    """
    os.makedirs(folder, exist_ok=True) # exist_ok=True prevents an error if the folder already exists

    # tqdm gives a nice progress bar
    for data_link in tqdm(data_links, desc="Downloading files"):

        filename = os.path.join(folder, data_link.split('/')[-1]) # the .split splits by slash, then gets the last element, which is the filename.

        # Don't want to redownload things we already have, so first check if file exists.
        if os.path.exists(filename):
            print(f"File {filename} already exists, skipping download.")
            continue

        time.sleep(5) # Be nice to the server, don't hammer it with requests.
        response = requests.get(base_url + data_link)
        with open(filename, 'wb') as file:
            file.write(response.content)

def main():

    url = "https://www.ons.gov.uk/economy/inflationandpriceindices/datasets/consumerpriceindicescpiandretailpricesindexrpiitemindicesandpricequotes"

    html = get_web_data(url)
    if html == None:
        print("Failed to get webpage data, exiting.")
        return
    
    data_links = get_data_links(html)

    folder = "data"
    base_url = "https://www.ons.gov.uk"

    download_data(data_links, folder, base_url)

if __name__ == "__main__":
    main()
