import logging.config
import requests
import pandas as pd
import zipfile
from io import BytesIO
import re
import os
import logging
import yaml
import openpyxl

def setupLogging():
    with open('log_configuration.yaml','r') as log_config:
        config = yaml.safe_load(log_config)
        logging.config.dictConfig(config)

setupLogging()
logger = logging.getLogger('basic')
logger.info('Logger initiated.')

OUTPUT_PATH = 'imported_data' # Where to store downloaded data

def extract_zip(file, extract_to):
    """Recursive function to extract zip documents."""
    file_names = []

    def _extract_zip(file, current_path):
        nonlocal file_names
        # 
        if isinstance(file, bytes):
            file_contents = BytesIO(file)
        else:
            file_contents = os.path.join(current_path, file)
            # logger.debug(file_contents)

        with zipfile.ZipFile(file_contents) as zipref:
            zipref.extractall(current_path)
            extracted_files = zipref.namelist()
            for efile in extracted_files:
                full_path = os.path.join(current_path, efile)
                if efile.endswith('.zip'):
                    try:
                        file_names.extend(_extract_zip(efile, current_path))
                    except TypeError:
                        logger.warning(f'Error encountered unzipping {efile}')
                else:
                    file_names.append(full_path)
        return file_names

    return _extract_zip(file, extract_to)


def download_ridership_data(data_package):
    """
    Imports ridership data. Returns a list of all the file names imported.
    """
    errors = 0
    file_names = []
    for resource in data_package['result']['resources']:
        link = resource['url']
        format = resource['format'].lower()
        
        response = requests.get(link, stream=True)
        
        pattern = '^.*/download/([^/]+)$'
        match = re.match(pattern, link)
        file_name = match[1]
        file_path = os.path.join(OUTPUT_PATH,file_name)

        if format == 'zip':
            file_names.extend(extract_zip(response.content, file_path))

        elif format == 'xlsx':
            with open(file_path,'wb') as file:
                file.write(response.content)
                file_names.append(file_name)

        else:
            print('Missing file handler for type',format)
            errors += 1 
    data_file_len = len(data_package['result']['resources'])
    logger.info(f'Downloaded {data_file_len-errors} files. {errors} errors detected during download.')
    return file_names

def download_station_data(station_data_pkg):
    """
    Downloads information on bicycle stations (payment types accepted, number of stands)
    """
    station_data = station_data_pkg['data']['stations'] # a list of dictionaries
    pd.DataFrame.from_dict(station_data).to_csv(OUTPUT_PATH+'/station_info.csv')

def get_data_package(url, params=None):
    "Handles URL requests. Returns JSON object."
    
    try:
        response = requests.get(url, params=params)
    except Exception as e:
        raise(e)
    return response.json()

# def fetch_files(file_list):
#     for file in file_list:
#         if file


def consolidate_ridership_data(file_name_list:list):
    # Loop through all folders and files and get all file names first
    df_list = list()
    master_df = pd.DataFrame()
    for file_name in file_name_list:
        if not file_name.startswith(OUTPUT_PATH): file_name = os.path.join(OUTPUT_PATH, file_name)
        if file_name.endswith(('.xlsx','.xls','.csv')):
            if file_name.endswith('.csv'):
                logger.debug(file_name)
                try:
                    df = pd.read_csv(file_name, encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(file_name, encoding='ISO-8859-1')
                df_list.append(df)
            elif file_name.endswith(('.xlsx','.xls')):
                df_list.append(pd.read_excel(file_name))
        else:
            logger.debug(f'Ignoring {file_name}')
    logger.debug(f'df_list len: {len(df_list)}')
    # Traverse through all files and consolidate into one master sheet
    pass

def main():
    """
    Executes the flow of the main script. Downloads data, then consolidates datasets.
    """
    # Ensure that the file path exists before we start importing data
    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)
        logger.info('Created data directory.')
    
    # Supply URLs to extract data from
    rdata_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show"
    params = {"id": "bike-share-toronto-ridership-data"}
    sdata_url = "https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_information"

    # Import data
    rider_data_pkg = get_data_package(rdata_url, params)
    station_data_pkg = get_data_package(sdata_url)
    file_names = download_ridership_data(rider_data_pkg)
    download_station_data(station_data_pkg)

    # Create master ridership dataset
    consolidate_ridership_data(file_names)   

if __name__=='__main__':
    main()