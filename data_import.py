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
from sqlalchemy import create_engine

def setupLogging():
    with open('log_configuration.yaml','r') as log_config:
        config = yaml.safe_load(log_config)
        logging.config.dictConfig(config)

setupLogging()
logger = logging.getLogger('basic')
logger.info('Logger initiated.')

OUTPUT_PATH = 'imported_data' # Where to store downloaded data


def extract_files(file, extract_location):
    
    file_list = []

    def _extract_files(file, extract_location):
        # Recursively loop through each file
        if isinstance(file, bytes): 
            file_contents = BytesIO(file)
        else:
            file_contents = os.path.join(extract_location, file)
        with zipfile.ZipFile(file_contents) as zipref:
            zipref.extractall(extract_location)
            extracted_files = zipref.namelist()

            for efile in extracted_files:
                full_path = os.path.join(extract_location, efile)
                if efile.endswith('.zip'): # zipfile found in unzipped file.
                    try:
                        file_list.extend(_extract_files(efile, extract_location))
                    except:
                        logging.error(efile)
                else:
                    file_list.append(full_path)
        return file_list
    return _extract_files(file, extract_location)



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
            file_names.extend(extract_files(response.content, file_path))

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
    #TODO: consolidate column names
    # Loop through all folders and files and get all file names first
    #TODO: Figure out how to deal with different columns across different files
    # when consolidating files 
    # Also figure out whether you need to loop through sheets in a particular book

    df_list = list()
    bad_files = ['readme','-2014-2015'] # add file names here to exclude them from final df
    search_cols = ['trip id', 
                   'trip  duration', 
                   'start station id',	
                   'start time', 
                   'start station name', 
                   'end station id', 
                   'end time', 
                   'end station name', 
                   'bike id', 
                   'user type',
                   'trip_id',
                   'trip_start_time',
                   'trip_stop_time'	,
                   'trip_duration_seconds',
                   'from_station_name',	
                   'to_station_name	user_type']
    for file_name in file_name_list:
        if not file_name.startswith(OUTPUT_PATH): file_name = os.path.join(OUTPUT_PATH, file_name)
        if file_name.endswith(('.xlsx','.xls','.csv')):
            if  any([string in file_name for string in bad_files]):
                continue 
            elif file_name.endswith('.csv'):
                try: # Some files have differnt encoding types.
                    df = pd.read_csv(file_name, encoding='utf-8', usecols=lambda x: x in search_cols)
                except UnicodeDecodeError:
                    df = pd.read_csv(file_name, encoding='ISO-8859-1', usecols=lambda x: x.lower() in search_cols)
            elif file_name.endswith(('.xlsx','.xls')):
                excel_file = pd.ExcelFile(file_name)
                sheets = excel_file.sheet_names
                _df = []
                for sheet in sheets:
                    _df.append(excel_file.parse(sheet, usecols=lambda x: x.lower() in search_cols))
                df = pd.concat(_df)
            df_list.append(df)
        else:
            logger.debug(f'Ignoring {file_name}')
    logger.debug(f'df_list len: {len(df_list)}')
    master_df = pd.concat(df_list)
    # Traverse through all files and consolidate into one master sheet
    logger.debug('Consolidation complete.')
    return master_df

def setup_db():
    engine = create_engine('sqlite:///ridership_db.db',echo=False)
    return engine

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
    master_df = consolidate_ridership_data(file_names)
    logger.debug(f'Columns: {master_df.columns.to_list()}')
    logger.debug(f'Number of rows {len(master_df)}')
    logger.debug(master_df.info())
    logger.debug(master_df.head(15))
    #master_df.to_csv('final_df.csv')   

    # Upload to sql db
    con = setup_db()

    master_df.to_sql('ridership_data', con=con, chunksize=10000)

if __name__=='__main__':
    main()