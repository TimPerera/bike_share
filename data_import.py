
import requests
import pandas as pd
import re
import os
import numpy as np

from utils import logger, extract_files, setup_db

OUTPUT_PATH = 'imported_data' # Where to store downloaded data

def download_ridership_data(data_package, required_files = []):
    """
    Imports ridership data. Returns a list of all the resources names imported.
    """
    errors = 0
    downloaded_files = []

    for resource in data_package['result']['resources']:
        link = resource['url']
        response = requests.get(link, stream=True)
        
        file_name = resource['name']
        file_format = resource['format'].lower()
        # bool tests that acts as filters
        match_found = any([re.search(x, file_name) is not None for x in required_files])
        
        # if required_files is defined but curr resource does not appear in this list then skip
        if required_files and not match_found:
            continue

        # else download, because either:
        # required_files has been defined, but the current resource is not what we are looking for
        # OR the required_files is not defined, in which case we want to download the current file

        file_path = os.path.join(OUTPUT_PATH,file_name)
        
        if file_format == 'zip':
            downloaded_files.extend(extract_files(response.content, file_path))

        elif file_format in ['xlsx','xls']:
            with open(file_path+'.xlsx','wb') as file:
                file.write(response.content)
                downloaded_files.append({'name':file_name, 'format':file_format})
        else:
            logger.debug(f'Missing file handler for type {file_format}')
            errors += 1 
    #data_file_len = len(data_package['result']['resources'])
    #logger.info(f'Downloaded {data_file_len-errors} files. {errors} errors detected during download.')
            
    return downloaded_files

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

def validate_data(df:pd.DataFrame):
    """Fix common data issues with dataframe that has been downloaded. 
    For now, this includes fixing the column names to follow the standard, and fix the 
    inconsistency observed in the timestamp. 
    """
    df.columns = df.columns.str.lower()
    df = df.rename(columns={'trip_id':'trip id',
                       'trip_start_time': 'start time',
                       'trip_stop_time':'end time',
                       'trip_duration_seconds':'trip duration',
                       'from_station_name':'start station name',
                       'to_station_name':'end station name',
                       'user_type':'user type'
                       })
    #logger.debug(df.loc[:5, 'start time'])
    try:
        
        df['start time'] = pd.to_datetime(df['start time'])
        df['end time'] = pd.to_datetime(df['end time'])
        mask_start_station_name_null = df['start station name'].upper()=='NULL'
        mask_end_station_name_null = df['end station name'].upper()=='NULL'
        mask_start_station_id = df['start station id'].upper()=='NULL'
        mask_end_station_id = df['end station id'].upper()=='NULL'
        non_int_mask_end_station = df['end station id'].apply(lambda x: np.isnan(x))
        non_int_mask_start_station = df['start station id'].apply(lambda x: np.isnan(x))
        
        combined_mask = (mask_start_station_name_null|
                         mask_end_station_name_null|
                         mask_start_station_id|
                         mask_end_station_id|
                         non_int_mask_end_station|
                         non_int_mask_start_station)
        
        df = df[~combined_mask]

        df = df.dropna(subset=['start station name',
                          'end station name', 
                          'start station id',
                          'end station id'], 
                  how='all')
        
        df['end station id'] = df['end station id'].astype(int)
        df['start station id'] = df['start station id'].astype(int)

    except Exception as e:
        logger.error(f'Problem detected when applying column corrections. {e}')
    return df


def consolidate_ridership_data(downloaded_files:list):
    #TODO: consolidate column names
    # Loop through all folders and files and get all file names first
    #TODO: Figure out how to deal with different columns across different files
    # when consolidating files 
    df_list = list()
    bad_files = ['readme'] # do not download list
    bad_formats = ['docx']
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
                   'to_station_name',
                   'user_type'
                   ]
    for file in downloaded_files:
        file_name = file['name']
        file_format = file['format'].lower()
        bad_file_found = file_name in bad_files
        bad_format_found = file_format in bad_formats
        if any([bad_file_found, bad_format_found]):
            continue
        if not file_name.startswith(OUTPUT_PATH): 
            full_fname = os.path.join(OUTPUT_PATH, file_name +'.' + file_format)
        else:
            full_fname = full_fname = file_name + '.' + file_format
        # logger.debug(f'full_fname:{full_fname}')

        if file_format in ['xlsx','xls','csv']:
            if file_format == 'csv':
                try: # Some files have differnt encoding types.
                    df = pd.read_csv(full_fname, encoding='utf-8', usecols=lambda x: x.lower() in search_cols)
                except UnicodeDecodeError:
                    df = pd.read_csv(full_fname, encoding='ISO-8859-1', usecols=lambda x: x.lower() in search_cols)
            elif file_format in ['xlsx','xls']:
                excel_file = pd.ExcelFile(full_fname)
                sheets = excel_file.sheet_names
                _df = []
                for sheet in sheets:
                    _df.append(excel_file.parse(sheet, usecols=lambda x: x.lower() in search_cols))
                    logger.debug(_df)
                df = pd.concat(_df)
            df_list.append(df)
        else:
            if os.path.isdir(full_fname):
                logger.debug(f'Ignoring {full_fname} as it is a directory.')
            else:
                logger.debug(f"Ignoring {full_fname} as it is an unexpected format: {file['format']}. Check file extension.")
    master_df = pd.concat(df_list)
    master_df_validated = validate_data(master_df)
    # Traverse through all files and consolidate into one master sheet
    logger.info('Consolidation complete.')
    return master_df_validated

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
    required_files = ['2022','2023'] # limits the files that are downloaded, acts as a filter.
    rider_data_pkg = get_data_package(rdata_url, params)
    station_data_pkg = get_data_package(sdata_url)
    file_names = download_ridership_data(rider_data_pkg, required_files=required_files)
    download_station_data(station_data_pkg)

    # Create master ridership dataset
    master_df = consolidate_ridership_data(file_names)
    logger.debug(f'Number of rows {len(master_df)}')
    # logger.debug(master_df.info())

    # Upload to sql db
    con = setup_db()
    try:
        master_df.to_sql('ridership_data', con=con, chunksize=10000, if_exists='replace')
    except Exception as e:
        logger.error(e)
    

if __name__=='__main__':
    data = main()


# TODO: file 2016 is causing problems when uploadiing to sql db. Check timestamp