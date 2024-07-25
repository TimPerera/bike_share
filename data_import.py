import logging.config
import requests
import pandas as pd
import zipfile
from io import BytesIO
import re
import os
import logging
from dotenv import load_dotenv
import yaml

load_dotenv()

def setupLogging():
    with open('log_configuration.yaml','r') as log_config:
        config = yaml.safe_load(log_config)
        logging.config.dictConfig(config)

setupLogging()
logger = logging.getLogger('basic')
logger.info('Logger initiated.')


OUTPUT_PATH = 'imported_data'

def import_ridership_data(data_package):
    """
    Imports ridership data. Returns a list of all the file names imported.
    """
    for resource in data_package['result']['resources']:
        link = resource['url']
        format = resource['format'].lower()
        
        response = requests.get(link, stream=True)
        
        pattern = '^.*/download/([^/]+)$'
        match = re.match(pattern, link)
        file_name = match[1]
        file_path = os.path.join(OUTPUT_PATH,file_name)

        if format == 'zip':
            resource
            zip_file_content = BytesIO(response.content)
            with zipfile.ZipFile(zip_file_content) as zip_ref:
                zip_ref.extractall(file_path)

        elif format == 'xlsx':
            with open(file_path,'wb') as file:
                file.write(response.content)

        else:
            print('Missing file handler for type',format)


def import_station_data(station_data_pkg):
    station_data = station_data_pkg['data']['stations']
    pd.DataFrame.from_dict(station_data).to_csv(OUTPUT_PATH+'/station_info.csv')

def get_data_package(url, params=None):
    "Handles URL requests. Returns JSON object."
    
    try:
        response = requests.get(url, params=params)
    except Exception as e:
        raise(e)
    return response.json()

def main():

    # Ensure that the file path exists before we start importing data
    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)
        print('Created data directory.')
    
    # Supply URLs to extract data from
    rdata_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show"
    params = {"id": "bike-share-toronto-ridership-data"}
    sdata_url = "https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_information"

    # Import data
    rider_data_pkg = get_data_package(rdata_url, params)
    station_data_pkg = get_data_package(sdata_url)
    import_ridership_data(rider_data_pkg)
    import_station_data(station_data_pkg)

    # Create master ridership dataset
    
        

if __name__=='__main__':
    main()