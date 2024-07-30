from io import BytesIO
import zipfile
import os
import yaml
import logging
import logging.config
import re
from sqlalchemy import create_engine

def setupLogging():
    with open('log_configuration.yaml','r') as log_config:
        config = yaml.safe_load(log_config)
        logging.config.dictConfig(config)

setupLogging()
logger = logging.getLogger('basic')
logger.info('Logger initiated.')

def setup_db():
    engine = create_engine('sqlite:///ridership_db.db',echo=False)
    return engine

def get_file_info(file_name:str)-> dict:
    if os.path.isfile(file_name): 
        pattern =  r'^(.*)\.(.*)$'
        match = re.match(pattern, file_name)
        if not match: 
            logger.warning(f'No pattern match found for {file_name}.')
            return None
        file_name = match.group(1)
        file_format = match.group(2)
        return {'name':file_name, 'format':file_format}

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
                    file_info = get_file_info(full_path)
                    if file_info:
                        file_list.append(file_info)
        return file_list
    return _extract_files(file, extract_location)
