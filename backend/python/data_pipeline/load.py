# ---------------------------------------------------------------------------- #
#                                    IMPORTS                                   #
# ---------------------------------------------------------------------------- #
from __future__ import annotations

import requests 

from loguru import logger

# ---------------------------------------------------------------------------- #
#                                    LOGGING                                   #
# ---------------------------------------------------------------------------- #

logger.add("gbif_pipeline.log", 
           format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", 
           level="INFO", 
           rotation="2 MB", 
           retention="10 days")

# ---------------------------------------------------------------------------- #
#                                    GLOBAL                                    #
# ---------------------------------------------------------------------------- #

# Gbif Keys for potential important species  
TARGET_KEYS = {
    # definetly flying
    "Lepidoptera": 797,
    "Diptera": 811,
    "Odonata": 789,
    # some relatives are flying (edge cases)
    "Hymenoptera": 1457,
    "Hemiptera": 809
}

# TaxonKey 216 = Insects (in general), Country = Germany, Limit = 300 (max rate per Request)
GBIF_URL_BASE = (
    "https://api.gbif.org/v1/occurrence/search?taxonKey=216&country=DE&limit=300"
    )

# TaxonKey = variable (to get already filtered list, Country Germany, Limit = variable
GBIF_URL_TARGET = (
    "https://api.gbif.org/v1/occurrence/search?taxonKey={key}&country=DE&limit={limit}&offset={offset}"
    )

# ---------------------------------------------------------------------------- #
#                                    LOADER                                    #
# ---------------------------------------------------------------------------- #

def load_gbif_data_filtered_chunked(name, key, offset):
    """
    Loads exactly one chunk (limit defined by gbif)
    for one key-name pair.

    Args:
        name (str): name of genus (gbif)
        key (int): key for genus given by gbif
        offset (int): starting position for pagination
    """
    # variables given by gbif api  
    limit = 300
    
    # url for given key-name pair 
    current_url = GBIF_URL_TARGET.format(key=key, limit=limit, offset=offset)
    
    logger.info(f"Fetching {name} | Offset: {offset}")
    
    try: 
        # set request
        current_response = requests.get(current_url, timeout=10)
        
        # check if data is accessible 
        if current_response.status_code != 200: 
            logger.error(
                f"Error with Name: {name} / Key: {key}"
                )
            # return None for data and True for cancellation
            return None, True
        
        # assigning data 
        data = current_response.json()
        
        # extract data
        results = data.get("results", [])
        
        # get information on counts and if all observations are loaded
        is_end = data.get("endOfRecords", False)
        total_count = data.get("count", 0)
        
        logger.info(
            f"Loaded {name} / Offset: {offset}, Total available: {total_count}"
            )
        
        return results, is_end
        
    except Exception as e:
        logger.error(f"Critical error with request ({name}): {e}")
        return None, True
