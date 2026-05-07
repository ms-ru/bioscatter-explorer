# ---------------------------------------------------------------------------- #
#                                    IMPORTS                                   #
# ---------------------------------------------------------------------------- #

from __future__ import annotations

import pandas as pd 

from loguru import logger

# ---------------------------------------------------------------------------- #
#                                    GLOBAL                                    #
# ---------------------------------------------------------------------------- #

NEEDED_COLS_SPECIES = [
    "speciesKey", 
    "family", 
    "genus", 
    "species", 
    "acceptedScientificName", 
    ]
    
NEEDED_COLS_OBSERVATION= [
    "gbifID", 
    "basisOfRecord", 
    "order",
    "decimalLatitude",
    "decimalLongitude",
    "continent",  
    "countryCode",
    "country", 
    "stateProvince",
    "year",
    "month",
    "day",
    "eventDate"   
    ]

REQUIRED_OBSERVATION = [
    "gbifID",
    "basisOfRecord", 
    "order",
    "decimalLatitude",
    "decimalLongitude",
    "continent",
    "countryCode",
    "year",
    "month",
    "eventDate",
    ]

REQUIRED_SPECIES = [
    "family", 
    "acceptedScientificName",
]

# ---------------------------------------------------------------------------- #
#                                    HELPER                                    #
# ---------------------------------------------------------------------------- #

def _build_sub_dataframe(
    df_filtered: pd.DataFrame,
    NEEDED_COLS: list
    )-> pd.DataFrame:
    """
    Helper function which adds missing needed cols as empty cols  
    to the dataframe.

    Args:
        df_raw (pd.DataFrame): gbif results from loading 
        NEEDED_COLS (list): List of all needed columns from gbif results

    Returns:
        pd.Dataframe: Dataframe for each table with only needed cols 
    """
    for col in NEEDED_COLS:
        if col not in df_filtered:
            df_filtered[col] = None
            
    return df_filtered

def _delete_missing_values_within_required_cols(
    df_filtered: pd.DataFrame,
    REQUIRED: list
    ) -> pd.DataFrame:
    """_summary_

    Args:
        df_filtered (pd.DataFrame): _description_
        REQUIRED (list): _description_

    Returns:
        pd.DataFrame: _description_
    """
    before = len(df_filtered)
    df_filtered = df_filtered.dropna(subset=REQUIRED)
    dropped = before - len(df_filtered)
    if dropped > 0:
        logger.info(f"Dropped {dropped} rows with missing required fields")
        
    return df_filtered
            

def _clean_sub_dataframe_species(df_filtered: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans data for species table

    Args:
        df_filtered (pd.DataFrame): df with filtered columns for species table for one chunk 

    Returns:
        pd.DataFrame: cleaned df 
    """
    try:
        if "speciesKey" in df_filtered.columns:
            df_filtered["speciesKey"] = df_filtered["speciesKey"].astype("Int64")

        logger.info(f"Transformation successful. {len(df_filtered)} Rows ready for DB.")
        return df_filtered

    except Exception as e:
        logger.error(f"Error occured during species tranformation: {e}")
        return None
    
def _clean_sub_dataframe_observation(df_filtered: pd.DataFrame) -> pd.DataFrame:
    """_summary_

    Args:
        df_filtered (pd.DataFrame): df with filtered columns for observation table for one chunk 

    Returns:
        pd.DataFrame: cleaned df 
    """
    try:
        # Clean up IDs: Prevents integers from being converted to floats (e.g., 1234.0)
        # Int64 (capital I) allows integers even if values are missing (NaN)
        if "gbifID" in df_filtered.columns:
            df_filtered["gbifID"] = df_filtered["gbifID"].astype("Int64").astype(str)

        # clean date components
        for date_col in ["year", "month", "day"]:
            if date_col in df_filtered.columns:
                df_filtered[date_col] = df_filtered[date_col].astype("Int64")

        # Set coordinates to foat 
        df_filtered["decimalLatitude"] = pd.to_numeric(df_filtered["decimalLatitude"], errors='coerce')
        df_filtered["decimalLongitude"] = pd.to_numeric(df_filtered["decimalLongitude"], errors='coerce')

        logger.info(f"Transformation of observation successful. {len(df_filtered)} Rows ready for DB.")
        return df_filtered

    except Exception as e:
        logger.error(f"Error occured during observation tranformation: {e}")
        return None
    
# ---------------------------------------------------------------------------- #
#                                   TRANSFORM                                  #
# ---------------------------------------------------------------------------- #

def transform_gbif_chunk(raw_results: list) -> pd.DataFrame:
    """
     Transforms raw GBIF chunk data into a cleaned, filtered DataFrame.

    Args:
        raw_results (list): List of dictionaries returned by the loader.

    Returns:
        pd.DataFrame | None: Filtered and cleaned DataFrame or None if empty.
    """
    
    # Break if no data is passed 
    if not raw_results:
        logger.warning(
            "No dictionary was passed by Loader"
        )
        return None
    
    # Transform into pd.Dataframe
    df_raw = pd.DataFrame(raw_results)
    
    # Cut to only needed cols 
    existing_cols_species = [col for col in NEEDED_COLS_SPECIES if col in df_raw.columns]
    existing_cols_observation = [col for col in NEEDED_COLS_OBSERVATION if col in df_raw.columns]
    
    df_species = df_raw[existing_cols_species].copy()
    df_observation = df_raw[existing_cols_observation].copy()
            
    # Setting order as in NEEDED_COLS
    df_filtered_species = df_species[NEEDED_COLS_SPECIES]
    df_filtered_observation = df_observation[NEEDED_COLS_OBSERVATION]
    
    # --------------------------------- Cleaning --------------------------------- #
    
    # Adding needed cols if missing with zeros 
    _build_sub_dataframe(df_filtered_species, NEEDED_COLS_SPECIES)
    _build_sub_dataframe(df_filtered_observation, NEEDED_COLS_OBSERVATION)

    # Deleting cols with zeros within the REQUIRED cols 
    df_filtered_species = _delete_missing_values_within_required_cols(df_filtered_species, REQUIRED_SPECIES)
    df_filtered_observation = _delete_missing_values_within_required_cols(df_filtered_observation, REQUIRED_OBSERVATION)
    
    # tranformation 
    df_filtered_species = _clean_sub_dataframe_species(df_filtered_species)
    df_filtered_observation = _clean_sub_dataframe_observation(df_filtered_observation)
    
    return df_filtered_species, df_filtered_observation
