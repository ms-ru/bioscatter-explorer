# ---------------------------------------------------------------------------- #
#                                    IMPORTS                                   #
# ---------------------------------------------------------------------------- #
from __future__ import annotations

import pandas as pd

from backend.python.models.models import Species, Observations
from backend.python.database import SessionLocal, engine

from loguru import logger

# ---------------------------------------------------------------------------- #
#                                    HELPER                                    #
# ---------------------------------------------------------------------------- #

def deduplicate_chunk(
    df: pd.DataFrame, 
    pk_column_name: str, 
    model_column, 
    session
) -> pd.DataFrame:
    """
    Removes rows from the DataFrame that already exist in the database.
    
    Args:
        df (pd.DataFrame): Cleaned DataFrame from transformer
        pk_column_name (str): Name of the primary key column (e.g. "gbifID")
        model_column: SQLAlchemy model column to query (e.g. Observations.gbifID)
        session: Active database session
        
    Returns:
        pd.DataFrame: DataFrame with only new (non-duplicate) entries
    """
    # Get all IDs from the current chunk
    chunk_ids = df[pk_column_name].tolist()
    
    # Ask the DB which of these IDs already exist
    db_result = (
        session
        .query(model_column)
        .filter(model_column.in_(chunk_ids))
        .all()
    )
    
    # db_result returns tuples like [(id1,), (id2,)], 
    # so we extract the first element of each tuple
    existing_ids = [row[0] for row in db_result]
    
    # Keep only rows where the ID is NOT in the database
    df_new = df[~df[pk_column_name].isin(existing_ids)]
    
    if len(existing_ids) > 0:
        logger.info(f"Skipped {len(existing_ids)} duplicates")
    
    return df_new

# ---------------------------------------------------------------------------- #
#                                     WRITE                                    #
# ---------------------------------------------------------------------------- #

def write_species_to_db(df_species: pd.DataFrame) -> None:
    """
    Writes species data to the species table.
    
    Args:
        df_species (pd.DataFrame): Cleaned species DataFrame from transformer
    """
    if df_species is None or df_species.empty:
        logger.warning("No species data to write")
        return
    
    try:
        # Rename "order" to "order_name" to match our database schema
        if "order" in df_species.columns:
            df_species = df_species.rename(columns={"order": "order_name"})
        
        # Open a session to check for duplicates
        with SessionLocal() as session:
            # Remove species that are already in the database
            df_new = deduplicate_chunk(
                df=df_species,
                pk_column_name="speciesKey",
                model_column=Species.speciesKey,
                session=session
            )
            
            if df_new.empty:
                logger.info("All species already exist in DB")
                return
            
            # Write new species to database
            # to_sql() takes a DataFrame and inserts all rows into the table
            # if_exists="append" means: add to existing data, don't replace
            # index=False means: don't write the DataFrame index as a column
            df_new.to_sql(
                name="species",
                con=engine,
                if_exists="append",
                index=False
            )
            
        logger.success(f"{len(df_new)} new species written to DB")
        
    except Exception as e:
        logger.error(f"Error writing species: {e}")


def write_observations_to_db(df_observations: pd.DataFrame) -> None:
    """
    Writes observation data to the observations table.
    
    HOW IT WORKS:
    1. Check for duplicates against existing gbifIDs in database
    2. Write only new observations using pandas to_sql()
    
    Args:
        df_observations (pd.DataFrame): Cleaned observations DataFrame from transformer
    """
    if df_observations is None or df_observations.empty:
        logger.warning("No observation data to write")
        return
    
    try:
        # Open a session to check for duplicates
        with SessionLocal() as session:
            # Remove observations that are already in the database
            df_new = deduplicate_chunk(
                df=df_observations,
                pk_column_name="gbifID",
                model_column=Observations.gbifID,
                session=session
            )
            
            if df_new.empty:
                logger.info("All observations already exist in DB")
                return
            
            # Write new observations to database
            df_new.to_sql(
                name="observations",
                con=engine,
                if_exists="append",
                index=False
            )
            
        logger.success(f"{len(df_new)} new observations written to DB")
        
    except Exception as e:
        logger.error(f"Error writing observations: {e}")
