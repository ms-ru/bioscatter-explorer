
-- Create table species 
CREATE TABLE species (

    speciesKey INTEGER PRIMARY KEY, 

    order_name TEXT NOT NULL,

    family TEXT NOT NULL, 

    genus TEXT,

    species TEXT,

    acceptedScientificName TEXT NOT NULL
);

CREATE TABLE observation (

    -- a unique ID for each entry 
    gbifID TEXT PRIMARY KEY NOT NULL,

    speciesKey INTEGER, 

    basisOfRecord TEXT NOT NULL, 

    decimalLatitude REAL NOT NULL, 

    decimalLongitude REAL NOT NULL,

    continent TEXT NOT NULL, 

    countryCode TEXT NOT NULL, 

    country TEXT NOT NULL,

    stateProvince TEXT,

    year INTEGER NOT NULL, 

    month INTEGER NOT NULL, 

    day INTEGER NOT NULL,

    eventDate TEXT NOT NULL,

FOREIGN KEY (speciesKey) REFERENCES species(speciesKey)
);
