from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, Float, ForeignKey
from typing import Optional

class Base(DeclarativeBase):
    pass 


class Species(Base):
    
    __tablename__ = "species"
    
    # PK
    speciesKey: Mapped[int]= mapped_column(primary_key=True)
    
    # TEXT
    order_name: Mapped[str] = mapped_column(String)
    family: Mapped[str] = mapped_column(String)
    genus: Mapped[Optional[str]] = mapped_column(String)
    species: Mapped[Optional[str]]  = mapped_column(String)
    acceptedScientificName: Mapped[str] = mapped_column(String)
    
    def __repr__(self) -> str:
        return (
            f"Species(id={self.speciesKey!r}, "
            f"name={self.acceptedScientificName!r}"
            )


class Observations(Base):
    
    __tablename__ = "observation"
    
    # PK
    gbifID: Mapped[str] = mapped_column(primary_key=True)
    
    # FK
    species_id: Mapped[int] = mapped_column(ForeignKey("species.speciesKey"))
    
    # INT
    year: Mapped[int] = mapped_column(Integer)
    month: Mapped[int] = mapped_column(Integer)
    day: Mapped[int] = mapped_column(Integer)
    
    # TEXT
    basisOfRecord: Mapped[str] = mapped_column(String) 
    continent: Mapped[str] = mapped_column(String)
    countryCode: Mapped[str] = mapped_column(String)
    country: Mapped[str] = mapped_column(String)
    stateProvince: Mapped[Optional[str]] = mapped_column(String)
    eventDate: Mapped[str] = mapped_column(String)
    
    # DECIMAL 
    decimalLatitude: Mapped[float] = mapped_column(Float)
    decimalLongitude: Mapped[float] = mapped_column(Float)
    
    def __repr__(self) -> str:
        return (
            f"Observation(id={self.gbifID!r}, "
            f"species_id={self.species_id!r}, "
            f"date={self.eventDate!r}, "
            f"location={self.countryCode!r})"
        )
