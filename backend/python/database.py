from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend.python.models.models import Base

# Engine 
DATABASE_URL = "sqlite:///db/bioscatter.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

# SessionMaker
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Initialize DB
def init_db():
    Base.metadata.create_all(bind=engine)
    
    