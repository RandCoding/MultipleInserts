import sqlalchemy
from sqlalchemy import Column,NUMERIC,VARCHAR
from mapping_classes import Base
from sqlalchemy.types import String
class airports(Base):
	__tablename__ = 'airports'
	IATA_CODE = Column(VARCHAR)
	AIRPORT = Column(String(255), primary_key=True)
	CITY = Column(VARCHAR)
	STATE = Column(VARCHAR)
	COUNTRY = Column(VARCHAR)
	LATITUDE = Column(NUMERIC)
	LONGITUDE = Column(NUMERIC)
        