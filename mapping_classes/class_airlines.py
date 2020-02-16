import sqlalchemy
from sqlalchemy import Column,VARCHAR
from mapping_classes import Base
from sqlalchemy.types import String
class airlines(Base):
	__tablename__ = 'airlines'
	IATA_CODE = Column(String(255),primary_key=True)
	AIRLINE = Column(VARCHAR)
        