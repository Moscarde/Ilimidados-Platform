from sqlalchemy import Column, Integer, Date, String, DECIMAL
from models.base import Base

class InstagramAudienceDemographicsRaw(Base):
    __tablename__ = "instagram_audience_demographics_raw"

    id = Column(Integer, primary_key=True, autoincrement=True)
    extraction_date = Column(Date, nullable=False)
    age = Column(String(10), nullable=False)
    women_percentage = Column(DECIMAL(5, 2), nullable=False)
    men_percentage = Column(DECIMAL(5, 2), nullable=False)