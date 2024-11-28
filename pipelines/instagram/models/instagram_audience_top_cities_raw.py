from sqlalchemy import Column, Integer, Date, String, DECIMAL
from models.base import Base

class InstagramAudienceTopCitiesRaw(Base):
    __tablename__ = "instagram_audience_top_cities_raw"

    id = Column(Integer, primary_key=True, autoincrement=True)
    extraction_date = Column(Date, nullable=False)
    city = Column(String(255), nullable=False)
    value_percentage = Column(DECIMAL(5, 2), nullable=False)