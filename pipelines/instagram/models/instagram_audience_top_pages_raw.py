from sqlalchemy import Column, Integer, Date, String, DECIMAL
from models.base import Base

class InstagramAudienceTopPagesRaw(Base):
    __tablename__ = "instagram_audience_top_pages_raw"

    id = Column(Integer, primary_key=True, autoincrement=True)
    extraction_date = Column(Date, nullable=False)
    page = Column(String(100), nullable=False)
    value_percentage = Column(DECIMAL(5, 2), nullable=False)