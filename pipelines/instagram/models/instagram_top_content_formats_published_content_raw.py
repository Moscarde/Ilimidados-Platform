from sqlalchemy import Column, Integer, Date
from models.base import Base

class InstagramTopContentFormatsPublishedContentRaw(Base):
    __tablename__ = "instagram_top_content_formats_published_content_raw"

    id = Column(Integer, primary_key=True, autoincrement=True)
    extraction_date = Column(Date, unique=True, nullable=False)
    stories = Column(Integer, nullable=False)
    posts = Column(Integer, nullable=False)
