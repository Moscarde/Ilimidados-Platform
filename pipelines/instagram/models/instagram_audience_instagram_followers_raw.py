from sqlalchemy import Column, Integer, Date
from models.base import Base

class InstagramAudienceInstagramFollowersRaw(Base):
    __tablename__ = "instagram_audience_instagram_followers_raw"

    id = Column(Integer, primary_key=True, autoincrement=True)
    extraction_date = Column(Date, unique=True, nullable=False)
    followers = Column(Integer, nullable=False)