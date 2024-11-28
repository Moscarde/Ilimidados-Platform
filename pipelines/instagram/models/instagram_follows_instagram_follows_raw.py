from sqlalchemy import Column, Integer, Date
from models.base import Base

class InstagramFollowsInstagramFollowsRaw(Base):
    __tablename__ = "instagram_follows_instagram_follows_raw"

    id = Column(Integer, primary_key=True, autoincrement=True)
    extraction_date = Column(Date, nullable=False)
    date = Column(Date, unique=True, nullable=False)
    primary_followers = Column(Integer, nullable=False)