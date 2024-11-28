from sqlalchemy import Column, Integer, Date
from models.base import Base

class InstagramReachReachRaw(Base):
    __tablename__ = "instagram_reach_reach_raw"

    id = Column(Integer, primary_key=True, autoincrement=True)
    extraction_date = Column(Date, nullable=False)
    date = Column(Date, unique=True, nullable=False)
    primary_reach = Column(Integer, nullable=False)