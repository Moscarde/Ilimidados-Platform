from sqlalchemy import Column, Integer, Date
from models.base import Base

class InstagramVisitsProfileVisitsRaw(Base):
    __tablename__ = "instagram_visits_profile_visits_raw"

    id = Column(Integer, primary_key=True, autoincrement=True)
    extraction_date = Column(Date, nullable=False)
    date = Column(Date, unique=True, nullable=False)
    primary_visits = Column(Integer, nullable=False)
