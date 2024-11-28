from sqlalchemy import Column, BigInteger, Integer, Text, Date, String, TIMESTAMP
from models.base import Base

class InstagramContentPostDataRaw(Base):
    __tablename__ = "instagram_content_post_data_raw"

    post_id = Column(BigInteger, primary_key=True, unique=True, nullable=False)
    ig_account_id = Column(Integer)
    description = Column(Text)
    duration_sec = Column(Integer)
    publish_time = Column(TIMESTAMP)
    permalink = Column(String(255))
    post_type = Column(String(100))
    data_comment = Column(Text)
    date = Column(Date)
    impressions = Column(Integer)
    reach = Column(Integer)
    likes = Column(Integer)
    shares = Column(Integer)
    follows = Column(Integer)
    comments = Column(Integer)
    saves = Column(Integer)