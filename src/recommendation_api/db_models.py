from sqlalchemy import Column, String, Integer, Float, Date, Text, JSON
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class RecommendationHistory(Base):
    __tablename__ = "recommendation_history"
    
    id = Column(Integer, primary_key=True)
    student_id = Column(String, nullable=False)
    book_id = Column(String, nullable=False)
    recommendation_date = Column(Date, nullable=False)
    query = Column(Text)
    confidence_score = Column(Float)
    book_metadata = Column(JSON)
    justification = Column(Text) 