from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel

Base = declarative_base()


# db model
class User(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "test"}
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    username = Column(String, unique=True, index=True)
    hashed_password = Column(String)


# Pydantic models for input and output validation
class UserResponse(BaseModel):
    username: str


class UserCreate(BaseModel):
    username: str
    password: str


class UserLogin(BaseModel):
    username: str
    password: str


class Token(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str


class TokenRefreshRequest(BaseModel):
    refresh_token: str
