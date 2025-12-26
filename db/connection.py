import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Load env vars from .env when present
load_dotenv()

# Determine environment
RUNNING_ENV = os.getenv("RUNNING_ENV", "local").lower()

if RUNNING_ENV == "test":
    # Server / test environment
    DB_USER = os.getenv("SN_DATABASE_USER")
    DB_PASSWORD = os.getenv("SN_DATABASE_PASSWORD")
    DB_HOST = os.getenv("SN_DATABASE_HOST")
    DB_NAME = os.getenv("SN_DATABASE_NAME")
    DB_PORT = os.getenv("SN_DATABASE_PORT", "3306")
else:
    # Local defaults
    DB_USER = os.getenv("DB_USER", "root")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "root")
    DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
    DB_NAME = os.getenv("DB_NAME", "filesystem")
    DB_PORT = os.getenv("DB_PORT", "8889")

# Build connection string
DATABASE_URL = "mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(
    user=quote_plus(DB_USER or ""),
    password=quote_plus(DB_PASSWORD or ""),
    host=DB_HOST or "",
    port=DB_PORT or "3306",
    db=DB_NAME or "",
)

# Print for debug (optional)
print(DATABASE_URL, "===========")

# Base class for models
Base = declarative_base()

# Create SQLAlchemy engine
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """Provide a DB session for dependency injection."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Create all tables if they do not exist
try:
    from db import models  # noqa: WPS433

    Base.metadata.create_all(engine)
    print("✅ Ensured all tables exist (created if missing)")
except Exception as exc:  # pragma: no cover
    print(f"❌ Table creation error: {exc}")
