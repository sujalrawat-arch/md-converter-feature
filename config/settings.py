import os
from dotenv import load_dotenv
import urllib.parse
# Load environment variables
load_dotenv()

class Settings:
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    AUTH_BASE_URL = os.getenv("AUTH_BASE_URL", "http://localhost:6501/auth")
    
    # Identity for this AI Microservice (used to authenticate itself)
    SERVICE_ID = os.getenv("SERVICE_ID")
    SERVICE_SECRET = os.getenv("SERVICE_SECRET")
    
    # Base URL for this service (useful for self-referencing or logs)
    AI_SERVICE_BASE_URL = os.getenv("AI_SERVICE_BASE_URL", "http://localhost:8000")

    # ----------------------------------------------------------------
    # 3. JWKS & TOKEN VALIDATION (For Middleware)
    # ----------------------------------------------------------------
    # Used by the SDK to verify incoming tokens locally
    SERVICE_JWKS_URL = os.getenv("SERVICE_JWKS_URL", f"{AUTH_BASE_URL}/.well-known/jwks.json")
    SERVICE_JWT_ISSUER = os.getenv("SERVICE_JWT_ISSUER", "auth-service")
    SERVICE_JWT_AUDIENCE = os.getenv("SERVICE_JWT_AUDIENCE", "itmtb-internal")

    # ----------------------------------------------------------------
    # 4. AWS CONFIGURATION (For S3, Textract, etc.)
    # ----------------------------------------------------------------
    AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    SECRET_NAME = os.getenv("SECRET_NAME")

    # Timeout for HTTP calls to other services
    AUTH_TIMEOUT = int(os.getenv("AUTH_TIMEOUT", "5"))

    @property
    def DATABASE_URL(self):
        password = urllib.parse.quote_plus(self.DB_PASSWORD)
        return f"mysql+pymysql://{self.DB_USER}:{password}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


settings = Settings()
