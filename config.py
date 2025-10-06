import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Database configuration
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/crawler_db')

    # Redis configuration (still used for progress tracking)
    REDIS_URL = os.getenv('REDIS_URI', 'redis://localhost:6379/0')

    # Connection pool settings
    DB_POOL_MIN_CONN = int(os.getenv('DB_POOL_MIN_CONN', '2'))
    DB_POOL_MAX_CONN = int(os.getenv('DB_POOL_MAX_CONN', '10'))
    CLOUDINARY_CLOUD_NAME = os.getenv('CLOUDINARY_CLOUD_NAME', '')
    CLOUDINARY_API_KEY = os.getenv('CLOUDINARY_API_KEY', '')
    CLOUDINARY_API_SECRET = os.getenv('CLOUDINARY_API_SECRET', '')
    @classmethod
    def validate(cls):
        """Validate that required configuration is present"""
        if not cls.DATABASE_URL:
            raise ValueError("DATABASE_URL must be set")
        if not cls.REDIS_URL:
            raise ValueError("REDIS_URL must be set")

        # Warn about optional Cloudinary config
        if not cls.CLOUDINARY_CLOUD_NAME:
            print("⚠ Warning: CLOUDINARY_CLOUD_NAME not set - asset uploads will be disabled")

        print("✓ Configuration validated successfully")
