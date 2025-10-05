import psycopg2
from config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_database():
    """Add missing columns to existing tables"""
    try:
        conn = psycopg2.connect(
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            dbname=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD
        )
        cursor = conn.cursor()

        logger.info("Starting database migration...")

        # Fix 1: Add updated_at to crawl_statistics if missing
        cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='crawl_statistics' AND column_name='updated_at'
                ) THEN
                    ALTER TABLE crawl_statistics ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                    RAISE NOTICE 'Added updated_at column to crawl_statistics';
                END IF;
            END $$;
        """)

        # Fix 2: Add unique constraint on ssl_info.domain if missing
        cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'ssl_info_domain_key'
                ) THEN
                    ALTER TABLE ssl_info ADD CONSTRAINT ssl_info_domain_key UNIQUE (domain);
                    RAISE NOTICE 'Added unique constraint on ssl_info.domain';
                END IF;
            END $$;
        """)

        # Fix 3: Create unique index if not exists
        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_ssl_info_domain ON ssl_info(domain);
        """)

        # Fix 4: Add updated_at to ssl_info if missing
        cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='ssl_info' AND column_name='updated_at'
                ) THEN
                    ALTER TABLE ssl_info ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                    RAISE NOTICE 'Added updated_at column to ssl_info';
                END IF;
            END $$;
        """)

        conn.commit()
        logger.info("✅ Database migration completed successfully!")

    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        conn.rollback()
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    migrate_database()
