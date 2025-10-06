import logging
from datetime import datetime
from database import DatabaseConfig
from psycopg2.extras import execute_batch

logger = logging.getLogger(__name__)


class DatabaseOperations:
    datetime = datetime  # Make datetime accessible for app.py

    @staticmethod
    def initialize_database():
        """Initialize database tables if they don't exist"""
        conn = None
        try:
            conn = DatabaseConfig.get_connection()
            cursor = conn.cursor()

            # Crawl logs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS crawl_logs (
                    id SERIAL PRIMARY KEY,
                    domain TEXT NOT NULL,
                    message TEXT NOT NULL,
                    level TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_crawl_logs_domain ON crawl_logs(domain)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_crawl_logs_timestamp ON crawl_logs(timestamp)")

            # Crawled pages table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS crawled_pages (
                    id SERIAL PRIMARY KEY,
                    url TEXT UNIQUE NOT NULL,
                    domain TEXT NOT NULL,
                    title TEXT,
                    content TEXT,
                    status_code INTEGER,
                    content_type TEXT,
                    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_crawled_pages_domain ON crawled_pages(domain)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_crawled_pages_url ON crawled_pages(url)")

            # Page metadata table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS page_metadata (
                    id SERIAL PRIMARY KEY,
                    page_id INTEGER REFERENCES crawled_pages(id) ON DELETE CASCADE,
                    meta_key TEXT NOT NULL,
                    meta_value TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_page_metadata_page_id ON page_metadata(page_id)")

            # Assets table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS assets (
                    id SERIAL PRIMARY KEY,
                    page_id INTEGER REFERENCES crawled_pages(id) ON DELETE CASCADE,
                    type TEXT NOT NULL,
                    url TEXT NOT NULL,
                    content BYTEA,
                    file_size INTEGER,
                    cloud_url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_assets_page_id ON assets(page_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_assets_type ON assets(type)")

            # Articles table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS articles (
                    id SERIAL PRIMARY KEY,
                    page_id INTEGER REFERENCES crawled_pages(id) ON DELETE CASCADE,
                    headline TEXT,
                    author TEXT,
                    published_date TIMESTAMP,
                    article_text TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_page_id ON articles(page_id)")

            # Crawl statistics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS crawl_statistics (
                    id SERIAL PRIMARY KEY,
                    domain TEXT UNIQUE NOT NULL,
                    total_pages INTEGER DEFAULT 0,
                    total_assets INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_crawl_statistics_domain ON crawl_statistics(domain)")

            # SSL info table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ssl_info (
                    id SERIAL PRIMARY KEY,
                    domain TEXT UNIQUE NOT NULL,
                    provider TEXT,
                    expiry_date TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_ssl_info_domain ON ssl_info(domain)")

            conn.commit()
            logger.info("Database schema initialized successfully (preserving existing data)")

        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            if conn:
                conn.rollback()
            raise

        finally:
            if conn:
                cursor.close()
                DatabaseConfig.release_connection(conn)

    @staticmethod
    def insert_ssl_info(domain, provider, expiry_date):
        """Insert or update SSL information for a domain"""
        conn = None
        try:
            conn = DatabaseConfig.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO ssl_info (domain, provider, expiry_date, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (domain) DO UPDATE SET
                    provider = EXCLUDED.provider,
                    expiry_date = EXCLUDED.expiry_date,
                    updated_at = CURRENT_TIMESTAMP
            """, (domain, provider, expiry_date))

            conn.commit()
            logger.info(f"SSL info saved for domain: {domain}")
            return True

        except Exception as e:
            logger.error(f"SSL insert failed for {domain}: {e}")
            if conn:
                conn.rollback()
            return False

        finally:
            if conn:
                cursor.close()
                DatabaseConfig.release_connection(conn)

    @staticmethod
    def insert_page(url, domain, title=None, content=None, status_code=None, content_type=None):
        """Insert or update a crawled page"""
        conn = None
        try:
            conn = DatabaseConfig.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO crawled_pages (url, domain, title, content, status_code, content_type)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO UPDATE SET
                    title = EXCLUDED.title,
                    content = EXCLUDED.content,
                    status_code = EXCLUDED.status_code,
                    content_type = EXCLUDED.content_type,
                    crawled_at = CURRENT_TIMESTAMP
                RETURNING id
            """, (url, domain, title, content, status_code, content_type))

            result = cursor.fetchone()
            conn.commit()
            page_id = result[0] if result else None
            logger.info(f"Page inserted/updated: {url} (ID: {page_id})")
            return page_id

        except Exception as e:
            logger.error(f"Failed to insert page {url}: {e}")
            if conn:
                conn.rollback()
            return None

        finally:
            if conn:
                cursor.close()
                DatabaseConfig.release_connection(conn)

    @staticmethod
    def insert_metadata_batch(page_id, metadata_dict):
        """Insert metadata for a page in batch"""
        if not metadata_dict:
            return

        conn = None
        try:
            conn = DatabaseConfig.get_connection()
            cursor = conn.cursor()

            data = [(page_id, key, str(value)[:1000]) for key, value in metadata_dict.items()]
            execute_batch(cursor, """
                INSERT INTO page_metadata (page_id, meta_key, meta_value)
                VALUES (%s, %s, %s)
            """, data, page_size=100)

            conn.commit()
            logger.info(f"Inserted {len(data)} metadata entries for page_id: {page_id}")

        except Exception as e:
            logger.error(f"Failed to insert metadata batch: {e}")
            if conn:
                conn.rollback()

        finally:
            if conn:
                cursor.close()
                DatabaseConfig.release_connection(conn)

    @staticmethod
    def insert_asset_batch(page_id, assets):
        """Insert assets for a page in batch"""
        if not assets:
            return

        conn = None
        try:
            conn = DatabaseConfig.get_connection()
            cursor = conn.cursor()

            data = [
                (page_id, asset['type'], asset['url'], asset.get('content'),
                 asset.get('file_size'), asset.get('cloud_url'))
                for asset in assets
            ]

            execute_batch(cursor, """
                INSERT INTO assets (page_id, type, url, content, file_size, cloud_url)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, data, page_size=50)

            conn.commit()
            logger.info(f"Inserted {len(data)} assets for page_id: {page_id}")

        except Exception as e:
            logger.error(f"Failed to insert asset batch: {e}")
            if conn:
                conn.rollback()

        finally:
            if conn:
                cursor.close()
                DatabaseConfig.release_connection(conn)

    @staticmethod
    def insert_article(page_id, headline, author, published_date, article_text):
        """Insert article content"""
        if not headline and not article_text:
            return

        conn = None
        try:
            conn = DatabaseConfig.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO articles (page_id, headline, author, published_date, article_text)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (page_id, headline, author, published_date, article_text))

            conn.commit()
            logger.info(f"Article inserted for page_id: {page_id}")

        except Exception as e:
            logger.error(f"Failed to insert article: {e}")
            if conn:
                conn.rollback()

        finally:
            if conn:
                cursor.close()
                DatabaseConfig.release_connection(conn)

    @staticmethod
    def insert_log(domain, message, level):
        """Insert a log entry"""
        conn = None
        try:
            conn = DatabaseConfig.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO crawl_logs (domain, message, level)
                VALUES (%s, %s, %s)
            """, (domain, str(message)[:2000], level))

            conn.commit()

        except Exception as e:
            logger.error(f"Failed to insert log: {e}")
            if conn:
                conn.rollback()

        finally:
            if conn:
                cursor.close()
                DatabaseConfig.release_connection(conn)

    @staticmethod
    def update_crawl_statistics(domain, total_pages=None, total_assets=None, status=None,
                                start_time=None, end_time=None):
        """Update crawl statistics for a domain"""
        conn = None
        try:
            conn = DatabaseConfig.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO crawl_statistics (domain, total_pages, total_assets, status, start_time, end_time, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (domain) DO UPDATE SET
                    total_pages = COALESCE(EXCLUDED.total_pages, crawl_statistics.total_pages),
                    total_assets = COALESCE(EXCLUDED.total_assets, crawl_statistics.total_assets),
                    status = COALESCE(EXCLUDED.status, crawl_statistics.status),
                    start_time = COALESCE(EXCLUDED.start_time, crawl_statistics.start_time),
                    end_time = COALESCE(EXCLUDED.end_time, crawl_statistics.end_time),
                    updated_at = CURRENT_TIMESTAMP
            """, (domain, total_pages, total_assets, status, start_time, end_time))

            conn.commit()
            logger.info(f"Statistics updated for domain: {domain}")

        except Exception as e:
            logger.error(f"Failed to update statistics: {e}")
            if conn:
                conn.rollback()

        finally:
            if conn:
                cursor.close()
                DatabaseConfig.release_connection(conn)

    @staticmethod
    def get_crawl_statistics(domain):
        """Get crawl statistics for a domain"""
        conn = None
        try:
            conn = DatabaseConfig.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT id, domain, total_pages, total_assets, status, start_time, end_time, updated_at
                FROM crawl_statistics
                WHERE domain = %s
            """, (domain,))

            return cursor.fetchone()

        except Exception as e:
            logger.error(f"Get statistics failed for {domain}: {e}")
            return None

        finally:
            if conn:
                cursor.close()
                DatabaseConfig.release_connection(conn)

    @staticmethod
    def get_recent_logs(domain, limit=50):
        """Get recent logs for a domain"""
        conn = None
        try:
            conn = DatabaseConfig.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT timestamp, level, message FROM crawl_logs
                WHERE domain = %s ORDER BY timestamp DESC LIMIT %s
            """, (domain, limit))

            return cursor.fetchall()

        except Exception as e:
            logger.error(f"Failed to get logs for {domain}: {e}")
            return []

        finally:
            if conn:
                cursor.close()
                DatabaseConfig.release_connection(conn)
