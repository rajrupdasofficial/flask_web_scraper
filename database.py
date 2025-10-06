import psycopg2
from psycopg2 import pool
from threading import Lock
import logging
from config import Config
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class DatabaseConfig:
    _pool = None
    _lock = Lock()

    @staticmethod
    def parse_database_url(database_url):
        """Parse DATABASE_URL into connection parameters"""
        try:
            parsed = urlparse(database_url)
            return {
                'host': parsed.hostname,
                'port': parsed.port or 5432,
                'dbname': parsed.path[1:] if parsed.path else 'postgres',
                'user': parsed.username,
                'password': parsed.password
            }
        except Exception as e:
            logger.error(f"Failed to parse DATABASE_URL: {e}")
            raise ValueError(f"Invalid DATABASE_URL format: {e}")

    @staticmethod
    def initialize_pool():
        """Initialize the connection pool from DATABASE_URL"""
        with DatabaseConfig._lock:
            if DatabaseConfig._pool is None:
                try:
                    Config.validate()

                    # Parse DATABASE_URL
                    db_params = DatabaseConfig.parse_database_url(Config.DATABASE_URL)

                    # Create connection pool
                    DatabaseConfig._pool = pool.SimpleConnectionPool(
                        minconn=Config.DB_POOL_MIN_CONN,
                        maxconn=Config.DB_POOL_MAX_CONN,
                        host=db_params['host'],
                        port=db_params['port'],
                        dbname=db_params['dbname'],
                        user=db_params['user'],
                        password=db_params['password'],
                        sslmode='require'
                    )
                    logger.info(f"Database pool initialized: {db_params['host']}:{db_params['port']}/{db_params['dbname']}")

                except Exception as e:
                    logger.error(f"Pool initialization failed: {e}")
                    raise

    @staticmethod
    def get_connection():
        """Get a connection from the pool"""
        if DatabaseConfig._pool is None:
            DatabaseConfig.initialize_pool()
        try:
            conn = DatabaseConfig._pool.getconn()
            logger.debug("Connection acquired from pool")
            return conn
        except Exception as e:
            logger.error(f"Get connection failed: {e}")
            raise

    @staticmethod
    def release_connection(conn):
        """Return a connection to the pool"""
        if DatabaseConfig._pool and conn:
            try:
                DatabaseConfig._pool.putconn(conn)
                logger.debug("Connection released to pool")
            except Exception as e:
                logger.error(f"Failed to release connection: {e}")

    @staticmethod
    def close_all():
        """Close all connections in the pool"""
        with DatabaseConfig._lock:
            if DatabaseConfig._pool:
                try:
                    DatabaseConfig._pool.closeall()
                    DatabaseConfig._pool = None
                    logger.info("All database connections closed")
                except Exception as e:
                    logger.error(f"Failed to close all connections: {e}")

    @staticmethod
    def test_connection():
        """Test database connection"""
        conn = None
        try:
            conn = DatabaseConfig.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            logger.info("Database connection test successful")
            return result[0] == 1
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
        finally:
            if conn:
                DatabaseConfig.release_connection(conn)
