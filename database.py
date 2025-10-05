import psycopg2
from psycopg2 import pool
from threading import Lock
import logging
from config import Config

logger = logging.getLogger(__name__)

class DatabaseConfig:
    _pool = None
    _lock = Lock()

    @staticmethod
    def initialize_pool():
        with DatabaseConfig._lock:
            if DatabaseConfig._pool is None:
                try:
                    Config.validate()
                    DatabaseConfig._pool = pool.SimpleConnectionPool(
                        minconn=1,
                        maxconn=10,
                        host=Config.DB_HOST,
                        port=Config.DB_PORT,
                        dbname=Config.DB_NAME,
                        user=Config.DB_USER,
                        password=Config.DB_PASSWORD,
                        sslmode='require'
                    )
                    logger.info("Database pool initialized")
                except Exception as e:
                    logger.error(f"Pool init failed: {e}")
                    raise

    @staticmethod
    def get_connection():
        if DatabaseConfig._pool is None:
            DatabaseConfig.initialize_pool()
        try:
            conn = DatabaseConfig._pool.getconn()
            logger.debug("Connection acquired")
            return conn
        except Exception as e:
            logger.error(f"Get connection failed: {e}")
            raise

    @staticmethod
    def release_connection(conn):
        if DatabaseConfig._pool:
            DatabaseConfig._pool.putconn(conn)
            logger.debug("Connection released")

    @staticmethod
    def close_all():
        if DatabaseConfig._pool:
            DatabaseConfig._pool.closeall()
            logger.info("All connections closed")
