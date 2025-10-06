import redis
import logging
from config import Config

logger = logging.getLogger(__name__)


class ProgressTracker:
    def __init__(self, domain, total=100):
        if not domain:
            raise ValueError("Domain must be provided")

        self.domain = domain

        try:
            self.redis = redis.from_url(Config.REDIS_URL, decode_responses=True)
            # Test connection
            self.redis.ping()
        except Exception as e:
            logger.error(f"Redis connection failed: {e}")
            raise

        self.key = f"progress:{domain}"
        self.total_key = f"total:{domain}"

        # Initialize if not exists
        if not self.redis.exists(self.key):
            self.set_progress(0)
        if not self.redis.exists(self.total_key):
            self.set_total(total)

    def set_progress(self, value):
        try:
            self.redis.set(self.key, float(value))
            logger.info(f"Progress set to {value} for {self.domain}")
        except Exception as e:
            logger.error(f"Failed to set progress: {e}")

    def increment_progress(self, amount=1):
        try:
            new_val = self.redis.incrbyfloat(self.key, float(amount))
            logger.info(f"Progress incremented by {amount} to {new_val} for {self.domain}")
            return new_val
        except Exception as e:
            logger.error(f"Failed to increment progress: {e}")
            return 0

    def get_progress(self):
        try:
            val = self.redis.get(self.key)
            return float(val) if val else 0.0
        except Exception as e:
            logger.error(f"Failed to get progress: {e}")
            return 0.0

    def set_total(self, total):
        try:
            self.redis.set(self.total_key, int(total))
        except Exception as e:
            logger.error(f"Failed to set total: {e}")

    def get_total(self):
        try:
            val = self.redis.get(self.total_key)
            return int(val) if val else 100
        except Exception as e:
            logger.error(f"Failed to get total: {e}")
            return 100

    def get_percentage(self):
        try:
            total = self.get_total()
            if total == 0:
                return 0.0
            progress = self.get_progress()
            percentage = min((progress / total) * 100, 100.0)
            return round(percentage, 2)
        except Exception as e:
            logger.error(f"Failed to get percentage: {e}")
            return 0.0

    def reset(self):
        try:
            self.redis.delete(self.key)
            self.redis.delete(self.total_key)
            self.set_progress(0)
            self.set_total(100)
            logger.info(f"Progress reset for {self.domain}")
        except Exception as e:
            logger.error(f"Failed to reset progress: {e}")
