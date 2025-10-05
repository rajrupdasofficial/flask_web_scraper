import logging
from datetime import datetime
from operations import DatabaseOperations

logger = logging.getLogger(__name__)

class PostgreSQLPipeline:
    def open_spider(self, spider):
        """Called when spider is opened"""
        try:
            logger.info(f"Opening pipeline for {spider.domain}")
            DatabaseOperations.update_crawl_statistics(
                spider.domain,
                status='running',
                start_time=datetime.now()
            )
            DatabaseOperations.insert_log(spider.domain, "Pipeline opened - crawl started", "INFO")
        except Exception as e:
            logger.error(f"Error opening pipeline: {e}")

    def close_spider(self, spider):
        """Called when spider is closed"""
        try:
            logger.info(f"Closing pipeline for {spider.domain}")

            # Get final counts
            total_pages = spider.pages_crawled
            total_assets = spider.assets_uploaded

            # Update statistics
            DatabaseOperations.update_crawl_statistics(
                spider.domain,
                total_pages=total_pages,
                total_assets=total_assets,
                status='completed',
                end_time=datetime.now()
            )

            DatabaseOperations.insert_log(
                spider.domain,
                f"Pipeline closed - Pages: {total_pages}, Assets: {total_assets}",
                "INFO"
            )

            logger.info(f"Pipeline closed successfully for {spider.domain}")

        except Exception as e:
            logger.error(f"Error closing pipeline: {e}")

    def process_item(self, item, spider):
        """Process each scraped item"""
        try:
            logger.info(f"Processing item: {item['url']}")

            # Insert page
            page_id = DatabaseOperations.insert_page(
                url=item['url'],
                domain=item['domain'],
                title=item.get('title'),
                content=item.get('content'),
                status_code=item.get('status_code'),
                content_type=item.get('content_type')
            )

            if not page_id:
                logger.warning(f"Page already exists or insert failed: {item['url']}")
                return item

            # Insert metadata
            if item.get('metadata'):
                DatabaseOperations.insert_metadata_batch(page_id, item['metadata'])
                logger.debug(f"Inserted {len(item['metadata'])} metadata entries")

            # Insert assets
            if item.get('assets'):
                DatabaseOperations.insert_asset_batch(page_id, item['assets'])
                logger.debug(f"Inserted {len(item['assets'])} assets")

            # Insert article if exists
            if item.get('article'):
                article = item['article']
                DatabaseOperations.insert_article(
                    page_id=page_id,
                    headline=article.get('headline'),
                    author=article.get('author'),
                    published_date=article.get('published_date'),
                    article_text=article.get('article_text')
                )
                logger.info(f"Inserted article: {article.get('headline', 'No headline')}")

            DatabaseOperations.insert_log(
                item['domain'],
                f"Successfully processed: {item['url']}",
                "INFO"
            )

            return item

        except Exception as e:
            logger.error(f"Error processing item {item.get('url', 'unknown')}: {e}")
            DatabaseOperations.insert_log(
                item.get('domain', 'unknown'),
                f"Failed to process item: {str(e)}",
                "ERROR"
            )
            # Don't raise - allow spider to continue
            return item
