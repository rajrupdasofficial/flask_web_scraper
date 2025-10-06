"""
Production Web Crawler - Fixed real-time stats and connection pooling
"""

import scrapy
from scrapy.http import Request, TextResponse
from scrapy import signals
from urllib.parse import urlparse, urljoin, urldefrag
import logging
import hashlib
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import Config
from progress_tracker import ProgressTracker
from operations import DatabaseOperations

logger = logging.getLogger(__name__)

# Suppress urllib3 warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)

# Initialize Cloudinary
CLOUDINARY_ENABLED = False
try:
    if Config.CLOUDINARY_CLOUD_NAME and Config.CLOUDINARY_API_KEY and Config.CLOUDINARY_API_SECRET:
        import cloudinary
        import cloudinary.uploader
        from cloudinary.exceptions import Error as CloudinaryError

        cloudinary.config(
            cloud_name=Config.CLOUDINARY_CLOUD_NAME,
            api_key=Config.CLOUDINARY_API_KEY,
            api_secret=Config.CLOUDINARY_API_SECRET,
            secure=True
        )
        CLOUDINARY_ENABLED = True
        logger.info("✓ Cloudinary configured")
    else:
        logger.warning("⚠ Cloudinary not configured")
except Exception as e:
    logger.warning(f"⚠ Cloudinary error: {e}")


class AssetUploader:
    """Concurrent asset uploader with proper connection pooling"""

    def __init__(self, domain, max_workers=5):
        self.domain = domain
        self.max_workers = max_workers

        # Create session with proper connection pooling
        self.session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )

        # Configure adapter with larger pool
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=20,
            pool_maxsize=20,
            pool_block=False
        )

        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def download_and_upload(self, asset_url, asset_type):
        """Download and upload single asset"""
        if not CLOUDINARY_ENABLED:
            return None

        try:
            response = self.session.get(asset_url, timeout=10, stream=True)

            if response.status_code != 200:
                return None

            content_length = response.headers.get('content-length')
            if content_length and int(content_length) > 10 * 1024 * 1024:
                return None

            content = response.content
            file_size = len(content)

            # Determine resource type
            resource_type = "raw" if asset_type in ['js', 'css'] else "image"

            # Upload to Cloudinary
            upload_result = cloudinary.uploader.upload(
                content,
                resource_type=resource_type,
                folder=f"crawler/{self.domain}/{asset_type}",
                use_filename=True,
                unique_filename=True,
                timeout=20
            )

            cloud_url = upload_result.get('secure_url')
            logger.info(f"✓ Uploaded {asset_type}: {cloud_url}")

            return {
                'cloud_url': cloud_url,
                'file_size': file_size,
            }

        except Exception as e:
            logger.debug(f"Upload failed: {str(e)[:80]}")
            return None

    def upload_assets_concurrent(self, assets_list):
        """Upload multiple assets concurrently"""
        if not CLOUDINARY_ENABLED or not assets_list:
            return assets_list

        uploaded_assets = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_asset = {
                executor.submit(self.download_and_upload, asset['url'], asset['type']): asset
                for asset in assets_list
            }

            for future in as_completed(future_to_asset):
                asset = future_to_asset[future]
                try:
                    result = future.result()
                    if result:
                        asset['cloud_url'] = result['cloud_url']
                        asset['file_size'] = result['file_size']
                except:
                    pass
                uploaded_assets.append(asset)

        return uploaded_assets


class WebCrawlerSpider(scrapy.Spider):
    """Production web crawler with real-time statistics"""

    name = 'production_crawler'
    handle_httpstatus_list = [200, 201, 301, 302, 303, 307, 308, 404, 500]

    custom_settings = {
        'CONCURRENT_REQUESTS': 16,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 8,
        'DOWNLOAD_DELAY': 0.5,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'DOWNLOAD_TIMEOUT': 30,
        'ROBOTSTXT_OBEY': False,

        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',

        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
        },

        'COOKIES_ENABLED': True,
        'RETRY_TIMES': 2,
        'REDIRECT_ENABLED': True,

        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 0.5,
        'AUTOTHROTTLE_MAX_DELAY': 3.0,

        'DEPTH_LIMIT': 3,
        'CLOSESPIDER_PAGECOUNT': 100,
        'CLOSESPIDER_TIMEOUT': 1800,

        'TELNETCONSOLE_ENABLED': False,
        'LOG_LEVEL': 'INFO',
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(spider.item_scraped, signal=signals.item_scraped)
        return spider

    def __init__(self, start_url=None, domain=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not start_url or not domain:
            raise ValueError("start_url and domain required")

        self.start_url = start_url.strip()
        self.domain = self._clean_domain(domain)
        self.allowed_domains = [self.domain]
        self.start_urls = [self.start_url]

        # Statistics
        self.pages_crawled = 0
        self.pages_failed = 0
        self.assets_uploaded = 0
        self.urls_seen = set()
        self.urls_crawled = set()

        # Asset uploader with proper pooling
        self.asset_uploader = AssetUploader(self.domain, max_workers=5)

        # Progress tracking
        self.progress_tracker = ProgressTracker(self.domain, total=100)
        self.start_time = datetime.now()

        logger.info("=" * 100)
        logger.info(f"SPIDER INITIALIZED | URL: {self.start_url} | Domain: {self.domain}")
        logger.info("=" * 100)

        DatabaseOperations.insert_log(self.domain, f"Spider initialized for {self.start_url}", "INFO")

    def _clean_domain(self, domain):
        if domain.startswith('http'):
            return urlparse(domain).netloc
        return domain.strip()

    def spider_opened(self, spider):
        DatabaseOperations.update_crawl_statistics(
            domain=self.domain,
            status='running',
            start_time=self.start_time
        )

    def item_scraped(self, item, response, spider):
        """Called after each item is scraped - update stats in real-time"""
        try:
            # Update database statistics immediately
            DatabaseOperations.update_crawl_statistics(
                domain=self.domain,
                total_pages=self.pages_crawled,
                total_assets=self.assets_uploaded,
                status='running'
            )
            logger.info(f"Stats updated: {self.pages_crawled} pages, {self.assets_uploaded} assets")
        except Exception as e:
            logger.error(f"Failed to update stats: {e}")

    def spider_closed(self, spider, reason):
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()

        logger.info("=" * 100)
        logger.info(f"SPIDER CLOSED: {reason}")
        logger.info(f"Domain: {self.domain}")
        logger.info(f"Duration: {duration:.1f}s")
        logger.info(f"Pages Crawled: {self.pages_crawled}")
        logger.info(f"Pages Failed: {self.pages_failed}")
        logger.info(f"Assets Uploaded: {self.assets_uploaded}")
        logger.info("=" * 100)

        # Final stats update
        DatabaseOperations.update_crawl_statistics(
            domain=self.domain,
            total_pages=self.pages_crawled,
            total_assets=self.assets_uploaded,
            status='completed' if self.pages_crawled > 0 else 'failed',
            end_time=end_time
        )

        DatabaseOperations.insert_log(
            self.domain,
            f"Crawl finished: {self.pages_crawled} pages, {self.assets_uploaded} assets",
            "INFO"
        )

        if self.pages_crawled > 0:
            self.progress_tracker.set_progress(100)

    def start_requests(self):
        logger.info(f"Starting crawl: {self.start_url}")
        yield Request(
            url=self.start_url,
            callback=self.parse,
            errback=self.errback,
            dont_filter=True,
            meta={'dont_redirect': False}
        )

    def parse(self, response):
        """Parse response with proper error handling"""
        url = response.url

        # Check if response is text-based
        if not isinstance(response, TextResponse):
            content_type = response.headers.get('Content-Type', b'').decode('utf-8', 'ignore')
            logger.warning(f"Skipping non-text: {url} ({content_type})")
            self.pages_failed += 1
            return

        # Check status
        if response.status >= 400:
            logger.warning(f"HTTP {response.status}: {url}")
            self.pages_failed += 1
            return

        # Deduplication
        url_hash = hashlib.md5(url.encode()).hexdigest()
        if url_hash in self.urls_crawled:
            return

        self.urls_crawled.add(url_hash)
        self.pages_crawled += 1

        logger.info(f"[{self.pages_crawled}] Crawling: {url}")

        try:
            # Extract page data
            item = self._extract_page_data(response)

            # Upload assets
            if item.get('assets'):
                logger.info(f"Found {len(item['assets'])} assets")
                uploaded_assets = self.asset_uploader.upload_assets_concurrent(item['assets'])
                item['assets'] = uploaded_assets
                self.assets_uploaded += sum(1 for a in uploaded_assets if a.get('cloud_url'))
                logger.info(f"Total assets uploaded: {self.assets_uploaded}")

            # Update progress
            progress_increment = 100.0 / max(self.custom_settings['CLOSESPIDER_PAGECOUNT'], 1)
            self.progress_tracker.increment_progress(progress_increment)

            logger.info(f"Progress: {self.progress_tracker.get_percentage():.1f}%")

            yield item

            # Follow links
            for request in self._extract_links(response):
                yield request

        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
            self.pages_failed += 1

    def _extract_page_data(self, response):
        """Extract data with error handling"""
        url = response.url

        # Title
        try:
            title = (response.css('title::text').get(default='').strip() or
                    response.css('h1::text').get(default='').strip() or
                    response.css('meta[property="og:title"]::attr(content)').get(default='').strip() or
                    'Untitled Page')
        except:
            title = 'Untitled Page'

        # Content
        try:
            paragraphs = response.css('p::text').getall()
            headings = response.css('h1::text, h2::text, h3::text').getall()
            content = ' '.join(paragraphs + headings)
            content = ' '.join(content.split())[:20000]
        except:
            content = ''

        # Metadata
        metadata = {}
        try:
            for meta in response.css('meta'):
                name = meta.attrib.get('name') or meta.attrib.get('property', '')
                content_val = meta.attrib.get('content', '')
                if name and content_val:
                    metadata[name] = content_val[:500]
        except:
            pass

        # Assets
        assets = self._extract_assets(response)

        # Article
        article = self._extract_article(response)

        return {
            'url': url,
            'domain': self.domain,
            'title': title,
            'content': content,
            'status_code': response.status,
            'content_type': response.headers.get('Content-Type', b'').decode('utf-8', 'ignore'),
            'metadata': metadata,
            'assets': assets,
            'article': article,
        }

    def _extract_assets(self, response):
        """Extract assets"""
        assets = []
        base_url = response.url

        try:
            # Images (limit to 10 per page)
            for img_url in response.css('img::attr(src), img::attr(data-src)').getall()[:10]:
                if img_url and not img_url.startswith('data:'):
                    assets.append({
                        'type': 'image',
                        'url': urljoin(base_url, img_url),
                        'cloud_url': None,
                        'file_size': None
                    })

            # JavaScript (limit to 8 per page)
            for js_url in response.css('script::attr(src)').getall()[:8]:
                if js_url and not js_url.startswith('data:'):
                    assets.append({
                        'type': 'js',
                        'url': urljoin(base_url, js_url),
                        'cloud_url': None,
                        'file_size': None
                    })

            # CSS (limit to 8 per page)
            for css_url in response.css('link[rel="stylesheet"]::attr(href)').getall()[:8]:
                if css_url:
                    assets.append({
                        'type': 'css',
                        'url': urljoin(base_url, css_url),
                        'cloud_url': None,
                        'file_size': None
                    })
        except Exception as e:
            logger.warning(f"Asset extraction error: {e}")

        # Deduplicate
        seen = set()
        unique = []
        for asset in assets:
            if asset['url'] not in seen:
                seen.add(asset['url'])
                unique.append(asset)

        return unique

    def _extract_article(self, response):
        """Extract article content"""
        try:
            article_text = ' '.join(
                response.css('article p::text, .post-content p::text, .entry-content p::text, main p::text').getall()
            ).strip()

            if len(article_text) < 100:
                return None

            return {
                'headline': response.css('h1::text, article h1::text').get(default='').strip(),
                'author': response.css('[rel="author"]::text, .author::text, meta[name="author"]::attr(content)').get(default='').strip(),
                'published_date': response.css('time::attr(datetime), meta[property="article:published_time"]::attr(content)').get(),
                'article_text': article_text[:30000]
            }
        except:
            return None

    def _extract_links(self, response):
        """Extract links"""
        try:
            links = response.css('a::attr(href)').getall()
            followed = 0
            max_links = 30

            for link in links:
                if followed >= max_links:
                    break

                if not link or link.startswith(('#', 'javascript:', 'mailto:')):
                    continue

                absolute_url, _ = urldefrag(urljoin(response.url, link))
                parsed = urlparse(absolute_url)

                if parsed.netloc != self.domain:
                    continue

                skip_ext = ('.pdf', '.jpg', '.jpeg', '.png', '.gif', '.zip', '.mp4', '.mp3')
                if absolute_url.lower().endswith(skip_ext):
                    continue

                url_hash = hashlib.md5(absolute_url.encode()).hexdigest()
                if url_hash in self.urls_seen:
                    continue

                self.urls_seen.add(url_hash)
                followed += 1

                yield Request(absolute_url, callback=self.parse, errback=self.errback)

            if followed > 0:
                logger.info(f"Following {followed} links")
        except Exception as e:
            logger.warning(f"Link extraction error: {e}")

    def errback(self, failure):
        """Handle failures"""
        self.pages_failed += 1
        logger.error(f"Request failed: {failure.request.url}")
