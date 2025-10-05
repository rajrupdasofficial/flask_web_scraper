from urllib.parse import urlparse, urljoin
import scrapy
from scrapy.http import Request
import logging
import cloudinary
import cloudinary.uploader
import requests
from config import Config
from progress_tracker import ProgressTracker
from operations import DatabaseOperations
import time

logger = logging.getLogger(__name__)

# Configure Cloudinary
cloudinary.config(
    cloud_name=Config.CLOUDINARY_CLOUD_NAME,
    api_key=Config.CLOUDINARY_API_KEY,
    api_secret=Config.CLOUDINARY_API_SECRET
)

class WebCrawlerSpider(scrapy.Spider):
    name = 'web_crawler'
    custom_settings = {
        'CONCURRENT_REQUESTS': 8,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 4,
        'DOWNLOAD_DELAY': 1,
        'ROBOTSTXT_OBEY': False,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'COOKIES_ENABLED': True,
        'TELNETCONSOLE_ENABLED': False,
        'LOG_LEVEL': 'INFO',
        'RETRY_TIMES': 3,
        'DOWNLOAD_TIMEOUT': 30,
        'DEPTH_LIMIT': 2,
        'CLOSESPIDER_PAGECOUNT': 50,
        'HTTPERROR_ALLOW_ALL': True,
        'REDIRECT_ENABLED': True,
        'REDIRECT_MAX_TIMES': 3,
    }

    def __init__(self, start_url=None, domain=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not start_url or not domain:
            raise ValueError("start_url and domain are required")

        self.start_urls = [start_url]

        # Clean domain
        if domain.startswith('http://') or domain.startswith('https://'):
            domain = urlparse(domain).netloc

        self.allowed_domains = [domain]
        self.domain = domain
        self.visited_urls = set()
        self.pages_crawled = 0
        self.assets_uploaded = 0

        # Initialize progress tracker
        self.progress_tracker = ProgressTracker(domain, total=100)

        DatabaseOperations.insert_log(domain, f"Spider initialized for {start_url}", "INFO")
        logger.info(f"Spider initialized: start_url={start_url}, domain={domain}")

    def start_requests(self):
        for url in self.start_urls:
            logger.info(f"Starting request for: {url}")
            yield Request(url, callback=self.parse, errback=self.errback, dont_filter=True)

    def parse(self, response):
        url = response.url
        logger.info(f"Parsing URL: {url} (Status: {response.status})")

        # Skip if already visited
        if url in self.visited_urls:
            logger.debug(f"Skipping duplicate: {url}")
            return

        self.visited_urls.add(url)
        self.pages_crawled += 1

        DatabaseOperations.insert_log(
            self.domain,
            f"Processing page {self.pages_crawled}: {url} (Status: {response.status})",
            "INFO"
        )

        # Extract page content
        title = response.css('title::text').get(default='').strip()
        content = ' '.join(response.css('body ::text').getall()).strip()[:5000]

        # Extract metadata
        metadata = {}
        for meta in response.css('meta'):
            key = meta.attrib.get('name') or meta.attrib.get('property')
            value = meta.attrib.get('content')
            if key and value:
                metadata[key] = value.strip()

        # Extract and upload assets
        assets = []

        # Images
        for img in response.css('img::attr(src)').getall()[:10]:  # Limit to 10 images
            asset_url = urljoin(url, img)
            cloud_url = self.upload_asset(asset_url, 'image')
            if cloud_url:
                assets.append({
                    'type': 'image',
                    'url': asset_url,
                    'cloud_url': cloud_url,
                    'content': None,
                    'file_size': None
                })
                self.assets_uploaded += 1

        # JavaScript files
        for js in response.css('script::attr(src)').getall()[:5]:  # Limit to 5 JS files
            asset_url = urljoin(url, js)
            cloud_url = self.upload_asset(asset_url, 'js')
            if cloud_url:
                assets.append({
                    'type': 'js',
                    'url': asset_url,
                    'cloud_url': cloud_url,
                    'content': None,
                    'file_size': None
                })
                self.assets_uploaded += 1

        # CSS files
        for css in response.css('link[rel="stylesheet"]::attr(href)').getall()[:5]:  # Limit to 5 CSS files
            asset_url = urljoin(url, css)
            cloud_url = self.upload_asset(asset_url, 'css')
            if cloud_url:
                assets.append({
                    'type': 'css',
                    'url': asset_url,
                    'cloud_url': cloud_url,
                    'content': None,
                    'file_size': None
                })
                self.assets_uploaded += 1

        # Extract article/blog content
        headline = response.css('h1::text, article h1::text, .post-title::text, .entry-title::text').get(default='').strip()
        author = response.css('[itemprop="author"]::text, .author::text, meta[name="author"]::attr(content), .byline::text').get(default='').strip()
        published_date = response.css('[itemprop="datePublished"]::attr(content), meta[name="pubdate"]::attr(content), time::attr(datetime)').get()
        article_text = ' '.join(response.css('article p::text, .post-content p::text, .entry-content p::text, .article-body p::text').getall()).strip()

        # Create item
        item = {
            'url': url,
            'domain': self.domain,
            'title': title,
            'content': content,
            'status_code': response.status,
            'content_type': response.headers.get('Content-Type', b'').decode('utf-8', errors='ignore'),
            'metadata': metadata,
            'assets': assets,
            'article': {
                'headline': headline,
                'author': author,
                'published_date': published_date,
                'article_text': article_text
            } if (headline or article_text) else None
        }

        # Update progress
        self.progress_tracker.increment_progress(1)
        current_progress = self.progress_tracker.get_percentage()
        logger.info(f"Progress: {current_progress}% (Pages: {self.pages_crawled}, Assets: {self.assets_uploaded})")

        yield item

        # Follow links
        followed_count = 0
        for href in response.css('a::attr(href)').getall():
            absolute_url = urljoin(url, href)
            parsed = urlparse(absolute_url)

            # Check if URL is within allowed domain and not visited
            if parsed.netloc == self.domain and absolute_url not in self.visited_urls:
                logger.debug(f"Following: {absolute_url}")
                yield Request(absolute_url, callback=self.parse, errback=self.errback)
                followed_count += 1

        logger.info(f"Followed {followed_count} new links from {url}")

    def errback(self, failure):
        url = failure.request.url
        error_msg = str(failure.value)
        logger.error(f"Request failed for {url}: {error_msg}")
        DatabaseOperations.insert_log(self.domain, f"Request failed for {url}: {error_msg}", "ERROR")

    def upload_asset(self, asset_url, asset_type):
        """Upload asset to Cloudinary"""
        try:
            logger.info(f"Uploading {asset_type}: {asset_url}")

            # Download asset first
            response = requests.get(asset_url, timeout=10, stream=True)
            if response.status_code != 200:
                logger.warning(f"Failed to download {asset_url}: Status {response.status_code}")
                return None

            # Check file size (limit to 10MB)
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) > 10 * 1024 * 1024:
                logger.warning(f"File too large: {asset_url}")
                return None

            # Upload to Cloudinary
            resource_type = "raw" if asset_type in ['js', 'css'] else "image"

            upload_result = cloudinary.uploader.upload(
                asset_url,
                resource_type=resource_type,
                folder=f"crawler/{self.domain}",
                use_filename=True,
                unique_filename=True
            )

            cloud_url = upload_result.get('secure_url')
            logger.info(f"Successfully uploaded to: {cloud_url}")

            return cloud_url

        except requests.exceptions.RequestException as e:
            logger.error(f"Download failed for {asset_url}: {e}")
            DatabaseOperations.insert_log(self.domain, f"Download failed for {asset_url}: {e}", "ERROR")
            return None
        except cloudinary.exceptions.Error as e:
            logger.error(f"Cloudinary upload failed for {asset_url}: {e}")
            DatabaseOperations.insert_log(self.domain, f"Cloudinary upload failed for {asset_url}: {e}", "ERROR")
            return None
        except Exception as e:
            logger.error(f"Unexpected error uploading {asset_url}: {e}")
            DatabaseOperations.insert_log(self.domain, f"Upload error for {asset_url}: {e}", "ERROR")
            return None

    def closed(self, reason):
        logger.info(f"Spider closed: {reason}")
        logger.info(f"Final stats - Pages: {self.pages_crawled}, Assets: {self.assets_uploaded}")
        DatabaseOperations.insert_log(
            self.domain,
            f"Spider closed: {reason}. Pages: {self.pages_crawled}, Assets: {self.assets_uploaded}",
            "INFO"
        )
