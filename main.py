import uuid
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import logging
from pymongo import MongoClient
import schedule
import time
import re
from newspaper import Article
from dateutil import parser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import pika
import json
import gzip
import zlib
import io
try:
    import brotli
except ImportError:
    brotli = None  # Nếu không cài brotli, sẽ bỏ qua hỗ trợ brotli
# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kết nối MongoDB
# client = MongoClient('mongodb://mongo:27017')
client = MongoClient('mongodb://localhost:27017')

db = client['olh_news']
articles_collection = db['articles']
categories_collection = db['categories']
sources_collection = db['sources']
crawl_metadata = db['crawl_metadata']

articles_collection.create_index([("link", 1)], unique=True)
crawl_metadata.create_index([("category_url", 1)])

# Kết nối RabbitMQ (giữ nguyên)
def get_rabbitmq_connection():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='rabbitmq', port=5672, heartbeat=600))
        return connection
    except Exception as e:
        logger.error(f"Lỗi kết nối RabbitMQ: {str(e)}")
        return None

def publish_to_rabbitmq(article_data):
    try:
        connection = get_rabbitmq_connection()
        if not connection:
            return False
        channel = connection.channel()
        exchange_name = 'news_exchange'
        queue_name = 'news_queue'
        routing_key = 'news.article'
        channel.exchange_declare(exchange=exchange_name, exchange_type='topic', durable=True)
        channel.queue_declare(queue=queue_name, durable=True)
        channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)
        article_json = article_data.copy()
        article_json['publish_date'] = article_json['publish_date'].isoformat() if article_json['publish_date'] else None
        article_json['crawl_date'] = article_json['crawl_date'].isoformat() if article_json['crawl_date'] else None
        message = json.dumps(article_json)
        channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=message,
                             properties=pika.BasicProperties(delivery_mode=2, content_type='application/json'))
        logger.info(f"Đã đẩy bài viết vào RabbitMQ: {article_data['title']}")
        connection.close()
        return True
    except Exception as e:
        logger.error(f"Lỗi khi gửi dữ liệu đến RabbitMQ: {str(e)}")
        return False

# Cấu hình requests
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Mobile/15E148 Safari/604.1'
]

session = requests.Session()
retry_strategy = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504, 104], allowed_methods=["HEAD", "GET", "OPTIONS"])
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

def get_random_headers():
    return {'User-Agent': random.choice(USER_AGENTS)}

@lru_cache(maxsize=1)
def get_sources():
    return list(sources_collection.find())

def get_categories():
    categories = list(categories_collection.find())
    return [cat['url'] for cat in categories]

def get_source_from_url(url):
    sources = get_sources()
    for source in sources:
        if source['url'] in url:
            return source
    return None

def get_last_crawl_time(category_url):
    metadata = crawl_metadata.find_one({'category_url': category_url}, {'last_crawl_time': 1})
    return metadata['last_crawl_time'] if metadata else datetime.now() - timedelta(days=1)

def update_last_crawl_time(category_url):
    crawl_metadata.update_one({'category_url': category_url}, {'$set': {'last_crawl_time': datetime.now()}}, upsert=True)

@lru_cache(maxsize=128)
def get_category_info(category_url):
    category = categories_collection.find_one({'url': category_url})
    if category:
        return {'_id': str(category['_id']), 'name': category['name'], 'source': category['source'], 'url': category['url']}
    source = get_source_from_url(category_url)
    if source:
        return {'_id': str(uuid.uuid4()), 'name': 'Unknown', 'source': source, 'url': category_url}
    return None

def check_keywords(category_doc, title, content):
    if not category_doc or "keyword" not in category_doc or not category_doc["keyword"]:
        return True
    keywords = [kw.lower() for kw in category_doc["keyword"]]
    title_lower, content_lower = title.lower(), content.lower()
    return any(keyword in title_lower or keyword in content_lower for keyword in keywords)


def decompress_response(response):
    """Giải nén nội dung response chỉ khi thực sự bị nén."""
    content = response.content

    # Kiểm tra magic number để xác định xem nội dung có thực sự bị nén hay không
    if content[:2] == b'\x1f\x8b':  # Magic number của gzip
        try:
            logger.info("Phát hiện nội dung nén gzip, đang giải nén...")
            return gzip.decompress(content)
        except Exception as e:
            logger.error(f"Lỗi khi giải nén gzip: {str(e)}")
            return content

    elif content[:3] == b'\x78\x9c\xda' or content[:3] == b'\x78\x01\xda':  # Magic number phổ biến của deflate
        try:
            logger.info("Phát hiện nội dung nén deflate, đang giải nén...")
            return zlib.decompress(content)
        except Exception as e:
            logger.error(f"Lỗi khi giải nén deflate: {str(e)}")
            return content

    elif brotli and content[:3] == b'\xce\xb2\xcf':  # Magic number của brotli (có thể thay đổi tùy phiên bản)
        try:
            logger.info("Phát hiện nội dung nén brotli, đang giải nén...")
            return brotli.decompress(content)
        except Exception as e:
            logger.error(f"Lỗi khi giải nén brotli: {str(e)}")
            return content

    # Nếu không phát hiện nén, trả về nội dung gốc
    return content
def extract_article_urls(category_url):
    try:
        response = session.get(category_url, headers=get_random_headers(), timeout=10)
        response.raise_for_status()

        # Giải nén nội dung nếu cần
        decompressed_content = decompress_response(response)
        soup = BeautifulSoup(decompressed_content, 'html.parser')

        article_urls = set()
        source = get_source_from_url(category_url)
        base_url = source['url'] if source else 'https://' + category_url.split('/')[2]

        url_patterns = source.get('url_patterns', [r'.*\.(html|htm|tpo|ldo|chn)$', r'-\d{6,}$']) if source else [
            r'.*\.(html|htm|tpo|ldo|chn)$', r'-\d{6,}$']
        exclude_patterns = source.get('exclude_patterns',
                                      ['/category/', '/tag/', '/author/', '/page/', '/search/']) if source else [
            '/category/', '/tag/', '/author/', '/page/', '/search/']

        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if not href or any(x in href.lower() for x in ['javascript', 'zalo.me', 'facebook.com', '#', '/login']):
                continue

            full_url = href if href.startswith('http') else f"{base_url}{href}"
            if (any(re.search(pattern, full_url) for pattern in url_patterns) and
                    not any(ex in full_url.lower() for ex in exclude_patterns) and
                    len(full_url) > 35):
                article_urls.add(full_url)

        if not article_urls:
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                full_url = href if href.startswith('http') else f"{base_url}{href}"
                if re.search(r'/[a-z0-9-]+/?$', full_url) and len(full_url) > 35:
                    article_urls.add(full_url)

        unique_urls = list(article_urls)[:30]
        logger.info(f"Tìm thấy {len(unique_urls)} URL từ {category_url}: {unique_urls}")
        return unique_urls
    except Exception as e:
        logger.error(f"Lỗi khi trích xuất URL từ {category_url}: {str(e)}")
        return []


def parse_article(args):
    article_url, category_info, last_crawl_time = args
    try:
        time.sleep(random.uniform(1, 3))
        response = session.get(article_url, headers=get_random_headers(), timeout=60)
        response.raise_for_status()
        decompressed_content = decompress_response(response)
        soup = BeautifulSoup(decompressed_content, 'html.parser')
        source = get_source_from_url(article_url)
        article = Article(article_url, language='vi')
        article.set_html(decompressed_content.decode('utf-8', errors='ignore'))
        article.parse()

        # Lấy tiêu đề
        title_selectors = source.get('title_selectors', ['h1', 'h2', '.title', 'title']) if source else ['h1', 'h2', '.title', 'title']
        title = article.title
        if not title:
            for selector in title_selectors:
                title_tag = soup.select_one(selector)
                if title_tag:
                    title = title_tag.get_text(strip=True)
                    break
            if not title and soup.find('title'):
                title = soup.find('title').get_text(strip=True)
        if not title:
            logger.warning(f"Không tìm thấy tiêu đề cho {article_url}")
            return None

        # Lấy ngày đăng
        date_selectors = source.get('date_selectors', ['.date', '.time', 'time', '.publish-date']) if source else ['.date', '.time', 'time', '.publish-date']
        publish_date = article.publish_date
        if not publish_date:
            for selector in date_selectors:
                date_tag = soup.select_one(selector)
                if date_tag:
                    try:
                        publish_date = parser.parse(date_tag.get_text(strip=True)).replace(tzinfo=None)
                        break
                    except:
                        continue
        # Chuẩn hóa publish_date thành offset-naive
        publish_date = publish_date.replace(tzinfo=None) if publish_date else datetime.now().replace(tzinfo=None)

        # Lấy nội dung
        content_selectors = source.get('content_selectors', ['article', '.content', '.article-body', 'p']) if source else ['article', '.content', '.article-body', 'p']
        content = article.text.strip()
        if not content or len(content.split()) < 100:
            for selector in content_selectors:
                content_tags = soup.select(selector)
                if content_tags:
                    content = ' '.join(tag.get_text(strip=True) for tag in content_tags)
                    break
        if not content or len(content.split()) < 100:
            logger.warning(f"Nội dung quá ngắn hoặc không tìm thấy cho {article_url}")
            return None

        # Kiểm tra từ khóa
        category_doc = categories_collection.find_one({'url': category_info['url']})
        if not check_keywords(category_doc, title, content):
            return None

        description = article.meta_description or content[:200]
        images = [article.top_image] if article.top_image else list(article.images)

        article_data = {
            '_id': str(uuid.uuid4()),
            'title': title,
            'link': article_url,
            'description': description,
            'content': content,
            'category': category_info,
            'publish_date': publish_date,
            'images': images,
            'author': article.authors[0] if article.authors else None,
            'crawl_date': datetime.now().replace(tzinfo=None)  # Cũng chuẩn hóa crawl_date
        }
        logger.info(f"Đã phân tích bài viết: {title}")
        return article_data
    except Exception as e:
        logger.error(f"Lỗi khi phân tích bài viết {article_url}: {str(e)}")
        return None
def crawl_category(category_url, articles_collection):
    last_crawl_time = get_last_crawl_time(category_url)
    category_info = get_category_info(category_url)
    if not category_info:
        logger.error(f"Không xác định được danh mục cho {category_url}")
        return

    logger.info(f"Bắt đầu crawl danh mục: {category_url}, lần crawl cuối: {last_crawl_time}")
    article_urls = extract_article_urls(category_url)
    existing_urls = set(articles_collection.distinct('link', {'link': {'$in': article_urls}}))
    new_urls = [url for url in article_urls if url not in existing_urls]

    with ThreadPoolExecutor(max_workers=5) as executor:
        articles = list(filter(None, executor.map(parse_article, [(url, category_info, last_crawl_time) for url in new_urls])))

    if articles:
        valid_articles = [article for article in articles if article['publish_date'] >= last_crawl_time]
        if valid_articles:
            for article in valid_articles:
                try:
                    articles_collection.insert_one(article)
                    # publish_to_rabbitmq(article)
                except Exception as e:
                    logger.error(f"Lỗi khi lưu bài viết {article['link']}: {str(e)}")
            logger.info(f"Đã lưu {len(valid_articles)} bài viết từ {category_url}")
    update_last_crawl_time(category_url)


def crawl_all_categories(articles_collection):
    category_urls = get_categories()
    logger.info(f"Bắt đầu crawl tất cả danh mục lúc {datetime.now()} với {len(category_urls)} categories")
    for category_url in category_urls:
        crawl_category(category_url, articles_collection)
    logger.info("Hoàn thành crawl tất cả danh mục.")

def main():
    crawl_all_categories(articles_collection)
    schedule.every(1).minutes.do(crawl_all_categories, articles_collection)
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except Exception as e:
            logger.error(f"Lỗi trong vòng lặp chính: {str(e)}")
            time.sleep(60)

if __name__ == "__main__":
    main()