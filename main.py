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

# Import các crawler riêng biệt
import tienphong_crawler
import nhandan_crawler
import vnexpress_crawler

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kết nối MongoDB và tạo index
client = MongoClient('mongodb://mongo:27017')
db = client['olh_news']
articles_collection = db['articles']
categories_collection = db['categories']
sources_collection = db['sources']
crawl_metadata = db['crawl_metadata']

# Tạo index để tăng tốc truy vấn
articles_collection.create_index([("link", 1)], unique=True)
crawl_metadata.create_index([("category_url", 1)])


# Kết nối RabbitMQ
def get_rabbitmq_connection():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='rabbitmq',  # Thay đổi host này nếu RabbitMQ server không chạy trên localhost
            port=5672,  # Port mặc định của RabbitMQ
            heartbeat=600  # Heartbeat để giữ kết nối sống
        ))
        return connection
    except Exception as e:
        logger.error(f"Lỗi kết nối RabbitMQ: {str(e)}")
        return None


# Hàm để gửi dữ liệu đến RabbitMQ
def publish_to_rabbitmq(article_data):
    try:
        connection = get_rabbitmq_connection()
        if not connection:
            logger.error("Không thể kết nối đến RabbitMQ")
            return False

        channel = connection.channel()

        # Khai báo exchange và queue
        exchange_name = 'news_exchange'
        queue_name = 'news_queue'
        routing_key = 'news.article'

        channel.exchange_declare(exchange=exchange_name, exchange_type='topic', durable=True)
        channel.queue_declare(queue=queue_name, durable=True)
        channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)

        # Chuyển đổi datetime thành string để có thể serialize
        article_json = article_data.copy()
        article_json['publish_date'] = article_json['publish_date'].isoformat() if article_json[
            'publish_date'] else None
        article_json['crawl_date'] = article_json['crawl_date'].isoformat() if article_json['crawl_date'] else None

        # Chuyển đổi dữ liệu thành JSON và gửi
        message = json.dumps(article_json)
        channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Đảm bảo tin nhắn được lưu trữ
                content_type='application/json'
            )
        )

        logger.info(f"Đã đẩy bài viết vào RabbitMQ: {article_data['title']}")
        connection.close()
        return True
    except Exception as e:
        logger.error(f"Lỗi khi gửi dữ liệu đến RabbitMQ: {str(e)}")
        return False


USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Mobile/15E148 Safari/604.1'
]

# Tái sử dụng session requests với retry strategy cải tiến
session = requests.Session()
retry_strategy = Retry(
    total=5,
    backoff_factor=2,
    status_forcelist=[500, 502, 503, 504, 104],
    allowed_methods=["HEAD", "GET", "OPTIONS"]  # Đã sửa từ method_whitelist
)
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
    crawl_metadata.update_one(
        {'category_url': category_url},
        {'$set': {'last_crawl_time': datetime.now()}},
        upsert=True
    )


@lru_cache(maxsize=128)
def get_category_info(category_url):
    category = categories_collection.find_one({'url': category_url})
    if category:
        return {
            '_id': str(category['_id']),
            'name': category['name'],
            'source': category['source'],
            'url': category['url']
        }
    source = get_source_from_url(category_url)
    if source:
        return {
            '_id': str(uuid.uuid4()),
            'name': 'Unknown',
            'source': source,
            'url': category_url
        }
    return None


def check_keywords(category_doc, title, content):
    if not category_doc or "keyword" not in category_doc or not category_doc["keyword"]:
        return True
    keywords = [kw.lower() for kw in category_doc["keyword"]]
    title_lower, content_lower = title.lower(), content.lower()
    return any(keyword in title_lower or keyword in content_lower for keyword in keywords)


def extract_article_urls(category_url):
    try:
        response = session.get(category_url, headers=get_random_headers(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        article_urls = set()
        source = get_source_from_url(category_url)
        base_url = source['url'] if source else 'https://' + category_url.split('/')[2]

        url_patterns = {
            'cafebiz.vn': (lambda href: href.endswith('.chn') and re.search(r'\d{10,}', href), 'div', 'cfbiznews_box'),
            'vneconomy.vn': (
            lambda href: href.endswith('.htm') and not re.search(r'^/[a-z-]+\.htm$', href), 'article', 'story'),
            'thanhnien.vn': (
            lambda href: href.endswith('.htm') and re.search(r'-\d{15,}\.htm$', href), ['div', 'article'],
            re.compile('box-category-item|item-first|item-related|list__focus|box-category-middle')),
            'tuoitre.vn': (lambda href: href.endswith('.htm') and re.search(r'-\d{14}\.htm$', href), ['div', 'li'],
                           re.compile(
                               'box-category-item-main|item-first|item-related|box-category-item|box-category-middle')),
            'tinnhanhchungkhoan.vn': (
            lambda href: '-post' in href and re.search(r'-post\d+\.html$', href), 'article', 'story')
        }

        for domain, (condition, tag, class_) in url_patterns.items():
            if domain in category_url:
                items = soup.find_all(tag, class_=class_)
                for item in items:
                    a_tag = item.find('a', href=True)
                    if a_tag and condition(a_tag['href']):
                        href = a_tag['href']
                        full_url = href if href.startswith('http') else f"{base_url}{href}"
                        article_urls.add(full_url)
                break
        else:
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if not href or any(x in href for x in ['javascript', 'zalo.me', 'facebook.com']):
                    continue
                full_url = href if href.startswith('http') else f"{base_url}{href}"
                if (('tuoitre.vn' in category_url and href.endswith('.htm') and re.search(r'-\d{14}\.htm$', href)) or
                        ('thanhnien.vn' in category_url and href.endswith('.htm') and re.search(r'-\d{15,}\.htm$',
                                                                                                href)) or
                        ('laodong.vn' in category_url and href.endswith('.ldo') and re.search(r'-\d{15,}\.ldo$',
                                                                                              href)) or
                        ('tinnhanhchungkhoan.vn' in category_url and '-post' in href and re.search(r'-post\d+\.html$',
                                                                                                   href)) or
                        re.match(r'.*\.html$|/.*-\d+$', href)):
                    article_urls.add(full_url)

        unique_urls = list(article_urls)[:30]
        logger.info(f"Tìm thấy {len(unique_urls)} URL từ {category_url}")
        return unique_urls
    except Exception as e:
        logger.error(f"Lỗi khi trích xuất URL từ {category_url}: {str(e)}")
        return []


def parse_article(args):
    article_url, category_info, last_crawl_time = args
    try:
        time.sleep(random.uniform(1, 3))  # Thêm delay ngẫu nhiên
        response = session.get(article_url, headers=get_random_headers(), timeout=60)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        article = Article(article_url, language='vi')
        article.set_html(response.text)
        article.parse()

        title = article.title or soup.find('title').get_text(strip=True) if soup.find('title') else None
        if not title:
            return None

        publish_date = article.publish_date
        if publish_date:
            try:
                publish_date = parser.parse(str(publish_date)).replace(tzinfo=None)
            except Exception:
                publish_date = datetime.now()
        else:
            publish_date = datetime.now()

        content = article.text.strip() if article.text else ''
        if not content or len(content.split()) < 50:
            return None

        category_doc = categories_collection.find_one({'url': category_info['url']})
        if not check_keywords(category_doc, title, content):
            return None

        description = article.meta_description or content[:200]
        images = [article.top_image] if article.top_image else list(article.images)

        author = None
        if 'cafebiz.vn' in article_url:
            last_line = content.split('\n')[-1].strip()
            author = last_line if last_line and last_line.lower() != "https" else 'cafebiz'
        else:
            author = article.authors[0] if article.authors else None
            if not author:
                author_rules = {
                    'vneconomy.vn': ('span', 'author'),
                    'thanhnien.vn': ('div', 'detail-author'),
                    'vietnamnet.vn': ('p', 'article-detail-author__info'),
                    'nguoiquansat.vn': ('span', 'author')
                }
                for domain, (tag, class_) in author_rules.items():
                    if domain in article_url:
                        author_tag = soup.find(tag, class_=class_) or soup.find('meta', {'name': 'author'})
                        if author_tag:
                            author = author_tag.get_text(strip=True) if author_tag.name != 'meta' else author_tag.get(
                                'content', '').strip()
                            if author == "Https":
                                author = domain.split('.')[0]
                        break
                if not author and content:
                    last_line = content.split('\n')[-1].strip()
                    author = last_line.replace('Theo ', '').strip() if last_line.startswith('Theo ') else last_line

        article_data = {
            '_id': str(uuid.uuid4()),
            'title': title,
            'link': article_url,
            'description': description,
            'content': content,
            'category': category_info,
            'publish_date': publish_date,
            'images': images,
            'author': author,
            'crawl_date': datetime.now()
        }
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

    if 'tienphong.vn' in category_url:
        tienphong_crawler.crawl_tienphong_category(category_url, articles_collection, categories_collection,
                                                   last_crawl_time, publish_to_rabbitmq)
    elif 'nhandan.vn' in category_url:
        nhandan_crawler.crawl_nhandan_category(category_url, articles_collection, categories_collection,
                                               last_crawl_time, publish_to_rabbitmq)
    elif 'vnexpress.net' in category_url:
        vnexpress_crawler.crawl_vnexpress_category(category_url, articles_collection, categories_collection,
                                                   last_crawl_time, publish_to_rabbitmq)
    else:
        article_urls = extract_article_urls(category_url)
        existing_urls = set(articles_collection.distinct('link', {'link': {'$in': article_urls}}))
        new_urls = [url for url in article_urls if url not in existing_urls]

        with ThreadPoolExecutor(max_workers=5) as executor:
            articles = list(
                filter(None, executor.map(parse_article, [(url, category_info, last_crawl_time) for url in new_urls])))

        if articles:
            valid_articles = [article for article in articles if article['publish_date'] >= last_crawl_time]
            if valid_articles:
                for article in valid_articles:
                    try:
                        articles_collection.insert_one(article)
                        # Đẩy vào RabbitMQ
                        publish_to_rabbitmq(article)
                    except Exception as e:
                        logger.error(f"Lỗi khi lưu bài viết {article['link']}: {str(e)}")
                logger.info(f"Đã lưu {len(valid_articles)} bài viết từ {category_url}")

    update_last_crawl_time(category_url)


def crawl_all_categories(articles_collection):
    category_urls = get_categories()  # Lấy danh mục mới nhất mỗi lần
    logger.info(f"Bắt đầu crawl tất cả danh mục lúc {datetime.now()}")
    for category_url in category_urls:
        crawl_category(category_url, articles_collection)
    logger.info("Hoàn thành crawl tất cả danh mục.")


def main():
    crawl_all_categories(articles_collection)
    schedule.every(5).minutes.do(crawl_all_categories, articles_collection)

    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except Exception as e:
            logger.error(f"Lỗi trong vòng lặp chính: {str(e)}")
            time.sleep(60)


if __name__ == "__main__":
    main()