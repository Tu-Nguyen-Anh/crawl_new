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
import pytz

# Cấu hình logging (chỉ giữ INFO và ERROR)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Hàm lấy thời gian hiện tại theo múi giờ Asia/Ho_Chi_Minh
def get_current_time_vn():
    vn_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    return datetime.now(vn_timezone)

# Kết nối MongoDB
# client = MongoClient('mongodb://localhost:27017')
client = MongoClient('mongodb://mongo:27017')


db = client['olh_news']
articles_collection = db['articles']
categories_collection = db['categories']
sources_collection = db['sources']
crawl_metadata = db['crawl_metadata']
crawl_schedule_collection = db['crawl_config']

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
        channel.queue_declare(queue_name, durable=True)
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
retry_strategy = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504, 104],
                       allowed_methods=["HEAD", "GET", "OPTIONS"])
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
    vn_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    if metadata and 'last_crawl_time' in metadata:
        last_crawl_time = metadata['last_crawl_time']
        if last_crawl_time.tzinfo is None:
            last_crawl_time = vn_timezone.localize(last_crawl_time)
        return last_crawl_time
    return get_current_time_vn() - timedelta(days=1)

def update_last_crawl_time(category_url):
    crawl_metadata.update_one(
        {'category_url': category_url},
        {'$set': {'last_crawl_time': get_current_time_vn()}},
        upsert=True
    )

@lru_cache(maxsize=128)
def get_category_info(category_url):
    category = categories_collection.find_one({'url': category_url})
    if category:
        return {'_id': str(category['_id']), 'name': category['name'], 'source': category['source'],
                'url': category['url']}
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

def extract_article_urls(category_url):
    try:
        response = session.get(category_url, headers=get_random_headers(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
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
                    len(full_url) > 45):
                article_urls.add(full_url)

        if not article_urls:
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                full_url = href if href.startswith('http') else f"{base_url}{href}"
                if re.search(r'/[a-z0-9-]+/?$', full_url) and len(full_url) > 45:
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
        time.sleep(random.uniform(1, 3))
        response = session.get(article_url, headers=get_random_headers(), timeout=60)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        source = get_source_from_url(article_url)

        article = Article(article_url, language='vi')
        article.set_html(response.text)
        article.parse()

        title_selectors = source.get('title_selectors', ['h1', 'h2', '.title', 'title']) if source else ['h1', 'h2',
                                                                                                         '.title',
                                                                                                         'title']
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

        date_selectors = source.get('date_selectors', ['.date', '.time', 'time', '.publish-date']) if source else [
            '.date', '.time', 'time', '.publish-date']
        publish_date = article.publish_date
        vn_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
        if not publish_date:
            for selector in date_selectors:
                date_tag = soup.select_one(selector)
                if date_tag:
                    try:
                        publish_date = parser.parse(date_tag.get_text(strip=True))
                        if publish_date.tzinfo is None:
                            publish_date = vn_timezone.localize(publish_date)
                        break
                    except:
                        continue
        if not publish_date:
            publish_date = get_current_time_vn()
        elif publish_date.tzinfo is None:
            publish_date = vn_timezone.localize(publish_date)

        content_selectors = source.get('content_selectors',
                                      ['article', '.content', '.article-body', 'p']) if source else ['article',
                                                                                                    '.content',
                                                                                                    '.article-body',
                                                                                                    'p']
        content = article.text.strip()
        if not content or len(content.split()) < 300:
            for selector in content_selectors:
                content_tags = soup.select(selector)
                if content_tags:
                    content = ' '.join(tag.get_text(strip=True) for tag in content_tags)
                    break
        if not content or len(content.split()) < 300:
            logger.warning(f"Nội dung quá ngắn hoặc không tìm thấy cho {article_url}")
            return None

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
            'crawl_date': get_current_time_vn()
        }
        logger.info(f"Đã phân tích bài viết: {title}")
        return article_data
    except Exception as e:
        logger.error(f"Lỗi khi phân tích bài viết {article_url}: {str(e)}")
        return None

def crawl_category(category_url, articles_collection):
    try:
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
            articles = list(
                filter(None, executor.map(parse_article, [(url, category_info, last_crawl_time) for url in new_urls])))

        if articles:
            valid_articles = [article for article in articles if article['publish_date'] >= last_crawl_time]
            if valid_articles:
                for article in valid_articles:
                    try:
                        articles_collection.insert_one(article)
                        publish_to_rabbitmq(article)
                    except Exception as e:
                        logger.error(f"Lỗi khi lưu bài viết {article['link']}: {str(e)}")
                logger.info(f"Đã lưu {len(valid_articles)} bài viết từ {category_url}")
        update_last_crawl_time(category_url)
    except Exception as e:
        logger.error(f"Lỗi khi crawl danh mục {category_url}: {str(e)}")

def crawl_all_categories(articles_collection):
    try:
        category_urls = get_categories()
        logger.info(f"Bắt đầu crawl tất cả danh mục lúc {get_current_time_vn()} với {len(category_urls)} danh mục")
        for category_url in category_urls:
            crawl_category(category_url, articles_collection)
        logger.info("Hoàn thành crawl tất cả danh mục.")
    except Exception as e:
        logger.error(f"Lỗi khi crawl tất cả danh mục: {str(e)}")

# Đọc và áp dụng cấu hình từ MongoDB
last_config = None

def apply_schedule_config():
    global last_config
    try:
        config = crawl_schedule_collection.find_one(sort=[("updated_at", -1)])

        if not config:
            logger.warning("Không tìm thấy cấu hình trong crawl_config.")
            if last_config is not None:
                schedule.clear('crawl')
                last_config = None
            return

        # Chuẩn hóa dữ liệu từ MongoDB
        current_config = config.copy()
        current_config_time = current_config.get("updated_at")
        if isinstance(current_config_time, dict) and "$numberLong" in current_config_time:
            current_config["updated_at"] = int(current_config_time["$numberLong"])

        mode = current_config.get("mode")
        times = current_config.get("times", ["08:00", "12:00", "18:00"]) if mode == 2 else None
        minutes = current_config.get("minutes", 5) if mode == 3 else None

        config_changed = False
        if last_config is None:
            config_changed = True
        else:
            last_mode = last_config.get("mode")
            last_times = last_config.get("times", ["08:00", "12:00", "18:00"]) if last_mode == 2 else None
            last_minutes = last_config.get("minutes", 5) if last_mode == 3 else None

            if mode != last_mode or (mode == 2 and times != last_times) or (mode == 3 and minutes != last_minutes):
                config_changed = True

        if config_changed:
            last_config = current_config.copy()
            schedule.clear('crawl')

            if mode == 1:
                logger.info("Crawl ngay lập tức")
                crawl_all_categories(articles_collection)

            elif mode == 2:
                for t in times:
                    try:
                        schedule.every().day.at(t).tag('crawl').do(crawl_all_categories, articles_collection)
                    except Exception as e:
                        logger.error(f"Lỗi khi lên lịch cho thời gian {t}: {str(e)}")
                logger.info(f"Crawl hàng ngày tại: {times}")

            elif mode == 3:
                if not isinstance(minutes, (int, float)) or minutes <= 0:
                    logger.error(f"Giá trị minutes không hợp lệ: {minutes}")
                    return
                schedule.every(minutes).minutes.tag('crawl').do(crawl_all_categories, articles_collection)
                logger.info(f"Crawl mỗi {minutes} phút")

            else:
                logger.error(f"Cấu hình không hợp lệ: mode={mode}")
                return
    except Exception as e:
        logger.error(f"Lỗi khi áp dụng cấu hình: {str(e)}", exc_info=True)

def check_schedule_config():
    try:
        apply_schedule_config()
    except Exception as e:
        logger.error(f"Lỗi khi kiểm tra cấu hình: {str(e)}")

def main():
    logger.info("Khởi động chương trình")
    apply_schedule_config()
    schedule.every(5).seconds.tag('config_check').do(check_schedule_config)
    logger.info("Bắt đầu vòng lặp chính, kiểm tra cấu hình mỗi 5 giây")
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            logger.error(f"Lỗi trong vòng lặp chính: {str(e)}")
            time.sleep(6)

if __name__ == "__main__":
    main()