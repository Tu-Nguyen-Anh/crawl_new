import uuid
import schedule
import time
import logging
from pymongo import MongoClient
from datetime import datetime, timedelta

from vnexpress_crawler import crawl_vnexpress_category
from nhandan_crawler import crawl_nhandan_category
from tienphong_crawler import crawl_tienphong_category

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = MongoClient('mongodb://localhost:27017/')
db = client['olh_news']
articles_collection = db['articles']
categories_collection = db['categories']
sources_collection = db['sources']
crawl_metadata = db['crawl_metadata']

SOURCES = [
    {"_id": str(uuid.uuid4()), "url": "https://vnexpress.net", "name": "VN EXPRESS"},
    {"_id": str(uuid.uuid4()), "url": "https://nhandan.vn", "name": "NHAN DAN"},
    {"_id": str(uuid.uuid4()), "url": "https://tienphong.vn", "name": "TIEN PHONG"}
]

def initialize_sources():
    for source in SOURCES:
        if not sources_collection.find_one({'_id': source['_id']}):
            sources_collection.insert_one(source)

def initialize_categories():
    initialize_sources()
    # Remove hardcoded categories here, they will be managed in the database

def get_category_info(category_url):
    category = categories_collection.find_one({'url': category_url})
    if category:
        return {
            '_id': category['_id'],
            'name': category['name'],
            'source': category['source'],
            'url': category['url']
        }
    return None

def get_last_crawl_time(category_url):
    metadata = crawl_metadata.find_one({'category_url': category_url})
    return metadata['last_crawl_time'] if metadata else datetime.now() - timedelta(days=1)

def update_last_crawl_time(category_url):
    crawl_metadata.update_one({'category_url': category_url}, {'$set': {'last_crawl_time': datetime.now()}}, upsert=True)

def fetch_categories_from_db():
    categories_by_source = {}
    for category in categories_collection.find():
        source_url = category['source']['url']
        if source_url not in categories_by_source:
            categories_by_source[source_url] = []
        categories_by_source[source_url].append(category['url'])
    return categories_by_source

def crawl_source(source_url, categories, articles_collection):
    source_name = next((s['name'].lower().replace(' ', '') for s in SOURCES if s['url'] == source_url), None)
    if not source_name:
        logger.error(f"Không tìm thấy tên nguồn cho URL: {source_url}")
        return

    for category_url in categories:
        last_crawl_time = get_last_crawl_time(category_url)
        logger.info(f"Bắt đầu crawl danh mục: {category_url}, lần crawl cuối: {last_crawl_time}") # Thêm dòng này

        if source_url == "https://vnexpress.net":
            crawl_function = crawl_vnexpress_category
        elif source_url == "https://nhandan.vn":
            crawl_function = crawl_nhandan_category
        elif source_url == "https://tienphong.vn":
            crawl_function = crawl_tienphong_category
        else:
            logger.warning(f"Không có hàm crawl được định nghĩa cho nguồn: {source_url}")
            continue

        crawl_function(category_url, articles_collection, last_crawl_time, get_category_info)
        update_last_crawl_time(category_url)

def crawl_all_sources(articles_collection):
    logger.info(f"Bắt đầu crawl tất cả danh mục lúc {datetime.now()}")
    categories_by_source = fetch_categories_from_db()
    if not categories_by_source:
        logger.info("Không có danh mục nào trong database. Dừng crawl.")
        return
    for source_url, category_urls in categories_by_source.items():
        crawl_source(source_url, category_urls, articles_collection)
    logger.info("Hoàn thành crawl tất cả danh mục.")

def main():
    initialize_sources()
    # Initialize categories only if the collection is empty
    if categories_collection.count_documents({}) == 0:
        pass

    crawl_all_sources(articles_collection) # Lần crawl đầu tiên
    schedule.every(1).minutes.do(crawl_all_sources, articles_collection)
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except Exception as e:
            logger.error(f"Lỗi trong vòng lặp chính: {str(e)}")
            time.sleep(60)

if __name__ == "__main__":
    main()