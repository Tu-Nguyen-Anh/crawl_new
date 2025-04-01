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

# Import các crawler riêng biệt
import tienphong_crawler
import nhandan_crawler
import vnexpress_crawler

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kết nối MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['olh_news']
articles_collection = db['articles']
categories_collection = db['categories']
sources_collection = db['sources']
crawl_metadata = db['crawl_metadata']

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Mobile/15E148 Safari/604.1'
]

def get_random_headers():
    return {'User-Agent': random.choice(USER_AGENTS)}

def get_sources():
    return list(sources_collection.find())

def get_categories():
    categories = list(categories_collection.find())
    logger.info("Danh sách categories hiện tại trong database:")
    for cat in categories:
        keywords_str = ', '.join(cat.get('keyword', [])) if cat.get('keyword') else 'None'
        logger.info(f"- {cat['url']} (Tên: {cat['name']}, Nguồn: {cat['source']['name']}, Keywords: {keywords_str})")
    return categories

def get_source_from_url(url):
    sources = get_sources()
    for source in sources:
        if source['url'] in url:
            return source
    return None

def get_last_crawl_time(category_url):
    metadata = crawl_metadata.find_one({'category_url': category_url})
    return metadata['last_crawl_time'] if metadata else datetime.now() - timedelta(days=1)

def update_last_crawl_time(category_url):
    crawl_metadata.update_one(
        {'category_url': category_url},
        {'$set': {'last_crawl_time': datetime.now()}},
        upsert=True
    )

def get_category_info(category_url):
    category = categories_collection.find_one({'url': category_url})
    if category:
        # Đảm bảo trường 'keyword' luôn tồn tại, mặc định là danh sách rỗng nếu không có
        return {
            '_id': category['_id'],
            'name': category['name'],
            'source': category['source'],
            'url': category['url'],
            'keyword': category.get('keyword', [])
        }
    source = get_source_from_url(category_url)
    if source:
        return {
            '_id': str(uuid.uuid4()),
            'name': 'Unknown',
            'source': source,
            'url': category_url,
            'keyword': []
        }
    logger.error(f"Không tìm thấy danh mục cho URL: {category_url}")
    return None

def extract_article_urls(category_url):
    try:
        response = requests.get(category_url, headers=get_random_headers(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        article_urls = []
        source = get_source_from_url(category_url)
        base_url = source['url'] if source else 'https://' + category_url.split('/')[2]

        if 'cafebiz.vn' in category_url:
            news_boxes = soup.find_all('div', class_='cfbiznews_box')
            for box in news_boxes:
                a_tag = box.find('a', href=True)
                if a_tag:
                    href = a_tag['href']
                    if href.endswith('.chn') and re.search(r'\d{10,}', href):
                        full_url = href if href.startswith('http') else f"{base_url}{href}"
                        article_urls.append(full_url)
        elif 'vneconomy.vn' in category_url:
            story_articles = soup.find_all('article', class_='story')
            for article in story_articles:
                a_tag = article.find('a', href=True)
                if a_tag:
                    href = a_tag['href']
                    if href.endswith('.htm') and not re.search(r'^/[a-z-]+\.htm$', href):
                        full_url = href if href.startswith('http') else f"{base_url}{href}"
                        article_urls.append(full_url)
        elif 'thanhnien.vn' in category_url:
            story_items = soup.find_all(['div', 'article'], class_=re.compile('box-category-item|item-first|item-related|list__focus|box-category-middle'))
            for item in story_items:
                a_tag = item.find('a', href=True)
                if a_tag:
                    href = a_tag['href']
                    if href and href.endswith('.htm') and re.search(r'-\d{15,}\.htm$', href):
                        full_url = href if href.startswith('http') else f"{base_url}{href}"
                        article_urls.append(full_url)
        elif 'tuoitre.vn' in category_url:
            story_items = soup.find_all(['div', 'li'], class_=re.compile('box-category-item-main|item-first|item-related|box-category-item|box-category-middle'))
            for item in story_items:
                a_tag = item.find('a', href=True)
                if a_tag:
                    href = a_tag['href']
                    if href and href.endswith('.htm') and re.search(r'-\d{14}\.htm$', href):
                        full_url = href if href.startswith('http') else f"{base_url}{href}"
                        article_urls.append(full_url)
        elif 'tinnhanhchungkhoan.vn' in category_url:
            story_articles = soup.find_all('article', class_='story')
            for article in story_articles:
                a_tag = article.find('a', href=True)
                if a_tag:
                    href = a_tag['href']
                    if (href and '-post' in href and re.search(r'-post\d+\.html$', href) and
                        (article.find('div', class_='story__meta') or article.find('figure'))):
                        full_url = href if href.startswith('http') else f"{base_url}{href}"
                        article_urls.append(full_url)

        if not article_urls:
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if not href or 'javascript' in href or 'zalo.me' in href or 'facebook.com' in href:
                    continue
                if 'tuoitre.vn' in category_url and href.endswith('.htm') and re.search(r'-\d{14}\.htm$', href):
                    full_url = href if href.startswith('http') else f"{base_url}{href}"
                    article_urls.append(full_url)
                elif 'thanhnien.vn' in category_url and href.endswith('.htm') and re.search(r'-\d{15,}\.htm$', href):
                    full_url = href if href.startswith('http') else f"{base_url}{href}"
                    article_urls.append(full_url)
                elif 'laodong.vn' in category_url and href.endswith('.ldo') and re.search(r'-\d{15,}\.ldo$', href):
                    full_url = href if href.startswith('http') else f"{base_url}{href}"
                    article_urls.append(full_url)
                elif 'tinnhanhchungkhoan.vn' in category_url and '-post' in href and re.search(r'-post\d+\.html$', href):
                    parent_article = a_tag.find_parent('article', class_='story')
                    if parent_article and (parent_article.find('div', class_='story__meta') or parent_article.find('figure')):
                        full_url = href if href.startswith('http') else f"{base_url}{href}"
                        article_urls.append(full_url)
                elif re.match(r'.*\.html$|/.*-\d+$', href):
                    full_url = href if href.startswith('http') else f"{base_url}{href}"
                    article_urls.append(full_url)

        unique_urls = list(set(article_urls))[:30]
        logger.info(f"Tìm thấy {len(unique_urls)} URL từ {category_url}: {unique_urls}")
        return unique_urls
    except Exception as e:
        logger.error(f"Lỗi khi trích xuất URL từ {category_url}: {str(e)}")
        return []


def check_keywords_match(title, content, keywords):
    """Kiểm tra xem có từ khóa nào trong keywords xuất hiện chính xác trong title hoặc content"""
    # Log danh sách từ khóa nhận được để kiểm tra
    logger.info(f"Danh sách từ khóa nhận được: {keywords}")

    if not keywords:  # Nếu không có từ khóa, trả về True (lấy tất cả bài viết)
        logger.info("Không có từ khóa nào trong danh sách, sẽ lấy tất cả bài viết")
        return True

    # Kết hợp title và content thành một chuỗi
    full_text = f"{title}\n{content}"
    lines = full_text.split('\n')

    for i, line in enumerate(lines, 1):
        for keyword in keywords:
            # Sử dụng regex với \b để khớp từ độc lập, giữ nguyên dấu và chữ cái
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, line):
                location = "title" if i == 1 else "content"
                logger.info(f"Khớp keyword '{keyword}' tại {location}, dòng {i}: {line.strip()}")
                return True
    logger.info(f"Không tìm thấy từ khóa nào trong '{title}' hoặc nội dung")
    return False

def parse_article(article_url, category_info, last_crawl_time):
    try:
        session = requests.Session()
        retry_strategy = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)

        response = session.get(article_url, headers=get_random_headers(), timeout=30)
        response.raise_for_status()

        article = Article(article_url, language='vi')
        article.set_html(response.text)
        article.parse()

        title = article.title
        if not title:
            soup = BeautifulSoup(response.text, 'html.parser')
            title_tag = soup.find('title')
            title = title_tag.get_text(strip=True) if title_tag else None
            if not title:
                logger.info(f"Bài viết {article_url} không có title, bỏ qua")
                return None

        content = article.text.strip() if article.text else ''
        if not content or len(content.split()) < 50:
            logger.info(f"Bài viết {article_url} không có nội dung hoặc quá ngắn, bỏ qua")
            return None

        # Kiểm tra từ khóa trước khi tiếp tục
        if not check_keywords_match(title, content, category_info['keyword']):
            logger.info(f"Bài viết {article_url} không chứa keyword nào trong {category_info['keyword']}")
            return None

        # Tiếp tục xử lý và trả về dữ liệu bài viết
        publish_date = article.publish_date or datetime.now()
        try:
            publish_date = parser.parse(str(publish_date)).replace(tzinfo=None)
        except Exception:
            publish_date = datetime.now()

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
            'author': None,  # Tạm bỏ qua phần tác giả để tập trung vào từ khóa
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

    if 'tienphong.vn' in category_url:
        tienphong_crawler.crawl_tienphong_category(category_url, articles_collection, last_crawl_time, get_category_info)
    elif 'nhandan.vn' in category_url:
        nhandan_crawler.crawl_nhandan_category(category_url, articles_collection, last_crawl_time, get_category_info)
    elif 'vnexpress.net' in category_url:
        vnexpress_crawler.crawl_vnexpress_category(category_url, articles_collection, last_crawl_time, get_category_info)
    else:
        logger.info(f"Bắt đầu crawl danh mục: {category_url}, lần crawl cuối: {last_crawl_time}")
        article_urls = extract_article_urls(category_url)

        for url in article_urls:
            if articles_collection.find_one({'link': url}):
                continue
            article_data = parse_article(url, category_info, last_crawl_time)
            if article_data:
                articles_collection.insert_one(article_data)
                logger.info(f"Đã lưu: {article_data['title']}")

    update_last_crawl_time(category_url)

def crawl_all_categories(articles_collection):
    categories = get_categories()
    logger.info(f"Bắt đầu crawl tất cả danh mục lúc {datetime.now()}")
    for category in categories:
        crawl_category(category['url'], articles_collection)
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