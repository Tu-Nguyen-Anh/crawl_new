import uuid
import requests
from bs4 import BeautifulSoup
import schedule
import time
from pymongo import MongoClient
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = MongoClient('mongodb://localhost:27017/')
db = client['olh_news']
articles_collection = db['articles']
categories_collection = db['categories']
sources_collection = db['sources']
crawl_metadata = db['crawl_metadata']

# Updated category dictionaries with full URLs as keys
VNEXPRESS_CATEGORIES = {
    'https://vnexpress.net/thoi-su': 'Thời sự',
    'https://vnexpress.net/kinh-doanh': 'Kinh doanh',
    'https://vnexpress.net/the-gioi': 'Thế giới',
    'https://vnexpress.net/giai-tri': 'Giải trí',
    'https://vnexpress.net/the-thao': 'Thể thao',
    'https://vnexpress.net/phap-luat': 'Pháp luật',
    'https://vnexpress.net/giao-duc': 'Giáo dục',
    'https://vnexpress.net/suc-khoe': 'Sức khỏe',
    'https://vnexpress.net/doi-song': 'Đời sống',
    'https://vnexpress.net/du-lich': 'Du lịch',
    'https://vnexpress.net/khoa-hoc': 'Khoa học',
    'https://vnexpress.net/so-hoa': 'Số hóa',
    'https://vnexpress.net/oto-xe-may': 'Xe',
    'https://vnexpress.net/y-kien': 'Ý kiến',
    'https://vnexpress.net/tam-su': 'Tâm sự',
    'https://vnexpress.net/thu-gian': 'Thư giãn',
    'https://vnexpress.net/bat-dong-san': 'Bất động sản',
    'https://vnexpress.net/goc-nhin': 'Góc nhìn',
    'https://vnexpress.net/tin-tuc-24h': 'Tin tức 24h',
    'https://vnexpress.net/tin-nong': 'Tin nóng'
}

NHANDAN_CATEGORIES = {
    'https://nhandan.vn/chinhtri/': 'Chính trị',
    'https://nhandan.vn/kinhte/': 'Kinh tế',
    'https://nhandan.vn/xahoi/': 'Xã hội',
    'https://nhandan.vn/vanhoa/': 'Văn hóa',
    'https://nhandan.vn/giaoduc/': 'Giáo dục',
    'https://nhandan.vn/khoahoc-congnghe/': 'Khoa học - Công nghệ',
    'https://nhandan.vn/thethao/': 'Thể thao',
    'https://nhandan.vn/moi-truong/': 'Môi trường',
    'https://nhandan.vn/thegioi/': 'Thế giới',
    'https://nhandan.vn/phapluat/': 'Pháp luật',
    'https://nhandan.vn/y-te/': 'Y Tế',
    'https://nhandan.vn/du-lich/': 'Du lịch',
    'https://nhandan.vn/factcheck/': 'Kiểm chứng thông tin',
    'https://nhandan.vn/hanoi/': 'Hà Nội',
    'https://nhandan.vn/tphcm/': 'Thành phố Hồ Chí Minh',
    'https://nhandan.vn/trung-du-va-mien-nui-bac-bo/': 'Trung du và miền núi Bắc Bộ'
}

TIENPHONG_CATEGORIES = {
    'https://tienphong.vn/dia-oc/': 'Địa ốc',
    'https://tienphong.vn/kinh-te/': 'Kinh tế',
    'https://tienphong.vn/song-xanh': 'Sóng xanh',
    'https://tienphong.vn/giai-tri/': 'Giải trí',
    'https://tienphong.vn/the-thao/': 'Thể thao',
    'https://tienphong.vn/hoa-hau/': 'Hoa hậu',
    'https://tienphong.vn/suc-khoe/': 'Sức khỏe',
    'https://tienphong.vn/giao-duc/': 'Giáo dục',
    'https://tienphong.vn/phap-luat/': 'Pháp luật',
    'https://tienphong.vn/van-hoa/': 'Văn hóa',
    'https://tienphong.vn/hang-khong-du-lich/': 'Hàng không - Du lịch',
    'https://tienphong.vn/hanh-trang-nguoi-linh/': 'Hành trang người lính',
    'https://tienphong.vn/gioi-tre/': 'Giới trẻ',
    'https://tienphong.vn/ban-doc/': 'Bạn đọc',
    'https://tienphong.vn/quizz/': 'quizz/',
    'https://tienphong.vn/nhip-song-thu-do/': 'Nhịp sống thủ đô',
    'https://tienphong.vn/toi-nghi/': 'Tôi nghĩ',
    'https://tienphong.vn/nhip-song-phuong-nam/': 'Nhịp sống Phương Nam',
    'https://tienphong.vn/chuyen-dong-24h/': 'Chuyển động 24h',
    'https://tienphong.vn/xe/': 'Xe'
}

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
    all_categories = {**VNEXPRESS_CATEGORIES, **NHANDAN_CATEGORIES, **TIENPHONG_CATEGORIES}
    for url, name in all_categories.items():
        source = next((s for s in SOURCES if url.startswith(s['url'])), None)
        if not source or categories_collection.find_one({'url': url}):
            continue
        category_data = {
            '_id': str(uuid.uuid4()),
            'name': name,
            'source': source,  # Lưu toàn bộ object source thay vì chỉ source_id
            'url': url
        }
        categories_collection.insert_one(category_data)

def get_category_info(category_url):
    category = categories_collection.find_one({'url': category_url})
    if category:
        return {
            '_id': category['_id'],
            'name': category['name'],
            'source': category['source'],  # Trả về source đã được nhúng
            'url': category['url']
        }
    return None

def get_last_crawl_time(source):
    metadata = crawl_metadata.find_one({'source': source})
    return metadata['last_crawl_time'] if metadata else datetime.now() - timedelta(days=1)

def update_last_crawl_time(source):
    crawl_metadata.update_one({'source': source}, {'$set': {'last_crawl_time': datetime.now()}}, upsert=True)

def crawl_vnexpress_category(category_url, collection, last_crawl_time):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5'
    }
    try:
        response = requests.get(category_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = soup.find_all('article', class_='item-news')[:10]
        category_info = get_category_info(category_url)

        for article in articles:
            title_tag = article.find('h3', class_='title-news')
            if not title_tag:
                continue
            link = title_tag.find('a')['href']
            if collection.find_one({'link': link}):
                continue

            article_response = requests.get(link, headers=headers)
            article_soup = BeautifulSoup(article_response.content, 'html.parser')
            publish_date = None
            date_tag = article_soup.find('span', class_='date')
            if date_tag:
                try:
                    date_clean = date_tag.text.strip().split(' (GMT')[0].split(', ', 1)[1]
                    publish_date = datetime.strptime(date_clean, '%d/%m/%Y, %H:%M')
                except (ValueError, IndexError):
                    continue
            if publish_date and publish_date <= last_crawl_time:
                continue

            title = title_tag.text.strip()
            description_tag = article_soup.find('p', class_='description')
            description = description_tag.text.strip() if description_tag else ''
            content_tag = article_soup.find('article', class_='fck_detail')
            content = content_tag.get_text(separator='\n', strip=True) if content_tag else ''
            images = [img.get('data-src') or img.get('src') for img in article_soup.find_all('img', class_='lazy')
                     if (img.get('data-src') or img.get('src')) and (img.get('data-src') or img.get('src')).startswith('http')]

            author = None
            for tag in [article_soup.find('p', class_='author'), article_soup.find('strong', class_='author')]:
                if tag:
                    author = tag.text.strip()
                    break
            if not author and content:
                last_p = article_soup.find('p', class_='Normal', attrs={'style': 'text-align:right;'})
                author = last_p.find('strong').text.strip() if last_p and last_p.find('strong') else ' '.join(content.split()[-2:])

            article_data = {
                '_id': str(uuid.uuid4()),
                'title': title,
                'link': link,
                'description': description,
                'content': content,
                'category': category_info,
                'publish_date': publish_date,
                'images': images,
                'author': author,
                'crawl_date': datetime.now()
            }
            collection.insert_one(article_data)
            logger.info(f"Đã lưu: {title}")

    except Exception as e:
        logger.error(f"Lỗi khi crawl danh mục {category_url}: {str(e)}")

def crawl_nhandan_category(category_url, collection, last_crawl_time):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        response = requests.get(category_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = soup.find_all('article', class_='story')[:10]
        category_info = get_category_info(category_url)

        for article in articles:
            title_tag = article.find(['h2', 'h3', 'h4'], class_='story__heading')
            if not title_tag:
                continue
            link = title_tag.find('a', class_='cms-link')['href']
            link = 'https://nhandan.vn' + link if not link.startswith('http') else link
            if collection.find_one({'link': link}):
                continue

            article_response = requests.get(link, headers=headers)
            article_soup = BeautifulSoup(article_response.content, 'html.parser')
            publish_date = None
            date_tag = article_soup.find('time', class_='time')
            if date_tag:
                try:
                    date_clean = date_tag.text.strip().split('ngày ')[1].split(' - ')[0].strip()
                    time_clean = date_tag.text.strip().split(' - ')[1].strip()
                    publish_date = datetime.strptime(f"{date_clean} {time_clean}", '%d/%m/%Y %H:%M')
                except (ValueError, IndexError):
                    continue
            if publish_date and publish_date <= last_crawl_time:
                continue

            title = title_tag.text.strip()
            description = (article_soup.find('div', class_='article__sapo') or '').text.strip()
            content_div = article_soup.find('div', class_='article__body')
            content = content_div.get_text(separator='\n', strip=True) if content_div else ''
            if content_div:
                for unwanted in content_div.find_all(['script', 'style', 'table', 'div', 'aside']):
                    unwanted.decompose()
            images = [img.get('data-src') or img.get('src') for img in article_soup.find_all('img')
                     if (img.get('data-src') or img.get('src')) and (img.get('data-src') or img.get('src')).startswith('http')]

            author = None
            author_source = article_soup.find('div', class_='article__author-source')
            if author_source:
                for tag in [author_source.find('a', class_='name'), author_source.find('p', class_='name'), author_source.find('span', class_='name')]:
                    if tag:
                        author = tag.text.strip()
                        break
            if not author and content_div and content_div.find_all('p'):
                last_p = content_div.find_all('p')[-1]
                author = last_p.text.split('-')[-1].strip() if '-' in last_p.text else None

            article_data = {
                '_id': str(uuid.uuid4()),
                'title': title,
                'link': link,
                'description': description,
                'content': content,
                'category': category_info,
                'publish_date': publish_date,
                'images': images,
                'author': author,
                'crawl_date': datetime.now()
            }
            collection.insert_one(article_data)
            logger.info(f"Đã lưu: {title}")

    except Exception as e:
        logger.error(f"Lỗi khi crawl danh mục {category_url}: {str(e)}")

def crawl_tienphong_category(category_url, collection, last_crawl_time):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        response = requests.get(category_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = soup.find_all('article', class_='story')[:10]
        category_info = get_category_info(category_url)

        for article in articles:
            title_tag = article.find(['h2', 'h3', 'h5'], class_='story__heading')
            if not title_tag:
                continue
            link = title_tag.find('a', class_='cms-link')['href']
            link = 'https://tienphong.vn' + link if not link.startswith('http') else link
            if collection.find_one({'link': link}):
                continue

            article_response = requests.get(link, headers=headers)
            article_soup = BeautifulSoup(article_response.content, 'html.parser')
            publish_date = None
            date_tag = article_soup.select_one('div.article__meta time')
            if date_tag and hasattr(date_tag, 'text') and date_tag.text.strip():
                try:
                    publish_date = datetime.strptime(date_tag.text.strip(), '%d/%m/%Y | %H:%M')
                except ValueError:
                    if 'datetime' in date_tag.attrs:
                        publish_date = datetime.strptime(date_tag['datetime'], '%Y-%m-%dT%H:%M:%S%z')
            if not publish_date and (meta_date := article_soup.find('meta', property='article:published_time')):
                publish_date = datetime.strptime(meta_date['content'], '%Y-%m-%dT%H:%M:%S%z')
            if publish_date and publish_date <= last_crawl_time:
                continue

            title = title_tag.text.strip()
            description_tag = article_soup.find('div', class_='article__sapo') or article_soup.find('h2', class_='article__sapo')
            description = description_tag.text.strip() if description_tag else ''
            content_div = article_soup.find('div', class_='article__body')
            if content_div:
                for unwanted in content_div.find_all(['script', 'style', 'aside', 'div', 'table']):
                    unwanted.decompose()
                content = content_div.get_text(separator='\n', strip=True)
            else:
                content = ''
            images = [img['data-src'] for img in (content_div or article_soup).find_all('img', {'data-src': True})
                     if img['data-src'].startswith('http')] or ([article_soup.find('meta', property='og:image')['content']]
                     if article_soup.find('meta', property='og:image') else [])

            author_div = article_soup.find('div', class_='article__author')
            author = author_div.find('span', class_='name cms-author').text.strip() if author_div and author_div.find('span', class_='name cms-author') else None
            if not author and (meta_author := article_soup.find('meta', property='dable:author')):
                author = meta_author['content']

            article_data = {
                '_id': str(uuid.uuid4()),
                'title': title,
                'link': link,
                'description': description,
                'content': content,
                'category': category_info,
                'publish_date': publish_date,
                'images': images,
                'author': author,
                'crawl_date': datetime.now()
            }
            collection.insert_one(article_data)
            logger.info(f"Đã lưu: {title}")

    except Exception as e:
        logger.error(f"Lỗi khi crawl danh mục {category_url}: {str(e)}")

def crawl_all_sources(articles_collection):
    logger.info(f"Bắt đầu crawl lúc {datetime.now()}")
    for categories, crawl_func, source in [
        (VNEXPRESS_CATEGORIES, crawl_vnexpress_category, 'vnexpress'),
        (NHANDAN_CATEGORIES, crawl_nhandan_category, 'nhandan'),
        (TIENPHONG_CATEGORIES, crawl_tienphong_category, 'tienphong')
    ]:
        last_crawl_time = get_last_crawl_time(source)
        for url in categories.keys():
            crawl_func(url, articles_collection, last_crawl_time)
        update_last_crawl_time(source)
    logger.info("Hoàn thành crawl")

def main():
    initialize_categories()
    crawl_all_sources(articles_collection)
    schedule.every(5).minutes.do(crawl_all_sources, articles_collection)
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except Exception as e:
            logger.error(f"Lỗi trong vòng lặp chính: {str(e)}")
            time.sleep(60)

if __name__ == "__main__":
    main()