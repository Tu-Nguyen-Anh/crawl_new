import requests
from bs4 import BeautifulSoup
import schedule
import time
from pymongo import MongoClient
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = MongoClient('mongodb://mongodb:27017/')
db = client['olh_news']
articles_collection = db['articles']
categories_collection = db['categories']
crawl_metadata = db['crawl_metadata']  # Collection lưu thời gian crawl gần nhất

VNEXPRESS_CATEGORIES = {
    'thoi-su': 'Thời sự', 'kinh-doanh': 'Kinh doanh', 'the-gioi': 'Thế giới', 'giai-tri': 'Giải trí',
    'the-thao': 'Thể thao', 'phap-luat': 'Pháp luật', 'giao-duc': 'Giáo dục', 'suc-khoe': 'Sức khỏe',
    'doi-song': 'Đời sống', 'du-lich': 'Du lịch', 'khoa-hoc': 'Khoa học', 'so-hoa': 'Số hóa',
    'oto-xe-may': 'Xe', 'y-kien': 'Ý kiến', 'tam-su':'Tâm sự', 'thu-gian':'Thư giãn', 'bat-dong-san':'Bất động sản', 'goc-nhin':'Góc nhìn'
}

NHANDAN_CATEGORIES = {
    'chinhtri/': 'Chính trị', 'kinhte/': 'Kinh tế', 'xahoi/': 'Xã hội', 'vanhoa/': 'Văn hóa',
    'giaoduc/': 'Giáo dục', 'khoahoc-congnghe/': 'Khoa học - Công nghệ', 'thethao/': 'Thể thao',
    'moi-truong/': 'Môi trường', 'thegioi/': 'Thế giới', 'phapluat/': 'Pháp luật', 'y-te/': 'Y Tế',
    'du-lich/': 'Du lịch', 'factcheck/': 'Kiểm chứng thông tin', 'hanoi/': 'Hà Nội', 'tphcm/': 'Thành phố Hồ Chí Minh',
    'trung-du-va-mien-nui-bac-bo/': 'Trung du và miền núi Bắc Bộ', 'xe/': "Xe"
}

TIENPHONG_CATEGORIES = {
    'dia-oc/': 'Địa ốc', 'kinh-te/': 'Kinh tế', 'song-xanh': 'Sóng xanh', 'giai-tri/': 'Giải trí',
    'the-thao/': 'Thể thao', 'hoa-hau/': 'Hoa hậu', 'suc-khoe/': 'Sức khỏe', 'giao-duc/': 'Giáo dục',
    'phap-luat/': 'Pháp luật', 'van-hoa/': 'Văn hóa', 'hang-khong-du-lich/': 'Hàng không - Du lịch',
    'hanh-trang-nguoi-linh/': 'Hành trang người lính', 'gioi-tre/': 'Giới trẻ', 'ban-doc/': 'Bạn đọc',
    'quizz/': 'quizz/', 'nhip-song-thu-do/': 'Nhịp sống thủ đô', 'toi-nghi/': 'Tôi nghĩ', 
    'nhip-song-phuong-nam/': 'Nhịp sống Phương Nam', 'chuyen-dong-24h/': 'Chuyển động 24h'
}

SOURCES = [
    {"id": 1, "url": "https://vnexpress.net", "name": "VN EXPRESS"},
    {"id": 2, "url": "https://nhandan.vn", "name": "NHAN DAN"},
    {"id": 3, "url": "https://tienphong.vn", "name": "TIEN PHONG"}
]

def initialize_categories():
    all_categories = {**VNEXPRESS_CATEGORIES, **NHANDAN_CATEGORIES, **TIENPHONG_CATEGORIES}
    id_counter = 1
    for code, name in all_categories.items():
        source = next((s for s in SOURCES if code in (VNEXPRESS_CATEGORIES if s['id'] == 1 else NHANDAN_CATEGORIES if s['id'] == 2 else TIENPHONG_CATEGORIES)), None)
        if not source or categories_collection.find_one({'code': code}):
            continue
        category_data = {'id': id_counter, 'code': code, 'name': name, 'source': source}
        categories_collection.insert_one(category_data)
        id_counter += 1

def get_category_info(category_code):
    category = categories_collection.find_one({'code': category_code})
    return category and {k: category[k] for k in ['_id', 'id', 'code', 'name', 'source']}

def get_last_crawl_time(source):
    metadata = crawl_metadata.find_one({'source': source})
    return metadata['last_crawl_time'] if metadata else datetime.now() - timedelta(days=1)

def update_last_crawl_time(source):
    crawl_metadata.update_one({'source': source}, {'$set': {'last_crawl_time': datetime.now()}}, upsert=True)

def crawl_vnexpress_category(category_url, category_code, collection, last_crawl_time):
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
        category_info = get_category_info(category_code)

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
                'title': title, 'link': link, 'description': description, 'content': content, 'category_id': category_info,
                'publish_date': publish_date, 'images': images, 'author': author, 'crawl_date': datetime.now()
            }
            collection.insert_one(article_data)
            logger.info(f"Đã lưu: {title}")

    except Exception as e:
        logger.error(f"Lỗi khi crawl danh mục {category_code}: {str(e)}")
def crawl_nhandan_category(category_url, category_code, collection, last_crawl_time):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        response = requests.get(category_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = soup.find_all('article', class_='story')[:10]
        category_info = get_category_info(category_code)

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
            date_tag = article_soup.find('time', class_='time')
            publish_date = None
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
                'title': title, 'link': link, 'description': description, 'content': content, 'category_id': category_info,
                'publish_date': publish_date, 'images': images, 'author': author, 'crawl_date': datetime.now()
            }
            collection.insert_one(article_data)
            logger.info(f"Đã lưu: {title}")

    except Exception as e:
        logger.error(f"Lỗi khi crawl danh mục {category_code}: {str(e)}")

def crawl_tienphong_category(category_url, category_code, collection, last_crawl_time):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        response = requests.get(category_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = soup.find_all('article', class_='story')[:10]
        category_info = get_category_info(category_code)

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
            if date_tag and date_tag.text.strip():
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
            description = (article_soup.find('div', class_='article__sapo') or article_soup.find('h2', class_='article__sapo') or '').text.strip()
            
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

            author = (article_soup.find('div', class_='article__author') or '').find('span', class_='name cms-author').text.strip() if article_soup.find('div', class_='article__author') else None
            if not author and (meta_author := article_soup.find('meta', property='dable:author')):
                author = meta_author['content']

            article_data = {
                'title': title, 'link': link, 'description': description, 'content': content, 'category_id': category_info,
                'publish_date': publish_date, 'images': images, 'author': author, 'crawl_date': datetime.now(),
            }
            collection.insert_one(article_data)
            logger.info(f"Đã lưu: {title}")

    except Exception as e:
        logger.error(f"Lỗi khi crawl danh mục {category_code}: {str(e)}")

def crawl_all_sources(articles_collection):
    logger.info(f"Bắt đầu crawl lúc {datetime.now()}")
    for base_url, categories, crawl_func, source in [
        ('https://vnexpress.net/', VNEXPRESS_CATEGORIES, crawl_vnexpress_category, 'vnexpress'),
        ('https://nhandan.vn/', NHANDAN_CATEGORIES, crawl_nhandan_category, 'nhandan'),
        ('https://tienphong.vn/', TIENPHONG_CATEGORIES, crawl_tienphong_category, 'tienphong')
    ]:
        last_crawl_time = get_last_crawl_time(source)
        for code in categories:
            crawl_func(f"{base_url}{code}", code, articles_collection, last_crawl_time)
        update_last_crawl_time(source)
    logger.info("Hoàn thành crawl")


# def crawl_all_sources(articles_collection):
#     logger.info(f"Bắt đầu crawl lúc {datetime.now()}")
#     for base_url, categories, crawl_func, source in [
#         ('https://vnexpress.net/', VNEXPRESS_CATEGORIES, crawl_vnexpress_category, 'vnexpress'),
#         ('https://nhandan.vn/', NHANDAN_CATEGORIES, crawl_nhandan_category, 'nhandan'),
#         ('https://tienphong.vn/', TIENPHONG_CATEGORIES, crawl_tienphong_category, 'tienphong')
#     ]:
#         # Đặt last_crawl_time là 3 ngày trước ngay trong hàm
#         last_crawl_time = datetime.now() - timedelta(days=3)
#         for code in categories:
#             crawl_func(f"{base_url}{code}", code, articles_collection, last_crawl_time)
#         update_last_crawl_time(source)
#     logger.info("Hoàn thành crawl")

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