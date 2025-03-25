import requests
from bs4 import BeautifulSoup
import schedule
import time
from pymongo import MongoClient
from datetime import datetime
import logging

# Thiết lập logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kết nối MongoDB
client = MongoClient('mongodb://10.8.0.1:27017/')
db = client['olh_news']
articles_collection = db['articles']  # Collection cho bài viết
categories_collection = db['categories']  # Collection cho danh mục

# Danh sách danh mục từ các báo
VNEXPRESS_CATEGORIES = {
    'thoi-su': 'Thời sự',
    'kinh-doanh': 'Kinh doanh',
    'the-gioi': 'Thế giới',
    'giai-tri': 'Giải trí',
    'the-thao': 'Thể thao',
    'phap-luat': 'Pháp luật',
    'giao-duc': 'Giáo dục',
    'suc-khoe': 'Sức khỏe',
    'doi-song': 'Đời sống',
    'du-lich': 'Du lịch',
    'khoa-hoc': 'Khoa học',
    'so-hoa': 'Số hóa',
    'xe': 'Xe',
    'y-kien': 'Ý kiến'
}

NHANDAN_CATEGORIES = {
    'chinhtri/': 'Chính trị',
    'kinhte/': 'Kinh tế',
    'xahoi/': 'Xã hội',
    'vanhoa/': 'Văn hóa',
    'giaoduc/': 'Giáo dục',
    'khoahoc-congnghe/': 'Khoa học - Công nghệ',
    'thethao/': 'Thể thao',
    'moi-truong/': 'Môi trường',
    'thegioi/': 'Thế giới',
    'phapluat/': 'Pháp luật',
    'y-te/': 'Y Tế',
    'du-lich/': 'Du lịch'
}

LAODONG_CATEGORIES = {
    'thoi-su': 'Thời sự',
    'kinh-te': 'Kinh tế',
    'xa-hoi': 'Xã hội',
    'van-hoa-giai-tri': 'Văn hóa - Giải trí',
    'the-thao': 'Thể thao',
    'lao-dong-viec-lam': 'Lao động - Việc làm',
    'suc-khoe': 'Sức khỏe',
    'giao-duc': 'Giáo dục',
    'phap-luat': 'Pháp luật',
    'cong-doan': 'Công đoàn'
}

# Hàm khởi tạo danh mục vào bảng categories
def initialize_categories():
    # Xóa dữ liệu cũ trong bảng categories (nếu cần)
    # categories_collection.delete_many({})
    
    # Tạo danh sách tất cả danh mục từ các nguồn
    all_categories = {}
    all_categories.update(VNEXPRESS_CATEGORIES)
    all_categories.update(NHANDAN_CATEGORIES)
    all_categories.update(LAODONG_CATEGORIES)
    
    # Khởi tạo ID từ 1
    id_counter = 1
    for code, name in all_categories.items():
        # Kiểm tra xem danh mục đã tồn tại chưa
        if not categories_collection.find_one({'code': code}):
            category_data = {
                'id': id_counter,
                'code': code,
                'name': name
            }
            categories_collection.insert_one(category_data)
            logger.info(f"Đã thêm danh mục: {name} (code: {code}, id: {id_counter})")
            id_counter += 1

# Hàm crawl cho VnExpress
def crawl_vnexpress_category(category_url, category_code, collection, categories_collection):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(category_url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = soup.find_all('article', class_='item-news')
        
        # Tìm category_id từ categories_collection
        category = categories_collection.find_one({'code': category_code})
        category_id = category['id'] if category else None
        
        for article in articles:
            try:
                title_tag = article.find('h3', class_='title-news')
                if not title_tag:
                    continue
                    
                title = title_tag.text.strip()
                link = title_tag.find('a')['href']
                
                if collection.find_one({'link': link}):
                    logger.info(f"Bài viết đã tồn tại: {title}")
                    continue
                
                article_response = requests.get(link, headers=headers)
                article_soup = BeautifulSoup(article_response.content, 'html.parser')
                
                description = article_soup.find('p', class_='description')
                description = description.text.strip() if description else ''
                
                content = article_soup.find('article', class_='fck_detail')
                content_text = content.get_text(separator='\n', strip=True) if content else ''
                
                publish_date = None
                date_tag = article_soup.find('span', class_='date')
                if date_tag:
                    publish_date_str = date_tag.text.strip()
                    try:
                        date_clean = publish_date_str.split(' (GMT')[0]
                        date_parts = date_clean.split(', ', 1)[1]
                        publish_date = datetime.strptime(date_parts, '%d/%m/%Y, %H:%M')
                    except ValueError as e:
                        logger.warning(f"Không thể parse ngày: {publish_date_str}, lỗi: {e}")
                
                images = [img.get('data-src') or img.get('src') for img in article_soup.find_all('img', class_='lazy') if (img.get('data-src') or img.get('src')) and (img.get('data-src') or img.get('src')).startswith('http')]
                
                author = None
                author_tag = article_soup.find('p', class_='author')
                if author_tag:
                    author = author_tag.text.strip()
                else:
                    strong_author = article_soup.find('strong', class_='author')
                    if strong_author:
                        author = strong_author.text.strip()
                    elif content:
                        last_p = content.find_all('p')[-1] if content.find_all('p') else None
                        if last_p and '-' in last_p.text:
                            author = last_p.text.split('-')[-1].strip()
                
                article_data = {
                    'title': title,
                    'link': link,
                    'description': description,
                    'content': content_text,
                    'category_id': category_id,  # Lưu category_id thay vì category_name
                    'publish_date': publish_date,
                    'images': images,
                    'author': author,
                    'crawl_date': datetime.now(),
                    'source': 'vnexpress'
                }
                
                collection.insert_one(article_data)
                logger.info(f"Đã lưu: {title} - Tác giả: {author} - Category ID: {category_id}")
                
            except Exception as e:
                logger.error(f"Lỗi khi crawl bài viết {link}: {str(e)}")
                
    except Exception as e:
        logger.error(f"Lỗi khi crawl danh mục {category_code}: {str(e)}")

# Hàm crawl cho Nhân Dân
def crawl_nhandan_category(category_url, category_code, collection, categories_collection):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(category_url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = soup.find_all('article', class_='story')
        
        # Tìm category_id từ categories_collection
        category = categories_collection.find_one({'code': category_code})
        category_id = category['id'] if category else None
        
        if not articles:
            logger.warning(f"Không tìm thấy bài viết nào trong danh mục {category_code}")
            return
        
        for article in articles:
            try:
                title_tag = article.find(['h2', 'h3', 'h4'], class_='story__heading')
                if not title_tag:
                    continue
                    
                title = title_tag.text.strip()
                link_tag = title_tag.find('a', class_='cms-link')
                if not link_tag or 'href' not in link_tag.attrs:
                    continue
                link = link_tag['href']
                if not link.startswith('http'):
                    link = 'https://nhandan.vn' + link
                
                if collection.find_one({'link': link}):
                    logger.info(f"Bài viết đã tồn tại: {title}")
                    continue
                
                article_response = requests.get(link, headers=headers)
                article_soup = BeautifulSoup(article_response.content, 'html.parser')
                
                description_tag = article_soup.find('h2', class_='article-sapo')
                description = description_tag.text.strip() if description_tag else ''
                
                content = ''
                content_div = article_soup.find('div', class_='detail-content-body') or article_soup.find('div', class_='detail-content') or article_soup.find('article')
                if content_div:
                    for unwanted in content_div.find_all(['script', 'style', 'figure', 'figcaption', 'aside']):
                        unwanted.decompose()
                    content = content_div.get_text(separator='\n', strip=True)
                    if not content:
                        logger.warning(f"Nội dung trống sau khi xử lý cho bài viết: {link}")
                else:
                    logger.warning(f"Không tìm thấy khối nội dung cho bài viết: {link}")
                
                if content:
                    logger.info(f"Nội dung lấy được cho {link}: {content[:200]}...")
                else:
                    logger.warning(f"Nội dung rỗng cho bài viết: {link}")
                
                publish_date = None
                date_tag = article_soup.find('time')
                if date_tag:
                    publish_date_str = date_tag.text.strip()
                    try:
                        date_clean = publish_date_str.split('ngày ')[1]
                        publish_date = datetime.strptime(date_clean, '%d/%m/%Y %H:%M')
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Không thể parse ngày: {publish_date_str}, lỗi: {e}")
                
                images = [img.get('data-src') or img.get('src') for img in article_soup.find_all('img') if (img.get('data-src') or img.get('src')) and (img.get('data-src') or img.get('src')).startswith('http')]
                
                author = None
                author_tag = article_soup.find('div', class_='author-name')
                if author_tag:
                    author = author_tag.text.strip()
                elif content_div and content_div.find_all('p'):
                    last_p = content_div.find_all('p')[-1]
                    if last_p and '-' in last_p.text:
                        author = last_p.text.split('-')[-1].strip()
                
                article_data = {
                    'title': title,
                    'link': link,
                    'description': description,
                    'content': content,
                    'category_id': category_id,  # Lưu category_id thay vì category_name
                    'publish_date': publish_date,
                    'images': images,
                    'author': author,
                    'crawl_date': datetime.now(),
                    'source': 'nhandan'
                }
                
                collection.insert_one(article_data)
                logger.info(f"Đã lưu: {title} - Tác giả: {author} - Category ID: {category_id}")
                
            except Exception as e:
                logger.error(f"Lỗi khi crawl bài viết {link}: {str(e)}")
                
    except Exception as e:
        logger.error(f"Lỗi khi crawl danh mục {category_code}: {str(e)}")

# Hàm crawl cho Lao Động
def crawl_laodong_category(category_url, category_code, collection, categories_collection):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(category_url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = soup.find_all('article', class_='article-item')
        
        # Tìm category_id từ categories_collection
        category = categories_collection.find_one({'code': category_code})
        category_id = category['id'] if category else None
        
        for article in articles:
            try:
                title_tag = article.find('h3', class_='article-title')
                if not title_tag:
                    continue
                    
                title = title_tag.text.strip()
                link = title_tag.find('a')['href']
                if not link.startswith('http'):
                    link = 'https://laodong.vn' + link
                
                if collection.find_one({'link': link}):
                    logger.info(f"Bài viết đã tồn tại: {title}")
                    continue
                
                article_response = requests.get(link, headers=headers)
                article_soup = BeautifulSoup(article_response.content, 'html.parser')
                
                description = article_soup.find('h2', class_='article-excerpt')
                description = description.text.strip() if description else ''
                
                content = article_soup.find('div', class_='article-content')
                content_text = content.get_text(separator='\n', strip=True) if content else ''
                
                publish_date = None
                date_tag = article_soup.find('time', class_='article-published')
                if date_tag:
                    publish_date_str = date_tag.text.strip()
                    try:
                        publish_date = datetime.strptime(publish_date_str, '%H:%M %d/%m/%Y')
                    except ValueError as e:
                        logger.warning(f"Không thể parse ngày: {publish_date_str}, lỗi: {e}")
                
                images = [img.get('src') for img in article_soup.find_all('img') if img.get('src') and img.get('src').startswith('http')]
                
                author = None
                author_tag = article_soup.find('div', class_='article-author')
                if author_tag:
                    author = author_tag.text.strip()
                
                article_data = {
                    'title': title,
                    'link': link,
                    'description': description,
                    'content': content_text,
                    'category_id': category_id,  # Lưu category_id thay vì category_name
                    'publish_date': publish_date,
                    'images': images,
                    'author': author,
                    'crawl_date': datetime.now(),
                    'source': 'laodong'
                }
                
                collection.insert_one(article_data)
                logger.info(f"Đã lưu: {title} - Tác giả: {author} - Category ID: {category_id}")
                
            except Exception as e:
                logger.error(f"Lỗi khi crawl bài viết {link}: {str(e)}")
                
    except Exception as e:
        logger.error(f"Lỗi khi crawl danh mục {category_code}: {str(e)}")

# Hàm tổng hợp crawl từ cả 3 nguồn
def crawl_all_sources(articles_collection, categories_collection):
    logger.info(f"Bắt đầu crawl lúc {datetime.now()}")
    
    # Crawl VnExpress 
    vnexpress_base_url = 'https://vnexpress.net/'
    for category_code, _ in VNEXPRESS_CATEGORIES.items():
        category_url = f"{vnexpress_base_url}{category_code}"
        logger.info(f"Đang crawl danh mục VnExpress: {category_code}")
        crawl_vnexpress_category(category_url, category_code, articles_collection, categories_collection)
    
    # Crawl Nhân Dân
    # nhandan_base_url = 'https://nhandan.vn/'
    # for category_code, _ in NHANDAN_CATEGORIES.items():
    #     category_url = f"{nhandan_base_url}{category_code}"
    #     logger.info(f"Đang crawl danh mục Nhân Dân: {category_code}")
    #     crawl_nhandan_category(category_url, category_code, articles_collection, categories_collection)
    
    # Crawl Lao Động
    # laodong_base_url = 'https://laodong.vn/'
    # for category_code, _ in LAODONG_CATEGORIES.items():
    #     category_url = f"{laodong_base_url}{category_code}"
    #     logger.info(f"Đang crawl danh mục Lao Động: {category_code}")
    #     crawl_laodong_category(category_url, category_code, articles_collection, categories_collection)
    
    logger.info("Hoàn thành một lượt crawl")

def main():
    global articles_collection, categories_collection
    
    # Khởi tạo danh mục trước khi crawl
    initialize_categories()
    
    # Chạy crawl lần đầu
    crawl_all_sources(articles_collection, categories_collection)
    
    # Lên lịch chạy mỗi 5 phút
    schedule.every(5).minutes.do(crawl_all_sources, articles_collection, categories_collection)
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except Exception as e:
            logger.error(f"Lỗi trong vòng lặp chính: {str(e)}")
            time.sleep(60)

if __name__ == "__main__":
    main()