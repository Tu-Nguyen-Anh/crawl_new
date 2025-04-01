import uuid
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging
import re
from pymongo import MongoClient

logger = logging.getLogger(__name__)

# Kết nối MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['olh_news']
categories_collection = db['categories']


def check_keywords_match(title, content, keywords):
    """Kiểm tra xem có từ khóa nào trong keywords xuất hiện chính xác trong title hoặc content"""
    logger.info(f"Danh sách từ khóa nhận được: {keywords}")

    if not keywords:  # Nếu không có từ khóa, bỏ qua bài viết
        logger.info("Danh sách từ khóa rỗng, bỏ qua bài viết")
        return False

    full_text = f"{title}\n{content}"
    lines = full_text.split('\n')
    for i, line in enumerate(lines, 1):
        for keyword in keywords:
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, line):
                location = "title" if i == 1 else "content"
                logger.info(f"Khớp keyword '{keyword}' tại {location}, dòng {i}: {line.strip()}")
                return True
    logger.info(f"Không tìm thấy từ khóa nào trong '{title}' hoặc nội dung")
    return False


def crawl_vnexpress_category(category_url, collection, last_crawl_time, get_category_info):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5'
    }
    try:
        response = requests.get(category_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = soup.find_all('article', class_='item-news')
        category_info = get_category_info(category_url)  # Sử dụng get_category_info từ main
        if not category_info:
            logger.error(f"Không lấy được thông tin category cho {category_url}")
            return

        keywords = category_info.get('keyword', [])  # Lấy danh sách từ khóa từ category_info

        for article in articles[:30]:
            title_tag = article.find('h3', class_='title-news')
            if not title_tag:
                continue
            link = title_tag.find('a')['href']
            if collection.find_one({'link': link}):
                continue

            try:
                article_response = requests.get(link, headers=headers)
                article_response.raise_for_status()
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

                # Kiểm tra từ khóa trước khi lưu
                if not check_keywords_match(title, content, keywords):
                    logger.info(f"Bài viết {link} không chứa keyword nào trong {keywords}")
                    continue

                images = [img.get('data-src') or img.get('src') for img in article_soup.find_all('img', class_='lazy')
                          if (img.get('data-src') or img.get('src')) and (
                                      img.get('data-src') or img.get('src')).startswith('http')]

                author = None
                for tag in [article_soup.find('p', class_='author'), article_soup.find('strong', class_='author')]:
                    if tag:
                        author = tag.text.strip()
                        break
                if not author and content:
                    last_p = article_soup.find('p', class_='Normal', attrs={'style': 'text-align:right;'})
                    author = last_p.find('strong').text.strip() if last_p and last_p.find('strong') else ' '.join(
                        content.split()[-2:])

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
                logger.info(f"Đã lưu (VnExpress): {title} - {link}")

            except requests.exceptions.RequestException as e:
                pass  # Loại bỏ log lỗi request bài viết
            except Exception as e:
                pass  # Loại bỏ log lỗi khác khi crawl bài viết

    except requests.exceptions.RequestException as e:
        pass  # Loại bỏ log lỗi request danh mục
    except Exception as e:
        pass  # Loại bỏ log lỗi tổng quát danh mục


if __name__ == '__main__':
    # Phần test độc lập
    logging.basicConfig(level=logging.INFO)
    from pymongo import MongoClient
    from datetime import datetime, timedelta

    client = MongoClient('mongodb://localhost:27017/')
    db = client['olh_news']
    articles_collection = db['articles']


    # Hàm lấy category từ database cho test độc lập
    def dummy_get_category_info(url):
        category = categories_collection.find_one({'url': url})
        if category:
            return {
                '_id': category['_id'],
                'name': category['name'],
                'source': category['source'],
                'url': category['url'],
                'keyword': category.get('keyword', [])  # Lấy từ khóa từ database
            }
        # Fallback nếu không tìm thấy trong database
        logger.warning(f"Không tìm thấy category cho {url} trong database, sử dụng giá trị mặc định")
        return {
            '_id': str(uuid.uuid4()),
            'name': 'Thời sự',
            'source': {'name': 'VN EXPRESS'},
            'url': url,
            'keyword': []
        }


    last_crawl = datetime.now() - timedelta(days=1)
    test_category_url = 'https://vnexpress.net/thoi-su'
    crawl_vnexpress_category(test_category_url, articles_collection, last_crawl, dummy_get_category_info)