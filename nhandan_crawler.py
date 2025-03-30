import uuid
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def crawl_nhandan_category(category_url, collection, last_crawl_time, get_category_info):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        response = requests.get(category_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = soup.find_all('article', class_='story')[:30]
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
            logger.info(f"Đã lưu (Nhân Dân): {title}")

    except Exception as e:
        logger.error(f"Lỗi khi crawl danh mục Nhân Dân {category_url}: {str(e)}")

if __name__ == '__main__':
    # This block is for testing purposes only
    logging.basicConfig(level=logging.INFO)
    from pymongo import MongoClient
    from datetime import datetime, timedelta
    client = MongoClient('mongodb://localhost:27017/')
    db = client['olh_news']
    articles_collection = db['articles']

    # Dummy function for testing
    def dummy_get_category_info(url):
        return {'_id': 'some_id', 'name': 'Chính trị', 'source': {'name': 'NHAN DAN'}, 'url': url}

    last_crawl = datetime.now() - timedelta(days=1)
    # Example usage for testing with a specific category URL
    test_category_url = 'https://nhandan.vn/chinhtri/'
    crawl_nhandan_category(test_category_url, articles_collection, last_crawl, dummy_get_category_info)