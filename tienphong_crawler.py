import uuid
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def crawl_tienphong_category(category_url, collection, last_crawl_time, get_category_info):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        response = requests.get(category_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = soup.find_all('article', class_='story')[:30]
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
            logger.info(f"Đã lưu (Tiền Phong): {title}")

    except Exception as e:
        logger.error(f"Lỗi khi crawl danh mục Tiền Phong {category_url}: {str(e)}")

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
        return {'_id': 'some_id', 'name': 'Địa ốc', 'source': {'name': 'TIEN PHONG'}, 'url': url}

    last_crawl = datetime.now() - timedelta(days=1)
    # Example usage for testing with a specific category URL
    test_category_url = 'https://tienphong.vn/dia-oc/'
    crawl_tienphong_category(test_category_url, articles_collection, last_crawl, dummy_get_category_info)