import uuid
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def crawl_tienphong_category(category_url, collection, categories_collection, last_crawl_time):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        # Lấy thông tin category từ database dựa trên URL
        category_doc = categories_collection.find_one({"url": category_url})
        if not category_doc:
            logger.error(f"Không tìm thấy category trong database cho URL: {category_url}")
            return

        # Chuẩn bị thông tin category để lưu vào article
        category_info = {
            '_id': str(category_doc['_id']),
            'name': category_doc['name'],
            'source': category_doc['source'],
            'url': category_doc['url']
        }

        # Chuẩn bị keywords
        keywords = []
        check_keywords = False
        if "keyword" in category_doc and category_doc["keyword"] and len(category_doc["keyword"]) > 0:
            keywords = [kw.lower() for kw in category_doc["keyword"]]
            check_keywords = True
        else:
            logger.info(f"Không có keyword cho category {category_url}, sẽ crawl tất cả bài viết")

        response = requests.get(category_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = soup.find_all('article', class_='story')[:30]

        for article in articles:
            title_tag = article.find(['h2', 'h3', 'h5'], class_='story__heading')
            if not title_tag or not hasattr(title_tag, 'text'):
                logger.debug(f"Bỏ qua bài viết không có title_tag hợp lệ tại {category_url}")
                continue

            link_tag = title_tag.find('a', class_='cms-link')
            if not link_tag or 'href' not in link_tag.attrs:
                logger.debug(f"Bỏ qua bài viết không có link hợp lệ tại {category_url}")
                continue

            link = link_tag['href']
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

            title = title_tag.text.strip() if title_tag else ''
            description_tag = article_soup.find('div', class_='article__sapo') or article_soup.find('h2',
                                                                                                    class_='article__sapo')
            description = description_tag.text.strip() if description_tag and hasattr(description_tag, 'text') else ''

            content_div = article_soup.find('div', class_='article__body')
            if content_div:
                for unwanted in content_div.find_all(['script', 'style', 'aside', 'div', 'table']):
                    unwanted.decompose()
                content = content_div.get_text(separator='\n', strip=True)
            else:
                content = ''

            images = [img['data-src'] for img in (content_div or article_soup).find_all('img', {'data-src': True})
                      if img['data-src'].startswith('http')] or (
                         [article_soup.find('meta', property='og:image')['content']]
                         if article_soup.find('meta', property='og:image') else [])

            # Kiểm tra keyword nếu cần
            if check_keywords:
                title_lower = title.lower()
                content_lower = content.lower()
                has_keyword = any(keyword in title_lower or keyword in content_lower for keyword in keywords)
                if not has_keyword:
                    continue

            author_div = article_soup.find('div', class_='article__author')
            author = author_div.find('span', class_='name cms-author').text.strip() if author_div and author_div.find(
                'span', class_='name cms-author') else None
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


