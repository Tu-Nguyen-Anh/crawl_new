import uuid
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def crawl_vnexpress_category(category_url, collection, categories_collection, last_crawl_time):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5'
    }
    try:
        logger.info(f"Bắt đầu crawl VnExpress: {category_url}")

        # Lấy thông tin category từ database dựa trên URL
        category_doc = categories_collection.find_one({"url": category_url})
        if not category_doc:
            logger.error(f"Không tìm thấy category trong database cho URL: {category_url}")
            return

        category_info = {
            '_id': str(category_doc['_id']),
            'name': category_doc['name'],
            'source': category_doc['source'],
            'url': category_doc['url']
        }

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
        articles = soup.find_all('article', class_='item-news')

        for article in articles[:30]:
            title_tag = article.find('h3', class_='title-news')
            if not title_tag or not hasattr(title_tag, 'text'):
                logger.debug(f"Bỏ qua bài viết không có title_tag hợp lệ tại {category_url}")
                continue

            link_tag = title_tag.find('a')
            if not link_tag or 'href' not in link_tag.attrs:
                logger.debug(f"Bỏ qua bài viết không có link hợp lệ tại {category_url}")
                continue

            link = link_tag['href']
            if collection.find_one({'link': link}):
                continue

            try:
                article_response = requests.get(link, headers=headers)
                article_response.raise_for_status()
                article_soup = BeautifulSoup(article_response.content, 'html.parser')
                publish_date = None
                date_tag = article_soup.find('span', class_='date')
                if date_tag and hasattr(date_tag, 'text'):
                    try:
                        date_clean = date_tag.text.strip().split(' (GMT')[0].split(', ', 1)[1]
                        publish_date = datetime.strptime(date_clean, '%d/%m/%Y, %H:%M')
                    except (ValueError, IndexError):
                        continue
                if publish_date and publish_date <= last_crawl_time:
                    continue

                title = title_tag.text.strip() if title_tag else ''
                description_tag = article_soup.find('p', class_='description')
                description = description_tag.text.strip() if description_tag and hasattr(description_tag,
                                                                                          'text') else ''

                content_tag = article_soup.find('article', class_='fck_detail')
                content = content_tag.get_text(separator='\n', strip=True) if content_tag and hasattr(content_tag,
                                                                                                      'text') else ''

                images = [img.get('data-src') or img.get('src') for img in article_soup.find_all('img', class_='lazy')
                          if (img.get('data-src') or img.get('src')) and (
                                      img.get('data-src') or img.get('src')).startswith('http')]

                if check_keywords:
                    title_lower = title.lower()
                    content_lower = content.lower()
                    has_keyword = any(keyword in title_lower or keyword in content_lower for keyword in keywords)
                    if not has_keyword:
                        logger.debug(f"Bỏ qua bài viết không chứa keyword: {title}")
                        continue

                author = None
                for tag in [article_soup.find('p', class_='author'), article_soup.find('strong', class_='author')]:
                    if tag and hasattr(tag, 'text'):
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
                logger.error(f"Lỗi request bài viết {link}: {str(e)}")
            except Exception as e:
                logger.error(f"Lỗi xử lý bài viết {link}: {str(e)}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Lỗi request danh mục {category_url}: {str(e)}")
    except Exception as e:
        logger.error(f"Lỗi tổng quát khi crawl VnExpress {category_url}: {str(e)}")