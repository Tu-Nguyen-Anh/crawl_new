import pymongo
import telebot
from datetime import datetime, timedelta
import time

# MongoDB Connection
MONGO_URI = 'mongodb://mongodb:27017/'
DATABASE_NAME = 'olh_news'
COLLECTION_NAME = 'articles'

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN = '7708220149:AAHzqMdir3HhYEyCyJKkyGFUjNIQuR1QFjA'
CHAT_ID = '5882369573'

# Initialize Telegram Bot
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

# Biến theo dõi thời gian gửi cuối cùng và số lượng bài viết đã gửi
last_sent_time = None
sent_articles_count = 0  # Biến đếm số bài viết đã gửi

def get_recent_articles():
    """
    Fetch up to 50 most recent articles published after the last sent time
    """
    global last_sent_time
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    articles_collection = db[COLLECTION_NAME]
    
    if last_sent_time is None:
        last_sent_time = datetime.utcnow() - timedelta(minutes=5)
    
    query = {'publish_date': {'$gt': last_sent_time}}
    recent_articles = list(articles_collection.find(query).sort('publish_date', -1).limit(50))
    
    client.close()
    return recent_articles

def format_article_message(article):
    """
    Format article details for Telegram message without icons
    """
    title = article.get('title', 'No Title')
    link = article.get('link', 'No Link')
    description = article.get('description', 'No Description')
    author = article.get('author', 'Unknown Author')
    publish_date = article.get('publish_date', 'Unknown Date')
    
    category_name = article.get('category_id', {}).get('name', 'No Category')
    source_name = article.get('category_id', {}).get('source', {}).get('name', 'No Source')
    
    message = f"*{title}*\n\n" \
              f"*Tóm tắt:* {description}\n\n" \
              f"*Nguồn báo:* {source_name}\n\n" \
              f"*Tác giả:* {author}\n" \
              f"*Ngày đăng:* {publish_date}\n" \
              f"*Danh mục:* {category_name}\n" \
              f"[Đọc thêm]({link})"
    
    keyboard = telebot.types.InlineKeyboardMarkup()
    return message, keyboard

def send_articles_to_telegram():
    """
    Main function to fetch and send recent articles
    """
    global last_sent_time, sent_articles_count
    try:
        recent_articles = get_recent_articles()
        
        if not recent_articles:
            print(f"[{datetime.now()}] No new articles found. Total sent so far: {sent_articles_count}")
            return
        
        for article in recent_articles:
            message, keyboard = format_article_message(article)
            try:
                bot.send_message(
                    CHAT_ID, 
                    message, 
                    parse_mode='Markdown', 
                    reply_markup=keyboard
                )
                sent_articles_count += 1  # Tăng biến đếm khi gửi thành công
                print(f"[{datetime.now()}] Sent article: {article.get('title', 'Untitled')}. Total sent: {sent_articles_count}")
            except Exception as send_error:
                print(f"[{datetime.now()}] Error sending article: {send_error}")
        
        if recent_articles:
            last_sent_time = max(article['publish_date'] for article in recent_articles)
    
    except Exception as e:
        print(f"[{datetime.now()}] Error in send_articles_to_telegram: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('article_details_'))
def article_details_callback(call):
    """
    Handle callback for article details
    """
    try:
        article_id = call.data.split('_')[-1]
        
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        articles_collection = db[COLLECTION_NAME]
        
        article = articles_collection.find_one({'_id': article_id})
        
        if article:
            details_message = f"*{article.get('title', 'No Title')}*\n\n" \
                              f"*Author:* {article.get('author', 'Unknown')}\n" \
                              f"*Published:* {article.get('publish_date', 'Unknown')}\n" \
                              f"*Category:* {article.get('category_id', {}).get('name', 'No Category')}\n" \
                              f"*Source:* {article.get('category_id', {}).get('source', {}).get('name', 'No Source')}\n\n" \
                              f"*Description:* {article.get('description', 'No Description')}\n\n" \
                              f"*Content:* {article.get('content', 'No Content')}\n\n" \
                              f"[Read More]({article.get('link', '#')})"
            
            bot.answer_callback_query(call.id, "Fetched Article Details")
            bot.send_message(call.message.chat.id, details_message, parse_mode='Markdown')
        
        client.close()
    
    except Exception as e:
        print(f"[{datetime.now()}] Error in article details callback: {e}")
        bot.answer_callback_query(call.id, "Error fetching details")

def main():
    """
    Main execution loop
    """
    print(f"[{datetime.now()}] News Scraper and Telegram Bot Started...")
    while True:
        send_articles_to_telegram()
        time.sleep(300)  # Ngủ 5 phút

if __name__ == '__main__':
    main()