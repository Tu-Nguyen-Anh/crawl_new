# Sử dụng image chính thức của Python
FROM python:3.9-slim

# Đặt thư mục làm việc trong container
WORKDIR /app

# Copy file requirements.txt vào container
COPY requirements.txt /app/

# Cài đặt các thư viện từ requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ mã nguồn vào trong container
COPY . /app/

# Chạy script crawl.py
CMD ["python", "crawl.py"]
