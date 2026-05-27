# Sử dụng base image Python chính chủ (Đảm bảo 100% tồn tại vĩnh viễn)
FROM python:3.9-slim

# Cài đặt Java JRE (Bắt buộc để chạy PySpark backend)
# Lệnh mkdir để fix lỗi thiếu thư mục manpage kinh điển của Debian slim
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y default-jre && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY stream_processor.py .

CMD ["python", "stream_processor.py"]
