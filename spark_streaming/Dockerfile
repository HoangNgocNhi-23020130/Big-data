# Môi trường chạy PySpark tự build (Đảm bảo 100% hoạt động, không phụ thuộc Bitnami)
FROM openjdk:11-jre-slim

# Cài đặt Python 3 và Pip
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt
COPY stream_processor.py .

# Chạy trực tiếp bằng python3 (PySpark sẽ tự gọi Java backend)
# Chú ý: Đưa cấu hình --packages trực tiếp vào file Python thay vì truyền qua CMD
CMD ["python3", "stream_processor.py"]
