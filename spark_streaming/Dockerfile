# Chuyển sang dùng Java 11 chính chủ (eclipse-temurin) 
# Mục đích: Fix lỗi "java.nio.DirectByteBuffer" do PySpark 3.3.x không tương thích tốt với Java 17 (bản mặc định của Python slim)
FROM eclipse-temurin:11-jre

# Cài đặt Python 3 và Pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt
COPY stream_processor.py .

CMD ["python3", "stream_processor.py"]
