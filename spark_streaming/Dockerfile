# Chốt phương án TỐI ƯU NHẤT: Dùng Python 3.9 trên nền Debian 11 (Bullseye)
# - python:3.9: Tránh lỗi "typing.io" của Python 3.12+ (do PySpark 3.3 không hỗ trợ Python đời quá mới)
# - bullseye: Debian 11 mặc định dùng Java 11 (Tránh lỗi ByteBuffer của Java 17)
FROM python:3.9-slim-bullseye

# Cài đặt Java 11 (default-jre trên Bullseye chính là OpenJDK 11)
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y default-jre && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY stream_processor.py .

CMD ["python", "stream_processor.py"]
