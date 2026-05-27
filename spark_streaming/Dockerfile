# Môi trường chạy PySpark
FROM bitnami/spark:3.3
WORKDIR /app
COPY requirements.txt .
USER root
RUN pip install --no-cache-dir -r requirements.txt
USER 1001
COPY stream_processor.py .
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.elasticsearch:elasticsearch-spark-30_2.12:8.6.2", "stream_processor.py"]
