import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when, split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
    print("Khởi chạy PySpark Structured Streaming đọc từ Kafka...")

    # Cấu hình các biến môi trường hoặc dùng giá trị mặc định
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weblogs")
    ES_NODES = os.getenv("ES_NODES", "elasticsearch")
    ES_PORT = os.getenv("ES_PORT", "9200")
    ES_INDEX = os.getenv("ES_INDEX", "weblogs")

    # 1. Khởi tạo SparkSession
    spark = SparkSession.builder \
        .appName("WebLogStreamingProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.elasticsearch:elasticsearch-spark-30_2.12:8.6.2") \
        .getOrCreate()
        
    # 2. Đọc luồng dữ liệu từ Kafka topic
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # 3. Parse Log thô (Raw String) bằng Regex (Kiến trúc Schema-on-Read)
    # Vì Producer bắn log thô, Kafka lưu data dạng binary ở cột 'value', ta ép kiểu sang string
    string_df = raw_df.select(col("value").cast("string").alias("value"))

    # Biểu thức chính quy cho Apache Combined Log
    LOG_PATTERN = r'^(\S+) \S+ \S+ \[([^\]]+)\] "([A-Z]+) ([^ "]+) (HTTP/[0-9.]+)" (\d{3}) (\d+|-) "([^"]*)" "([^"]*)"'

    # Sử dụng regexp_extract của Spark để bóc tách 9 cột dữ liệu
    from pyspark.sql.functions import regexp_extract
    parsed_df = string_df.select(
        regexp_extract(col("value"), LOG_PATTERN, 1).alias("ip_address"),
        regexp_extract(col("value"), LOG_PATTERN, 2).alias("timestamp"),
        regexp_extract(col("value"), LOG_PATTERN, 3).alias("http_method"),
        regexp_extract(col("value"), LOG_PATTERN, 4).alias("endpoint"),
        regexp_extract(col("value"), LOG_PATTERN, 5).alias("protocol"),
        regexp_extract(col("value"), LOG_PATTERN, 6).alias("status_code"),
        regexp_extract(col("value"), LOG_PATTERN, 7).alias("response_size_bytes"),
        regexp_extract(col("value"), LOG_PATTERN, 8).alias("referer"),
        regexp_extract(col("value"), LOG_PATTERN, 9).alias("user_agent")
    )

    # Lọc bỏ các dòng bị rác (không parse được, ip_address rỗng)
    parsed_df = parsed_df.filter(col("ip_address") != "")

    # 4. Làm sạch dữ liệu (Data Cleaning & Transformation)
    cleaned_df = parsed_df \
        .withColumn("status_code", col("status_code").cast(IntegerType())) \
        .withColumn("response_size_bytes", 
                    when(col("response_size_bytes") == "-", 0)
                    .otherwise(col("response_size_bytes")).cast(IntegerType())) \
        .withColumn("timestamp", to_timestamp(col("timestamp"), "dd/MMM/yyyy:HH:mm:ss Z")) # Parse ngày tháng Apache
        
    # Xử lý các params dư thừa trong endpoint (nếu có theo Data Contract)
    # Ví dụ: /filter/27?abc=123 -> /filter/27
    cleaned_df = cleaned_df.withColumn("endpoint", split(col("endpoint"), "\\?").getItem(0))

    # --- TÍNH NĂNG NÂNG CAO: PHÁT HIỆN TẤN CÔNG DDOS (ANOMALY DETECTION) ---
    from pyspark.sql.functions import window, lit
    
    # Gom nhóm theo cửa sổ trượt 10 giây. Đếm số request của từng IP.
    # Kỹ thuật Watermark giúp xử lý dữ liệu đến trễ (late data).
    ddos_alerts_df = cleaned_df \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            window(col("timestamp"), "10 seconds"),
            col("ip_address")
        ) \
        .count() \
        .filter(col("count") > 100) \
        .select(
            col("window.start").alias("attack_time"),
            col("ip_address"),
            col("count").alias("request_count")
        ) \
        .withColumn("alert_type", lit("DDoS_Attack"))

    # Ghi cảnh báo DDoS vào một Index riêng trên Elasticsearch
    ddos_query = ddos_alerts_df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .outputMode("append") \
        .option("es.nodes", ES_NODES) \
        .option("es.port", ES_PORT) \
        .option("es.nodes.wan.only", "true") \
        .option("es.resource", "security_alerts") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/security_alerts") \
        .start()

    # 5. Ghi dữ liệu sạch trực tiếp vào Elasticsearch Index (Luồng chính)
    main_query = cleaned_df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .outputMode("append") \
        .option("es.nodes", ES_NODES) \
        .option("es.port", ES_PORT) \
        .option("es.nodes.wan.only", "true") \
        .option("es.resource", ES_INDEX) \
        .option("checkpointLocation", "/tmp/spark-checkpoints/weblogs") \
        .start()

    # Chờ cả 2 luồng Streaming chạy song song
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
