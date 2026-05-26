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
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Định nghĩa Schema cho JSON message nhận từ Kafka
    # Ban đầu đọc toàn bộ dưới dạng String để dễ dàng xử lý ngoại lệ ('-')
    schema = StructType([
        StructField("ip_address", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("http_method", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("protocol", StringType(), True),
        StructField("status_code", StringType(), True),
        StructField("response_size_bytes", StringType(), True),
        StructField("referer", StringType(), True),
        StructField("user_agent", StringType(), True)
    ])

    # 2. Đọc luồng dữ liệu từ Kafka topic
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # 3. Parse JSON từ message Kafka
    # Kafka lưu data dạng binary ở cột 'value', cần ép kiểu sang string và parse JSON
    json_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # 4. Làm sạch dữ liệu (Data Cleaning & Transformation)
    cleaned_df = json_df \
        .withColumn("status_code", col("status_code").cast(IntegerType())) \
        .withColumn("response_size_bytes", 
                    when(col("response_size_bytes") == "-", 0)
                    .otherwise(col("response_size_bytes")).cast(IntegerType())) \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) # Convert ISO8601 string thành TimestampType
        
    # Xử lý các params dư thừa trong endpoint (nếu có theo Data Contract)
    # Ví dụ: /filter/27?abc=123 -> /filter/27
    cleaned_df = cleaned_df.withColumn("endpoint", split(col("endpoint"), "\\?").getItem(0))

    # 5. Ghi dữ liệu trực tiếp vào Elasticsearch Index
    # Cần cấu hình checkpoint để đảm bảo fault-tolerance (chống mất dữ liệu khi sập)
    query = cleaned_df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .outputMode("append") \
        .option("es.nodes", ES_NODES) \
        .option("es.port", ES_PORT) \
        .option("es.resource", f"{ES_INDEX}/_doc") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/weblogs") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
