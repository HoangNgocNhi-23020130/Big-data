import os
from elasticsearch import Elasticsearch

def main():
    print("=== BẮT ĐẦU KIỂM TRA DATA LOSS (QA Test) ===")
    
    # 1. Kết nối và đếm số lượng log đã ghi vào Elasticsearch
    ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
    ES_INDEX = os.getenv("ES_INDEX", "weblogs")
    
    try:
        es = Elasticsearch([ES_HOST])
        es_count_res = es.count(index=ES_INDEX)
        es_total = es_count_res['count']
        print(f"[Elasticsearch] Tổng số documents đã ghi nhận vào index '{ES_INDEX}': {es_total}")
    except Exception as e:
        print(f"Lỗi kết nối Elasticsearch: {e}")
        es_total = 0
        return

    # 2. Đối chiếu với tổng số dòng trong file dataset gốc
    DATASET_PATH = os.getenv("DATASET_PATH", "../log_producer/data/weblog.csv")
    if os.path.exists(DATASET_PATH):
        import pandas as pd
        print("Đang đếm số dòng trong file CSV gốc...")
        df = pd.read_csv(DATASET_PATH, usecols=[0]) # Chỉ đọc 1 cột cho nhanh
        csv_total = len(df)
        print(f"[Dataset CSV] Tổng số dòng log gốc: {csv_total}")
        
        loss = csv_total - es_total
        loss_percent = (loss / csv_total) * 100 if csv_total > 0 else 0
        
        print("\n--- KẾT QUẢ KỂM THỬ ---")
        if loss == 0:
            print("✅ ZERO DATA LOSS: 100% dữ liệu đã chạy qua Kafka -> Spark -> Elasticsearch an toàn!")
        elif loss > 0:
            print(f"⚠️ CẢNH BÁO MẤT DỮ LIỆU: Hao hụt {loss} dòng ({loss_percent:.4f}%).")
            print("-> Hãy kiểm tra Log của Spark xem có record nào bị Parse Error/Drop không.")
        else:
            print(f"❓ DỮ LIỆU BỊ DƯ ({abs(loss)} dòng).")
            print("-> Bạn có thể đã chạy Producer nhiều lần trùng lặp mà chưa clear index Elasticsearch.")
    else:
        print(f"Không tìm thấy file dataset gốc tại {DATASET_PATH} để đối chiếu.")

if __name__ == "__main__":
    main()
