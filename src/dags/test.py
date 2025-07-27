import logging
from fraud_detection_training import FraudDetectionTraining

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Đường dẫn đến file config.yaml
    config_path = '../config.yaml'  # hoặc đường dẫn đúng nếu khác

    # Khởi tạo class training
    trainer = FraudDetectionTraining(config_path=config_path)

    # Gọi method đọc từ Kafka
    df = trainer.read_from_kafka()

    # Hiển thị kết quả
    print("✅ Kafka read successful. Sample data:")
    print(df.head())