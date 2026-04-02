from kafka import KafkaProducer
import json
import pandas as pd

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

df = pd.read_csv("data/students.csv")

for _, row in df.iterrows():
    producer.send("student_topic", row.to_dict())

print("Data sent to Kafka")