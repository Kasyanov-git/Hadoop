import json
import backoff
from kafka import KafkaConsumer

# Блок 3 Задание 1----------------------------------------------------
@backoff.on_exception(backoff.expo, Exception, max_tries=10,sleep=60)
def message_handler(value):
    print(value)
# -------------------------------------------------------------
def create_consumer():
    print("Connecting to Kafka brokers")
    consumer = KafkaConsumer(
        'itmo2023',
        bootstrap_servers=['localhost:29092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        print(f"Received: {message.value}")

if __name__ == '__main__':
    create_consumer()
