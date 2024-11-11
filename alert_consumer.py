from confluent_kafka import Consumer
from configs import kafka_config
import json

# Створення Kafka Consumer
consumer = Consumer(
    {
        "bootstrap.servers": kafka_config["bootstrap_servers"],
        "security.protocol": kafka_config["security_protocol"],
        "sasl.mechanism": kafka_config["sasl_mechanism"],
        "sasl.username": kafka_config["username"],
        "sasl.password": kafka_config["password"],
        "group.id": "alert_group",
        "auto.offset.reset": "earliest",
    }
)

# Підписка на топіки temperature_alerts та humidity_alerts
consumer.subscribe(["temperature_alerts", "humidity_alerts"])

print("Alert consumer started...")

# Обробка сповіщень
try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Читання з топіку
        if msg is None:
            continue
        elif msg.error():
            print(f"Error: {msg.error()}")
        else:
            alert = json.loads(msg.value().decode("utf-8"))
            print(f"Received alert: {alert['message']} for sensor {alert['sensor_id']}")
except KeyboardInterrupt:
    print("Consumer interrupted.")
finally:
    print("Closing consumer...")
    consumer.close()
