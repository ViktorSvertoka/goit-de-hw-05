from confluent_kafka import Consumer, Producer
from configs import kafka_config
import json
import time
from colorama import Fore, Style, init

# Ініціалізація colorama для роботи з кольорами
init(autoreset=True)

# Створення Kafka Consumer
consumer = Consumer(
    {
        "bootstrap.servers": kafka_config["bootstrap_servers"],
        "security.protocol": kafka_config["security_protocol"],
        "sasl.mechanism": kafka_config["sasl_mechanism"],
        "sasl.username": kafka_config["username"],
        "sasl.password": kafka_config["password"],
        "group.id": "sensor_group",
        "auto.offset.reset": "earliest",
    }
)

# Створення Kafka Producer
producer = Producer(
    {
        "bootstrap.servers": kafka_config["bootstrap_servers"],
        "security.protocol": kafka_config["security_protocol"],
        "sasl.mechanism": kafka_config["sasl_mechanism"],
        "sasl.username": kafka_config["username"],
        "sasl.password": kafka_config["password"],
    }
)

# Підписка на топік building_sensors
consumer.subscribe(["building_sensors"])


# Обробка отриманих повідомлень
def process_message(message):
    # Перетворення повідомлення в Python об'єкт
    data = json.loads(message.value().decode("utf-8"))
    sensor_id = data["sensor_id"]
    temperature = data["temperature"]
    humidity = data["humidity"]

    # Виведення отриманих даних з кольоровим виводом
    print(Fore.GREEN + f"Received data from sensor {sensor_id}:")
    print(Fore.YELLOW + f"Temperature: {temperature}°C, Humidity: {humidity}%")
    print("-" * 50)

    # Генерація сповіщень для перевищення температури
    if temperature > 40:
        alert_message = {
            "sensor_id": sensor_id,
            "temperature": temperature,
            "timestamp": data["timestamp"],
            "message": "Temperature exceeds threshold!",
        }
        print(Fore.RED + f"ALERT: Temperature exceeds threshold! Sending alert...")
        # Відправка сповіщення до топіку temperature_alerts
        producer.produce(
            "temperature_alerts", key=sensor_id, value=json.dumps(alert_message)
        )
        print(Fore.MAGENTA + f"Data sent to topic 'temperature_alerts'")

    # Генерація сповіщень для вологості
    if humidity > 80 or humidity < 20:
        alert_message = {
            "sensor_id": sensor_id,
            "humidity": humidity,
            "timestamp": data["timestamp"],
            "message": "Humidity exceeds threshold!",
        }
        print(Fore.CYAN + f"ALERT: Humidity exceeds threshold! Sending alert...")
        # Відправка сповіщення до топіку humidity_alerts
        producer.produce(
            "humidity_alerts", key=sensor_id, value=json.dumps(alert_message)
        )
        print(Fore.MAGENTA + f"Data sent to topic 'humidity_alerts'")

    # Важливо! Для гарантії відправки повідомлень у Kafka
    producer.flush()

    # Додатково для результату фільтрації і запису в топіки:
    print(
        Fore.WHITE
        + "Filtered data has been successfully sent to the appropriate topics."
    )
    print("-" * 50)


# Обробка повідомлень з топіку
try:
    timeout = 10  # Таймаут в секундах
    start_time = time.time()  # Час початку виконання

    while True:
        # Читання повідомлення з топіку з таймаутом
        msg = consumer.poll(timeout=1.0)  # Читання з топіку
        if msg is None:
            continue
        elif msg.error():
            print(Fore.RED + f"Error: {msg.error()}")
        else:
            process_message(msg)

        # Перевірка часу роботи скрипту
        if time.time() - start_time > timeout:
            print(Fore.MAGENTA + "Timeout reached, stopping consumer.")
            break

except KeyboardInterrupt:
    print(Fore.MAGENTA + " Consumer interrupted.")
finally:
    consumer.close()
    print(Fore.GREEN + "Consumer closed.")
