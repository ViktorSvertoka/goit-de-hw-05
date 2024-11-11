from colorama import Fore, Style
from confluent_kafka.admin import AdminClient, NewTopic
from configs import kafka_config

# Ініціалізація colorama
from colorama import init

init(autoreset=True)

# Створення клієнта Kafka
admin_client = AdminClient(
    {
        "bootstrap.servers": kafka_config["bootstrap_servers"],
        "security.protocol": kafka_config["security_protocol"],
        "sasl.mechanism": kafka_config["sasl_mechanism"],
        "sasl.username": kafka_config["username"],
        "sasl.password": kafka_config["password"],
    }
)

# Топіки для створення
topics = ["building_sensors", "temperature_alerts", "humidity_alerts"]

# Створення топіків
new_topics = [
    NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics
]

# Створення топіків у Kafka
try:
    admin_client.create_topics(new_topics)
    print("Topics created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Перевіряємо список існуючих топіків
existing_topics = admin_client.list_topics().topics.keys()

# Виведення створених топіків
print(Fore.GREEN + "\n==== Created Topics ====")
for topic in topics:
    print(f"{Fore.CYAN}✅ {topic}")

# Виведення всіх існуючих топіків
print(Fore.GREEN + "\n==== All Existing Topics ====")
for topic in existing_topics:
    if topic in topics:
        print(f"{Fore.YELLOW}✅ {topic} (created)")
    else:
        print(f"{Fore.RED}ℹ️  {topic}")
