import asyncio
import json
import os
import random
from datetime import datetime

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

BOOTSTRAP_SERVERS = (
    "localhost:19092"
    if os.getenv("RUNTIME_ENVIRONMENT") == "DOCKER"
    else "localhost:19092"
)

PIZZASTREAM_TOPIC = "pizzastream"
PIZZASTREAM_TYPES = [
    "Margherita",
    "Hawaiian",
    "Veggie",
    "Meat",
    "Pepperoni", 
    "Buffalo",
    "Supreme",
    "Chicken",
]


async def generate_pizza(user_id):
    
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    while True:
        data = {
            "timestamp_": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "pizza": random.choice(PIZZASTREAM_TYPES),
            "user_id": user_id,
            "quantity": random.randint(1, 10),
        }
        producer.send(
            PIZZASTREAM_TOPIC,
            key=user_id.encode("utf-8"),
            value=json.dumps(data).encode("utf-8"),
        )
        print(
            f"Sent a pizza stream event data to Redpanda: {data}"
        )
        await asyncio.sleep(random.randint(1, 5))


async def main():
    tasks = [
        generate_pizza(user_id)
        for user_id in [f"user_{i}" for i in range(10)]
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    # Create kafka topics if running in Docker.
    if os.getenv("RUNTIME_ENVIRONMENT") == "DOCKER":
        admin_client = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS, client_id="pizzastream-producer"
        )
        # Check if topics already exist first
        existing_topics = admin_client.list_topics()
        for topic in [PIZZASTREAM_TOPIC]:
            if topic not in existing_topics:
                admin_client.create_topics(
                    [NewTopic(topic, num_partitions=1, replication_factor=1)]
                )
    asyncio.run(main())
