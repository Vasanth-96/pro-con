from confluent_kafka import Producer
import json
import time
import random
from typing import Any

p = Producer({"bootstrap.servers": "localhost:9092"})


def delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


click_events = ["page_view", "button_click", "ad_click"]

while True:
    event = {
        "event_type": random.choice(click_events),
        "user_id": random.randint(1000, 9999),
        "timestamp": time.time(),
    }
    p.produce(
        "click-events", value=json.dumps(event), callback=delivery_report
    )
    p.flush()
    time.sleep(1)  # Send an event every second
