"""
Trend Analyzer Service

- Consumes: ENERGY_TOPIC (Via Schema Registry - Binary/JSON)
- Produces: TREND_TOPIC (Plain JSON String) -> FIX FOR DASHBOARD ERROR

Logic:
- Rolling moving average
- UP / DOWN trend
"""

import json
from collections import deque
from statistics import mean

from confluent_kafka import (
    DeserializingConsumer,
    SerializingProducer,
    KafkaError,
)
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

import config


# -------------------------
# Topics
# -------------------------
ENERGY_TOPIC = getattr(config, "ENERGY_TOPIC", "energy_stream")
TREND_TOPIC = getattr(config, "TREND_TOPIC", "trend_stream")


# -------------------------
# Load Input Schema (Only for consuming Energy)
# -------------------------
with open("schemas/energy_schema.json", "r", encoding="utf-8") as f:
    ENERGY_SCHEMA = f.read()


def dict_from_json(obj, ctx):
    return obj


# -------------------------
# Consumer (ENERGY - Schema Registry)
# -------------------------
def create_energy_consumer():
    sr = SchemaRegistryClient(config.sr_conf)

    json_deserializer = JSONDeserializer(
        schema_str=ENERGY_SCHEMA,
        schema_registry_client=sr,
        from_dict=dict_from_json,
    )

    return DeserializingConsumer({
        "bootstrap.servers": config.kafka_conf["bootstrap.servers"],
        "security.protocol": config.kafka_conf["security.protocol"],
        "sasl.mechanism": config.kafka_conf["sasl.mechanism"],
        "sasl.username": config.kafka_conf["sasl.username"],
        "sasl.password": config.kafka_conf["sasl.password"],

        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": json_deserializer,

        "group.id": "trend-analyzer-service",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })


# -------------------------
# Producer (TREND - Plain JSON)
# -------------------------
def create_plain_json_producer():
    """
    Produces plain JSON strings to avoid Schema Registry decoding errors in Dashboard.
    """
    prod_conf = config.kafka_conf.copy()
    prod_conf["key.serializer"] = StringSerializer("utf_8")
    prod_conf["value.serializer"] = StringSerializer("utf_8")

    return SerializingProducer(prod_conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"[TREND][ERROR] Delivery failed: {err}")


# -------------------------
# MAIN
# -------------------------
def main():
    consumer = create_energy_consumer()
    producer = create_plain_json_producer()

    consumer.subscribe([ENERGY_TOPIC])
    print(
        f"[TREND] Service started | "
        f"Consuming: {ENERGY_TOPIC} (SR) -> Producing: {TREND_TOPIC} (Plain JSON)"
    )

    # Rolling window for moving average
    window = deque(maxlen=10)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print("[TREND][ERROR]", msg.error())
                continue

            data = msg.value()
            if data is None:
                continue

            # Extract data
            try:
                consumption = float(data["energy_consumption"])
                timestamp = data["timestamp"]
                device_id = data.get("device_id", "unknown")
            except (KeyError, ValueError, TypeError):
                continue

            window.append(consumption)

            if len(window) < 5:
                continue

            moving_avg = mean(window)
            trend = "UP" if consumption > moving_avg else "DOWN"

            # ---- Console log ----
            print(
                f"[TREND] ts={timestamp} | "
                f"current={consumption:.4f} | "
                f"avg={moving_avg:.4f} | "
                f"trend={trend}"
            )

            # ---- Trend Event (Plain Dict) ----
            trend_event = {
                "timestamp": timestamp,
                "device_id": device_id,
                "energy_consumption": consumption,
                "moving_average": float(moving_avg),
                "trend": trend,
                "window_size": len(window),
            }

            # Serialize to JSON String before sending
            producer.produce(
                topic=TREND_TOPIC,
                key=str(timestamp),
                value=json.dumps(trend_event),  # Plain JSON serialization
                on_delivery=delivery_report,
            )
            producer.poll(0)

    except KeyboardInterrupt:
        print("\n[TREND] Terminated by user.")
    finally:
        print("[TREND] Flushing producer...")
        producer.flush(10)
        consumer.close()


if __name__ == "__main__":
    main()