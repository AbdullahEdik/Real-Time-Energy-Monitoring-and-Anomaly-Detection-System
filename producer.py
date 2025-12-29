import json
import time
import pandas as pd

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

import config


def load_json_schema(path: str) -> str:
    """ Load and return the JSON schema file as a string. """
    with open(path, "r") as f:
        return f.read()


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
    # Delivery success is intentionally silent for speed.


def main():

    print("[INFO] Loading CSV dataset...")
    try:
        df = pd.read_csv("smart_meter_data.csv")
    except FileNotFoundError:
        print("[ERROR] 'smart_meter_data.csv' not found.")
        return

    # ---- Data Preprocessing ----
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df = df.sort_values("Timestamp")

    anomaly_map = {"Normal": 0, "Abnormal": 1}
    df["Anomaly_Label_Int"] = df["Anomaly_Label"].map(anomaly_map)

    df["device_id"] = "meter_001"

    # ---- Load JSON Schemas ----
    energy_schema_str = load_json_schema("schemas/energy_schema.json")
    weather_schema_str = load_json_schema("schemas/weather_schema.json")

    # ---- Schema Registry ----
    schema_registry_client = SchemaRegistryClient(config.sr_conf)

    energy_serializer = JSONSerializer(
        schema_str=energy_schema_str,
        schema_registry_client=schema_registry_client,
        to_dict=lambda obj, ctx: obj
    )

    weather_serializer = JSONSerializer(
        schema_str=weather_schema_str,
        schema_registry_client=schema_registry_client,
        to_dict=lambda obj, ctx: obj
    )

    # ---- Kafka Producers ----
    energy_conf = config.kafka_conf.copy()
    energy_conf["key.serializer"] = StringSerializer("utf_8")
    energy_conf["value.serializer"] = energy_serializer

    weather_conf = config.kafka_conf.copy()
    weather_conf["key.serializer"] = StringSerializer("utf_8")
    weather_conf["value.serializer"] = weather_serializer

    producer_energy = SerializingProducer(energy_conf)
    producer_weather = SerializingProducer(weather_conf)

    print("[INFO] JSON data streaming started (Ctrl + C to stop)...")

    try:
        for idx, row in df.iterrows():

            ts_str = row["Timestamp"].strftime("%Y-%m-%d %H:%M:%S")

            # Build JSON payloads
            energy_obj = {
                "timestamp": ts_str,
                "device_id": row["device_id"],
                "energy_consumption": float(row["Electricity_Consumed"]),
                "anomaly_label": int(row["Anomaly_Label_Int"]),
                "original_label": row["Anomaly_Label"]
            }

            weather_obj = {
                "timestamp": ts_str,
                "temperature": float(row["Temperature"]),
                "humidity": float(row["Humidity"]),
                "wind_speed": float(row["Wind_Speed"])
            }

            # Send to Kafka
            producer_energy.produce(
                topic=config.ENERGY_TOPIC,
                key=ts_str,
                value=energy_obj,
                on_delivery=delivery_report
            )

            producer_weather.produce(
                topic=config.WEATHER_TOPIC,
                key=ts_str,
                value=weather_obj,
                on_delivery=delivery_report
            )

            producer_energy.poll(0)
            producer_weather.poll(0)

            if idx % 10 == 0:
                print(f"[{ts_str}] Sent -> {row['Electricity_Consumed']:.4f} kWh")

            time.sleep(0.2)

    except KeyboardInterrupt:
        print("\n[INFO] Streaming manually interrupted.")

    finally:
        print("[INFO] Flushing remaining messages...")
        producer_energy.flush()
        producer_weather.flush()
        print("[INFO] Producer finished cleanly.")


if __name__ == "__main__":
    main()
