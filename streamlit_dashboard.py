import streamlit as st
import pandas as pd
import time
import json
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from collections import deque

from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer, SerializationContext
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

import config


# =====================================================
# Load Schemas (ONLY FOR INPUT STREAMS)
# =====================================================
def load_schema(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


ENERGY_SCHEMA = load_schema("schemas/energy_schema.json")
WEATHER_SCHEMA = load_schema("schemas/weather_schema.json")


# =====================================================
# Kafka Consumer Factories
# =====================================================

def dict_from_json(obj: dict, ctx: SerializationContext):
    return obj


def create_schema_consumer(topic, schema):
    # Consumer for SR-encoded topics (Energy, Weather)
    sr = SchemaRegistryClient(config.sr_conf)

    deserializer = JSONDeserializer(
        schema_str=schema,
        schema_registry_client=sr,
        from_dict=dict_from_json
    )

    conf = {
        **config.kafka_conf,
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": deserializer,
        "group.id": f"streamlit-sr-{topic}",
        "auto.offset.reset": "latest",
    }

    consumer = DeserializingConsumer(conf)
    consumer.subscribe([topic])
    return consumer


def create_plain_json_consumer(topic):
    # Consumer for Plain JSON topics (Trend, Anomaly, Eval)
    conf = {
        **config.kafka_conf,
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": StringDeserializer("utf_8"),
        "group.id": f"streamlit-plain-{topic}",
        "auto.offset.reset": "latest",
    }

    c = DeserializingConsumer(conf)
    c.subscribe([topic])
    return c


# =====================================================
# Streamlit Setup
# =====================================================
st.set_page_config(
    page_title="Smart Meter – Advanced Analytics",
    layout="wide"
)

st.title("Smart Meter Monitoring – Advanced Analytics Dashboard")

# =====================================================
# Session Init (with DEQUE for memory safety)
# =====================================================
MAX_LEN = 1000

if "init" not in st.session_state:
    # Input streams (Schema Registry)
    st.session_state.energy_c = create_schema_consumer(
        config.ENERGY_TOPIC, ENERGY_SCHEMA
    )
    st.session_state.weather_c = create_schema_consumer(
        config.WEATHER_TOPIC, WEATHER_SCHEMA
    )

    # Downstream analytics streams (Plain JSON)
    st.session_state.trend_c = create_plain_json_consumer("trend_stream")
    st.session_state.anomaly_c = create_plain_json_consumer("anomaly_stream")
    st.session_state.eval_c = create_plain_json_consumer("evaluation_stream")

    # Buffers (Using Deque to prevent memory leak)
    st.session_state.energy = deque(maxlen=MAX_LEN)
    st.session_state.weather = deque(maxlen=MAX_LEN)
    st.session_state.trends = deque(maxlen=MAX_LEN)
    st.session_state.anomalies = deque(maxlen=MAX_LEN)
    st.session_state.evals = deque(maxlen=MAX_LEN)

    st.session_state.init = True


# =====================================================
# Poll helper
# =====================================================
def poll(consumer, buffer, parse_json=False):
    # Poll multiple messages to feel more real-time
    for _ in range(5):
        msg = consumer.poll(0.05)
        if msg is None:
            break
        if msg.error():
            continue

        val = msg.value()

        # If expected plain JSON string, parse it
        if parse_json and isinstance(val, str):
            try:
                val = json.loads(val)
            except json.JSONDecodeError:
                continue

        buffer.append(val)


# Poll all streams
poll(st.session_state.energy_c, st.session_state.energy, parse_json=False)
poll(st.session_state.weather_c, st.session_state.weather, parse_json=False)

poll(st.session_state.trend_c, st.session_state.trends, parse_json=True)
poll(st.session_state.anomaly_c, st.session_state.anomalies, parse_json=True)
poll(st.session_state.eval_c, st.session_state.evals, parse_json=True)

# =====================================================
# ENERGY CONSUMPTION
# =====================================================
if len(st.session_state.energy) > 0:
    df_e = pd.DataFrame(list(st.session_state.energy))
    if "timestamp" in df_e.columns:
        df_e["timestamp"] = pd.to_datetime(df_e["timestamp"])
        df_e = df_e.sort_values("timestamp")

        st.subheader("Energy Consumption")
        st.line_chart(df_e.set_index("timestamp")["energy_consumption"])

# =====================================================
# TREND PANEL
# =====================================================
if len(st.session_state.trends) > 0:
    t = st.session_state.trends[-1]

    st.subheader("Trend Analysis")
    c1, c2, c3 = st.columns(3)

    c1.metric("Current Value", f"{t.get('energy_consumption', 0):.3f}")
    c2.metric("Moving Average", f"{t.get('moving_average', 0):.3f}")
    c3.metric("Trend", t.get("trend", "N/A"))

# =====================================================
# WEATHER
# =====================================================
if len(st.session_state.weather) > 0:
    df_w = pd.DataFrame(list(st.session_state.weather))
    if "timestamp" in df_w.columns:
        df_w["timestamp"] = pd.to_datetime(df_w["timestamp"])
        df_w = df_w.sort_values("timestamp")

        st.subheader("Weather Conditions")
        st.line_chart(
            df_w.set_index("timestamp")[["temperature", "humidity", "wind_speed"]]
        )

# =====================================================
# TREND – WEATHER CORRELATION
# =====================================================
if len(st.session_state.trends) > 0 and len(st.session_state.weather) > 0:
    st.subheader("Trend–Weather Correlation")

    df_t = pd.DataFrame(list(st.session_state.trends))
    df_w = pd.DataFrame(list(st.session_state.weather))

    df_t["timestamp"] = pd.to_datetime(df_t["timestamp"])
    df_w["timestamp"] = pd.to_datetime(df_w["timestamp"])

    try:
        df = pd.merge_asof(
            df_t.sort_values("timestamp"),
            df_w.sort_values("timestamp"),
            on="timestamp",
            tolerance=pd.Timedelta("5s"),
            direction="nearest"
        )

        df = df.dropna()

        if not df.empty:
            c1, c2, c3 = st.columns(3)
            c1.metric("Energy vs Temp", round(df["energy_consumption"].corr(df["temperature"]), 3))
            c2.metric("Energy vs Humidity", round(df["energy_consumption"].corr(df["humidity"]), 3))
            c3.metric("Energy vs Wind", round(df["energy_consumption"].corr(df["wind_speed"]), 3))
    except Exception as e:
        st.warning("Collecting more data for correlation...")

# =====================================================
# ANOMALY TIMELINE HEATMAP
# =====================================================
if len(st.session_state.anomalies) > 0:
    st.subheader("Anomaly Timeline Heatmap")

    df_a = pd.DataFrame(list(st.session_state.anomalies))
    if "timestamp" in df_a.columns:
        df_a["timestamp"] = pd.to_datetime(df_a["timestamp"])
        # Filter for anomalies
        df_a = df_a[df_a.get("pred_anomaly", 0) == 1]

        if not df_a.empty:
            df_a["date"] = df_a["timestamp"].dt.date
            df_a["hour"] = df_a["timestamp"].dt.hour

            heat = df_a.groupby(["date", "hour"]).size().unstack(fill_value=0)

            fig, ax = plt.subplots(figsize=(10, 4))
            sns.heatmap(heat, cmap="Reds", ax=ax)
            ax.set_xlabel("Hour")
            ax.set_ylabel("Date")
            st.pyplot(fig)
        else:
            st.info("No anomalies detected yet.")

# =====================================================
# EVALUATION METRICS DASHBOARD
# =====================================================
if len(st.session_state.evals) > 0:
    e = st.session_state.evals[-1]

    st.subheader("Model Evaluation Metrics")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Accuracy", f"{e.get('accuracy', 0):.3f}")
    c2.metric("Precision", f"{e.get('precision', 0):.3f}")
    c3.metric("Recall", f"{e.get('recall', 0):.3f}")
    c4.metric("F1", f"{e.get('f1', 0):.3f}")

    if "confusion_matrix" in e:
        cm = np.array(e["confusion_matrix"])
        fig, ax = plt.subplots()
        sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", ax=ax)
        ax.set_xlabel("Predicted")
        ax.set_ylabel("Actual")
        st.pyplot(fig)

# =====================================================
# Auto Refresh
# =====================================================
time.sleep(1)
st.rerun()