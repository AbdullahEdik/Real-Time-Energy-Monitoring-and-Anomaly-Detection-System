"""
Hybrid anomaly detector + evaluation (STREAMING SERVICE)
Consumes:  ENERGY_TOPIC (JSON Schema Registry)
Produces:  ANOMALY_TOPIC (plain JSON), EVAL_TOPIC (plain JSON)

Methods:
- River Half-Space Trees (online anomaly detection)
- STL residual z-score (statistical validation)

Evaluation:
- Compares model prediction with ground truth anomaly label
- Every eval_interval samples publishes:
    - Accuracy, Precision, Recall, F1
    - Confusion Matrix
"""

from collections import deque
from typing import Any, Dict, Optional
import json
import numpy as np

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix

from statsmodels.tsa.seasonal import STL
import config

from river import anomaly
from river import preprocessing

# ------- Topics ---------
ENERGY_TOPIC = getattr(config, "ENERGY_TOPIC", "energy_stream")
ANOMALY_TOPIC = getattr(config, "ANOMALY_TOPIC", "anomaly_stream")
EVAL_TOPIC = getattr(config, "EVAL_TOPIC", "evaluation_stream")


# ---- JSON SCHEMA (input) ----
with open("schemas/energy_schema.json", "r", encoding="utf-8") as f:
    ENERGY_SCHEMA = f.read()


def identity(obj, ctx):
    return obj


def create_energy_consumer() -> DeserializingConsumer:
    """
    Consumes ENERGY_TOPIC using Schema Registry JSONDeserializer
    """
    schema_registry = SchemaRegistryClient(config.sr_conf)
    json_deserializer = JSONDeserializer(
        schema_str=ENERGY_SCHEMA,
        schema_registry_client=schema_registry,
        from_dict=identity,
    )

    return DeserializingConsumer({
        "bootstrap.servers": config.kafka_conf["bootstrap.servers"],
        "security.protocol": config.kafka_conf["security.protocol"],
        "sasl.mechanism": config.kafka_conf["sasl.mechanism"],
        "sasl.username": config.kafka_conf["sasl.username"],
        "sasl.password": config.kafka_conf["sasl.password"],
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": json_deserializer,
        "group.id": "hybrid-anomaly-eval-service",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })


def create_json_producer() -> SerializingProducer:
    """
    Produces to ANOMALY_TOPIC and EVAL_TOPIC as plain JSON string.
    This avoids requiring SR client + extra deps on the dashboard side.
    """
    prod_conf = config.kafka_conf.copy()
    prod_conf["key.serializer"] = StringSerializer("utf_8")
    prod_conf["value.serializer"] = StringSerializer("utf_8")
    return SerializingProducer(prod_conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"[PRODUCER][ERROR] Delivery failed: {err}")


# ------- HYBRID ANOMALY FUNCTIONS --------
def stl_anomaly(values: np.ndarray, period: int = 48, z_thresh: float = 2.5):
    """
    STL residual-based z-score on latest point.
    """
    if len(values) < period * 2:
        return False, 0.0

    stl = STL(values, period=period, robust=True).fit()
    resid = stl.resid

    std = float(np.std(resid))
    if std == 0.0:
        return False, 0.0

    z = float(resid[-1] / std)
    return abs(z) > z_thresh, z


def safe_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


# 1. Initialize the pipeline
# We use a StandardScaler because HST is sensitive to feature scales
hst_model = preprocessing.StandardScaler() | anomaly.HalfSpaceTrees(
    n_trees=25,
    height=15,
    window_size=100, # Matches your current WINDOW logic
    seed=42
)

def hst_streaming_anomaly(val: float):
    """
    Updates the model with ONE new point and returns the anomaly score.
    River expects a dict of features.
    """
    feature_dict = {'energy': val}

    score = hst_model.score_one(feature_dict)
    
    hst_model.learn_one(feature_dict)

    is_anomaly = score > 0.8
    
    return is_anomaly, score

def main():
    consumer = create_energy_consumer()
    producer = create_json_producer()
    consumer.subscribe([ENERGY_TOPIC])

    values = deque(maxlen=96)
    true_labels = []
    pred_labels = []
    eval_interval = 100

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            rec = msg.value()
            if rec is None: continue

            # 1. Extract Data
            ts = rec.get("timestamp")
            val = safe_float(rec.get("energy_consumption"))
            raw_gt = rec.get("original_label", "Normal")
            gt = 1 if raw_gt != "Normal" else 0
            
            if val is None: continue

            # 2. Update Streaming Models
            hst_flag, hst_score = hst_streaming_anomaly(val)
            
            # 3. STL still needs a window (warm-up required)
            values.append(val)
            stl_flag, stl_z = False, 0.0
            if len(values) >= 20: # Use the same warm-up as before
                stl_flag, stl_z = stl_anomaly(np.array(values, dtype=float))

            # 4. Final Hybrid Decision
            final_anom = int(hst_flag and stl_flag)
            
            # Record for Evaluation
            true_labels.append(gt)
            pred_labels.append(final_anom)

            # 5. Produce Anomaly Event (Updated fields)
            anomaly_event = {
                "timestamp": ts,
                "device_id": rec.get("device_id"),
                "energy_consumption": val,
                "gt_label": gt,
                "pred_anomaly": final_anom,
                "hst_score": float(hst_score),
                "stl_z": float(stl_z)
            }

            producer.produce(
                topic=ANOMALY_TOPIC,
                key=str(ts),
                value=json.dumps(anomaly_event),
                on_delivery=delivery_report
            )
            producer.poll(0)

            # ---- Evaluation publish every N samples ----
            if len(true_labels) % eval_interval == 0:
                y_true = np.array(true_labels, dtype=int)
                y_pred = np.array(pred_labels, dtype=int)

                acc = float(accuracy_score(y_true, y_pred))
                prec = float(precision_score(y_true, y_pred, zero_division=0))
                rec_ = float(recall_score(y_true, y_pred, zero_division=0))
                f1 = float(f1_score(y_true, y_pred, zero_division=0))
                cm = confusion_matrix(y_true, y_pred).tolist()

                # console
                print("\n===== [EVALUATION METRICS] =====")
                print("Confusion Matrix:")
                print(np.array(cm))
                print(
                    f"Accuracy={acc:.4f} | Precision={prec:.4f} "
                    f"| Recall={rec_:.4f} | F1={f1:.4f}"
                )
                print("================================\n")

                eval_event = {
                    "timestamp": ts,
                    "samples": int(len(y_true)),
                    "accuracy": acc,
                    "precision": prec,
                    "recall": rec_,
                    "f1": f1,
                    "confusion_matrix": cm,
                    "notes": "Metrics computed on streaming predictions vs ground-truth labels from dataset.",
                }

                producer.produce(
                    topic=EVAL_TOPIC,
                    key=str(ts),
                    value=json.dumps(eval_event),
                    on_delivery=delivery_report,
                )
                producer.poll(0)

    except KeyboardInterrupt:
        print("\n[ANOMALY] Terminated by user.")
    finally:
        try:
            print("[ANOMALY] Flushing producer...")
            producer.flush(10)
        except Exception:
            pass
        consumer.close()


if __name__ == "__main__":
    main()
