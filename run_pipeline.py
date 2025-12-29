import subprocess
import sys
import time
import os

processes = []

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def start_python(script_name, label, wait=2):
    path = os.path.join(BASE_DIR, script_name)
    print(f"Starting {label}: {path}")
    p = subprocess.Popen([sys.executable, path])
    processes.append(p)
    time.sleep(wait)
    return p


try:
    # Producer
    start_python("producer.py", "Kafka Producer", wait=4)

    # Trend Analyzer
    start_python("trend_analyzer.py", "Trend Analyzer", wait=2)

    # Anomaly Detector
    start_python("anomaly_detector.py", "Anomaly Detector", wait=2)

    # Dashboard
    print("Starting Streamlit Dashboard...")
    dashboard = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run",
         os.path.join(BASE_DIR, "streamlit_dashboard.py")]
    )
    processes.append(dashboard)

    print("\nPIPELINE IS RUNNING")
    print("Dashboard: http://localhost:8501")
    print("Press CTRL+C to stop everything\n")

    while True:
        time.sleep(1)

except KeyboardInterrupt:
    print("\nShutting down pipeline...")
    for p in processes:
        try:
            p.terminate()
        except Exception:
            pass
    print("All processes terminated.")
