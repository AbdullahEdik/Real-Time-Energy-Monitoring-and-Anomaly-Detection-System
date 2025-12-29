# AIN429 Project — Energy Anomaly Detector

Small toolkit for cleaning, streaming, analyzing, and visualizing smart meter energy data and detecting anomalies.

## Quickstart (Windows)

1. Create and activate a virtual environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Install dependencies (if you have a `requirements.txt`, use it; otherwise install common packages)

```powershell
pip install -r requirements.txt
```

3. Run the preprocessing + detection pipeline

```powershell
python run_pipeline.py
```

## Contents
- Project scripts
  - [anomaly_detector.py](anomaly_detector.py): anomaly detection logic and model pipeline
  - [clean.py](clean.py): data cleaning and preprocessing steps
  - [producer.py](producer.py): synthetic / streaming data producer
  - [run_pipeline.py](run_pipeline.py): orchestration for preprocessing and detection
  - [trend_analyzer.py](trend_analyzer.py): trend analysis utilities
  - [streamlit_dashboard.py](streamlit_dashboard.py): Streamlit UI for visualization
- Config and data
  - [config.py](config.py): configuration values
  - [smart_meter_data.csv](smart_meter_data.csv): sample dataset
- Schemas
  - [schemas/anomaly_schema.json](schemas/anomaly_schema.json)
  - [schemas/energy_schema.json](schemas/energy_schema.json)
  - [schemas/eval_schema.json](schemas/eval_schema.json)
  - [schemas/trend_schema.json](schemas/trend_schema.json)
  - [schemas/weather_schema.json](schemas/weather_schema.json)

