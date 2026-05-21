# enterprise_full_framework.py

import streamlit as st
import pandas as pd
import json
from datetime import datetime
import numpy as np

st.set_page_config(page_title="Enterprise Data Ingestion Framework", layout="wide")

st.title("🚀 Enterprise Data Ingestion + DPR + AI Framework")

# =========================
# CONFIG
# =========================
mode = st.radio(
    "Ingestion Mode",
    ["Auto Detect", "Structured", "Raw"]
)

dq_threshold_null = st.slider("Null % Threshold", 0, 100, 20)
dup_threshold = st.slider("Duplicate Threshold", 0, 100, 5)

uploaded_file = st.file_uploader("Upload File", type=["csv", "txt", "log", "json"])

# =========================
# HELPERS
# =========================

def add_metadata(df, file_name):
    now = datetime.utcnow()
    df["ingestion_timestamp"] = now
    df["source_file_name"] = file_name
    df["load_date"] = now.date().isoformat()
    return df

# -------------------------
# RAW PROCESS
# -------------------------
def process_raw(file, file_name):
    content = file.read().decode("utf-8", errors="ignore")
    lines = content.splitlines()
    now = datetime.utcnow()

    records = []
    for i, line in enumerate(lines):
        records.append({
            "record_id": i + 1,
            "raw_text": line,
            "length": len(line),
            "is_empty": len(line.strip()) == 0,
            "ingestion_timestamp": now,
            "source_file_name": file_name,
            "load_date": now.date().isoformat()
        })
    return pd.DataFrame(records)

# -------------------------
# STRUCTURED PROCESS
# -------------------------
def process_structured(file, file_name, file_type):
    if file_type == "csv":
        df = pd.read_csv(file)
    elif file_type == "json":
        df = pd.read_json(file)
    else:
        raise Exception("Unsupported format")

    return add_metadata(df, file_name)

# =========================
# PHASE 3: DATA CONTRACT
# =========================
def validate_contract(df):
    contract_issues = []

    if len(df.columns) < 3:
        contract_issues.append("Too few columns")

    if "ingestion_timestamp" not in df.columns:
        contract_issues.append("Missing ingestion_timestamp")

    return contract_issues

# =========================
# PHASE 3: DPR QA
# =========================
def dq_checks(df):
    total_rows = len(df)

    dq = {
        "rows": total_rows,
        "columns": len(df.columns),
        "duplicate_rows": int(df.duplicated().sum()),
        "null_percent": ((df.isnull().sum() / total_rows) * 100).to_dict()
    }

    return dq

# =========================
# PHASE 4: AI-lite anomaly
# =========================
def anomaly_detection(df):
    anomalies = {}

    numeric_cols = df.select_dtypes(include=np.number).columns

    for col in numeric_cols:
        series = df[col]
        if len(series) > 0:
            mean = series.mean()
            std = series.std() if series.std() != 0 else 1

            outliers = series[(series > mean + 3*std) | (series < mean - 3*std)]
            anomalies[col] = len(outliers)

    return anomalies

# =========================
# MAIN FLOW
# =========================
if uploaded_file:

    file_name = uploaded_file.name
    file_type = file_name.split(".")[-1].lower()

    st.subheader("📄 File Info")
    st.write(file_name, "| Type:", file_type)

    uploaded_file.seek(0)

    # PHASE 1/2 ingestion
    try:
        if mode == "Auto Detect":
            if file_type in ["csv", "json"]:
                df = process_structured(uploaded_file, file_name, file_type)
            else:
                df = process_raw(uploaded_file, file_name)

        elif mode == "Structured":
            df = process_structured(uploaded_file, file_name, file_type)

        else:
            df = process_raw(uploaded_file, file_name)

    except Exception as e:
        st.error(f"Ingestion failed: {e}")
        st.stop()

    # =========================
    # OUTPUT
    # =========================
    st.subheader("✅ Bronze Data")
    st.dataframe(df.head(100))

    # =========================
    # PHASE 3: CONTRACT
    # =========================
    st.subheader("📜 Data Contract Validation")
    contract = validate_contract(df)

    if contract:
        st.error(contract)
    else:
        st.success("Contract Passed")

    # =========================
    # PHASE 3: DQ CHECKS
    # =========================
    st.subheader("🔍 Data Quality (DPR)")

    dq = dq_checks(df)
    st.json(dq)

    # flag issues
    if dq["duplicate_rows"] > dup_threshold:
        st.warning("High duplicate rows!")

    for col, null_pct in dq["null_percent"].items():
        if null_pct > dq_threshold_null:
            st.warning(f"High nulls in {col}: {round(null_pct,2)}%")

    # =========================
    # PHASE 4: ANOMALY
    # =========================
    st.subheader("🤖 AI Anomaly Detection")

    anomalies = anomaly_detection(df)
    st.json(anomalies)

    # =========================
    # EXPORT
    # =========================
    st.subheader("⬇️ Export")

    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", csv, "bronze.csv")

    # =========================
    # DATABRICKS DDL
    # =========================
    st.subheader("🧱 Databricks DDL")

    ddl_cols = []
    for col, dtype in df.dtypes.items():
        if "int" in str(dtype):
            t = "INT"
        elif "float" in str(dtype):
            t = "DOUBLE"
        elif "bool" in str(dtype):
            t = "BOOLEAN"
        else:
            t = "STRING"

        ddl_cols.append(f"{col} {t}")

    ddl = f"""
CREATE TABLE bronze_auto (
{",".join(ddl_cols)}
)
USING DELTA
PARTITIONED BY (load_date);
"""
    st.code(ddl, language="sql")

    # =========================
    # PHASE 2: DATABRICKS LOAD
    # =========================
    st.subheader("⚙️ Databricks Load Code")

    code = """
df = spark.read.option("header", True).csv("/dbfs/FileStore/bronze.csv")

df.write.format("delta") \\
  .mode("append") \\
  .partitionBy("load_date") \\
  .saveAsTable("bronze_auto")
"""
    st.code(code, language="python")

    st.success("✅ End-to-end ingestion complete!")

else:
    st.info("Upload file to start")
