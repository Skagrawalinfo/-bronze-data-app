# enterprise_streamlit_ingestion.py

import streamlit as st
import pandas as pd
import json
from datetime import datetime

st.set_page_config(page_title="Enterprise Bronze Ingestion", layout="wide")

st.title("🚀 Enterprise Bronze Ingestion Framework")
st.write("Supports structured + unstructured ingestion with data quality checks (DPR Ready)")

# ✅ Config
mode = st.radio(
    "Ingestion Mode",
    ["Auto Detect", "Structured (CSV/JSON)", "Raw (TXT/LOG)"]
)

uploaded_file = st.file_uploader("Upload File", type=["csv", "txt", "log", "json"])

# ✅ Data Quality Check Function
def run_dq_checks(df):
    dq_summary = {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "null_counts": df.isnull().sum().to_dict(),
        "duplicate_rows": int(df.duplicated().sum())
    }
    return dq_summary

# ✅ Metadata enrichment
def add_metadata(df, file_name):
    ingestion_time = datetime.utcnow()

    df["ingestion_timestamp"] = ingestion_time
    df["source_file_name"] = file_name
    df["load_date"] = ingestion_time.date().isoformat()

    return df

# ✅ RAW TEXT PROCESSOR
def process_raw(file, file_name):
    content = file.read().decode("utf-8", errors="ignore")
    lines = content.splitlines()

    ingestion_time = datetime.utcnow()

    records = []
    for idx, line in enumerate(lines):
        records.append({
            "record_id": idx + 1,
            "raw_text": line,
            "data_length": len(line),
            "is_empty": len(line.strip()) == 0,
            "ingestion_timestamp": ingestion_time,
            "source_file_name": file_name,
            "load_date": ingestion_time.date().isoformat()
        })

    return pd.DataFrame(records)

# ✅ STRUCTURED PROCESSOR
def process_structured(file, file_name, file_type):
    try:
        if file_type == "csv":
            df = pd.read_csv(file)

        elif file_type == "json":
            df = pd.read_json(file)

        else:
            raise ValueError("Unsupported structured format")

        df = add_metadata(df, file_name)

        return df

    except Exception as e:
        st.error(f"Error processing structured file: {e}")
        return None

# ✅ MAIN FLOW
if uploaded_file is not None:

    file_name = uploaded_file.name
    file_type = file_name.split(".")[-1].lower()

    st.subheader("📄 File Info")
    st.write(f"File Name: {file_name}")
    st.write(f"Detected Type: {file_type}")

    # Reset file pointer (important for re-reads)
    uploaded_file.seek(0)

    # ✅ Mode logic
    if mode == "Auto Detect":
        if file_type in ["csv", "json"]:
            bronze_df = process_structured(uploaded_file, file_name, file_type)
        else:
            bronze_df = process_raw(uploaded_file, file_name)

    elif mode == "Structured (CSV/JSON)":
        bronze_df = process_structured(uploaded_file, file_name, file_type)

    else:
        bronze_df = process_raw(uploaded_file, file_name)

    # ✅ If processing successful
    if bronze_df is not None:

        st.subheader("✅ Bronze Data Preview")
        st.dataframe(bronze_df.head(100))

        # ✅ DQ Checks
        st.subheader("🔍 Data Quality Summary (DPR)")
        dq_summary = run_dq_checks(bronze_df)
        st.json(dq_summary)

        # ✅ Schema View
        st.subheader("📊 Schema")
        schema = {col: str(bronze_df[col].dtype) for col in bronze_df.columns}
        st.json(schema)

        # ✅ Download Options
        st.subheader("⬇️ Download")

        csv_data = bronze_df.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", csv_data, "bronze_data.csv", "text/csv")

        json_data = bronze_df.to_json(orient="records", indent=2)
        st.download_button("Download JSON", json_data, "bronze_data.json", "application/json")

        # ✅ Databricks DDL Generator
        st.subheader("🧱 Databricks Bronze Table DDL")

        ddl_cols = []
        for col, dtype in bronze_df.dtypes.items():
            if "int" in str(dtype):
                ddl_type = "INT"
            elif "float" in str(dtype):
                ddl_type = "DOUBLE"
            elif "bool" in str(dtype):
                ddl_type = "BOOLEAN"
            else:
                ddl_type = "STRING"

            ddl_cols.append(f"{col} {ddl_type}")

        ddl = f"""
CREATE TABLE IF NOT EXISTS bronze_auto_table (
    {', '.join(ddl_cols)}
)
USING DELTA
PARTITIONED BY (load_date);
"""
        st.code(ddl, language="sql")

        # ✅ PySpark Load Code
        st.subheader("⚙️ Databricks PySpark Load")

        pyspark_code = """
df = spark.read.option("header", True).csv("/dbfs/FileStore/bronze_data.csv")

df.write.format("delta") \\
  .mode("append") \\
  .partitionBy("load_date") \\
  .saveAsTable("bronze_auto_table")
"""
        st.code(pyspark_code, language="python")

        st.success("✅ Enterprise Bronze ingestion completed!")

else:
    st.info("Upload a file to begin.")
