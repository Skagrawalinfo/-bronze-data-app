# streamlit_app.py

import streamlit as st
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="Bronze Data Packet Generator", layout="wide")

st.title("Bronze Layer Data Packet Generator")
st.write("Upload a text file to generate a structured Bronze dataset for Databricks.")

uploaded_file = st.file_uploader("Upload Text File", type=["txt", "log", "csv"])

if uploaded_file is not None:
    content = uploaded_file.read().decode("utf-8")
    lines = content.splitlines()

    st.subheader("Raw File Preview")
    st.text(content[:1000])

    # ✅ Bronze Data Packet Generator
    def generate_bronze_packet(lines, file_name):
        ingestion_time = datetime.utcnow()

        records = []
        for idx, line in enumerate(lines):
            records.append({
                "record_id": idx + 1,
                "raw_text": line,
                "ingestion_timestamp": ingestion_time,
                "source_file_name": file_name,
                "data_length": len(line),
                "is_empty": len(line.strip()) == 0,
                "load_date": ingestion_time.date().isoformat()
            })

        return pd.DataFrame(records)

    bronze_df = generate_bronze_packet(lines, uploaded_file.name)

    st.subheader("Bronze Data Preview")
    st.dataframe(bronze_df.head(100))

    # ✅ Schema
    st.subheader("Bronze Schema")
    schema = {
        "record_id": "int",
        "raw_text": "string",
        "ingestion_timestamp": "timestamp",
        "source_file_name": "string",
        "data_length": "int",
        "is_empty": "boolean",
        "load_date": "date"
    }
    st.json(schema)

    # ✅ Downloads
    st.subheader("Download Data")

    csv = bronze_df.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", csv, "bronze_data.csv", "text/csv")

    json_data = bronze_df.to_json(orient="records", indent=2)
    st.download_button("Download JSON", json_data, "bronze_data.json", "application/json")

    # ✅ Databricks SQL (Bronze Table)
    st.subheader("Databricks Bronze Table DDL")
    ddl = """
CREATE TABLE IF NOT EXISTS bronze_text_data (
    record_id INT,
    raw_text STRING,
    ingestion_timestamp TIMESTAMP,
    source_file_name STRING,
    data_length INT,
    is_empty BOOLEAN,
    load_date DATE
)
USING DELTA
PARTITIONED BY (load_date);
"""
    st.code(ddl, language="sql")

    # ✅ PySpark Load Code
    st.subheader("Databricks PySpark Load Code")
    pyspark_code = """
from pyspark.sql.functions import current_timestamp

# Read file
df = spark.read.option("header", True).csv("/dbfs/FileStore/bronze_data.csv")

# Add ingestion timestamp
df = df.withColumn("ingestion_timestamp", current_timestamp())

# Write to Bronze Delta Table
df.write.format("delta") \\
    .mode("append") \\
    .partitionBy("load_date") \\
    .saveAsTable("bronze_text_data")
"""
    st.code(pyspark_code, language="python")

    st.success("✅ Bronze data packet generated successfully!")

else:
    st.info("Upload a file to begin.")
