# Databricks notebook source
# MAGIC %md
# MAGIC ##Create Source and Target

# COMMAND ----------

# MAGIC %run /Workspace/Shared/Bronze_Data_Ingestion/001.Admin/001.001.Global_Variables

# COMMAND ----------

src_tbl =  "idr_src"
tgt_tbl = "idr_tgt_01"

tgt_schema = "lh-external-source-2"


# COMMAND ----------

spark.sql(f'''
Create table IF NOT EXISTS dev_hub_bronze.`lh-external-source-2`.{tgt_tbl}(

data_received_utc_dttm timestamp,
source_file_path string,
AX_CLNT_NBR int
)
using delta
location 'abfss://lh-external-source@cbdchubraw01.dfs.core.windows.net/IDR_schema_change/{tgt_tbl}/'
''')

display(spark.sql(f"SELECT * FROM dev_hub_bronze.`lh-external-source-2`.idr_tgt_01"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dedup & Ingest  using 'schema evolution'

# COMMAND ----------

def sync_idr_tables():
    # Deduplicate source
    deduped = spark.sql(f'''
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY AX_CLNT_NBR
                                      ORDER BY data_received_utc_dttm DESC) AS rn
            FROM dev_hub_bronze.`lh-external-source-2`.idr_src
        ) t
        WHERE rn = 1
    ''')
    final = deduped.drop("rn")
    final.createOrReplaceTempView("final")
    display(final)

    # Merge with schema evolution (allow new columns)
    spark.sql(f"""
        MERGE  WITH SCHEMA EVOLUTION
        INTO {g_ucBronze}.`{tgt_schema}`.{tgt_tbl} 
        USING final 
        ON false -- ON s.pk = t.pk
        -- WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

sync_idr_tables()

# COMMAND ----------

display(spark.sql(f"""SELECT * FROM dev_hub_bronze.`lh-external-source-2`.idr_src"""))
display(spark.sql(f"""SELECT * FROM dev_hub_bronze.`lh-external-source-2`.idr_tgt_01"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table dev_hub_bronze.`lh-external-source-2`.idr_tgt_01

# COMMAND ----------

dbutils.fs.rm("abfss://lh-external-source@cbdchubraw01.dfs.core.windows.net/IDR_schema_change/idr_tgt_01/", True)

# COMMAND ----------

spark.sql(f"""
INSERT INTO dev_hub_bronze.`lh-external-source-2`.idr_src (data_received_utc_dttm, source_file_path, AX_CLNT_NBR, is_expired)
VALUES (current_timestamp(), 'sample_path.csv', 1001, false)
""")
