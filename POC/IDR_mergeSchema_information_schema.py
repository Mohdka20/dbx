# Databricks notebook source
# MAGIC %md
# MAGIC ##Create Source and Target

# COMMAND ----------

# MAGIC %run /Workspace/Shared/Bronze_Data_Ingestion/001.Admin/001.001.Global_Variables

# COMMAND ----------

src_tbl =  "idr_src"
tgt_tbl = "idr_tgt"

tgt_schema = "lh-external-source-2"


# COMMAND ----------

spark.sql(f'''
Create table if not exists dev_hub_bronze.`lh-external-source-2`.{src_tbl}
using delta
location 'abfss://lh-external-source@cbdchubraw01.dfs.core.windows.net/IDR_schema_change/{src_tbl}/'
as (
select 
data_received_utc_dttm,
source_file_path,
AX_CLNT_NBR
from dev_hub_bronze.lh_ax_idr.tcrrhdr)
''')

# COMMAND ----------

spark.sql(f'''
Create table dev_hub_bronze.`lh-external-source-2`.{tgt_tbl}
using delta
location 'abfss://lh-external-source@cbdchubraw01.dfs.core.windows.net/IDR_schema_change/{tgt_tbl}/'
as (
select 
*
from dev_hub_bronze.`lh-external-source-2`.idr_src)
''')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Adding extra column to idr source table

# COMMAND ----------

spark.sql("""
ALTER TABLE dev_hub_bronze.`lh-external-source-2`.idr_src
ADD COLUMNS (is_expired BOOLEAN)
""")

# Populate the new column with TRUE for all existing rows
spark.sql("""
UPDATE dev_hub_bronze.`lh-external-source-2`.idr_src
SET is_expired = false
""")

# Verify the column was added and populated
display(spark.sql("""
SELECT *
FROM dev_hub_bronze.`lh-external-source-2`.idr_src
LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dedup & Ingest  using 'INSERT OVERWRITE'
# MAGIC ### update schema using information_schema

# COMMAND ----------

def sync_idr_tables():
    src_meta = spark.sql(f"""
        SELECT column_name, data_type
        FROM system.information_schema.columns 
        WHERE table_catalog = '{g_ucBronze}'
          AND table_schema = '{tgt_schema}'
          AND table_name = 'idr_src'
    """)

    tgt_meta = spark.sql(f"""
        SELECT column_name, data_type
        FROM system.information_schema.columns 
        WHERE table_catalog = '{g_ucBronze}'
          AND table_schema = '{tgt_schema}'
          AND table_name = 'idr_tgt'
    """)

    # Convert to Python sets
    src_cols = set((row.column_name, row.data_type) for row in src_meta.collect())
    tgt_cols = set((row.column_name, row.data_type) for row in tgt_meta.collect())

    # Determine differences
    only_in_src = src_cols - tgt_cols

    # Add missing columns from source to target
    for col_name, data_type in only_in_src:
        spark.sql(f"""
            ALTER TABLE dev_hub_bronze.`{tgt_schema}`.{tgt_tbl}
            ADD COLUMNS ({col_name} {data_type})
        """)

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

    # Overwrite target table with deduplicated data
    spark.sql(f"""
        INSERT OVERWRITE {g_ucBronze}.`{tgt_schema}`.{tgt_tbl}
        SELECT *
        FROM final
    """)

# Execute the function
sync_idr_tables()

# COMMAND ----------

display(spark.sql(f"""SELECT * FROM dev_hub_bronze.`lh-external-source-2`.idr_src"""))
display(spark.sql(f"""SELECT * FROM dev_hub_bronze.`lh-external-source-2`.idr_tgt"""))

# COMMAND ----------


