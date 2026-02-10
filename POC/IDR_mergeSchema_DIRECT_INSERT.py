# Databricks notebook source
# MAGIC %md
# MAGIC ##Create Source and Target

# COMMAND ----------

src_tbl =  "idr_src"
tgt_tbl = "idr_tgt_02"

tgt_schema = "poc"


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS DEV_BRONZE.POC

# COMMAND ----------

spark.sql(f'''
Create table IF NOT EXISTS DEV_bronze.POC.{tgt_tbl}(

data_received_utc_dttm timestamp,
source_file_path string,
AX_CLNT_NBR int
)

''')

# COMMAND ----------

spark.sql(f'''
Create table IF NOT EXISTS DEV_bronze.POC.{src_tbl}(

data_received_utc_dttm timestamp,
source_file_path string,
AX_CLNT_NBR int,
is_expired boolean
)

''')

spark.sql(f"""
INSERT INTO DEV_bronze.POC.{src_tbl} (data_received_utc_dttm, source_file_path, AX_CLNT_NBR, is_expired) VALUES
(current_timestamp(), 'file1.csv', 1001, false),
(current_timestamp(), 'file2.csv', 1002, false),
(current_timestamp(), 'file1.csv', 1001, false), -- duplicate
(current_timestamp(), 'file3.csv', 1003, false)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dedup & Ingest  using 'AUTO LOADER'

# COMMAND ----------

# def sync_idr_tables():
#     # Deduplicate source
#     deduped = spark.sql(f'''
#         SELECT *
#         FROM (
#             SELECT *,
#                    ROW_NUMBER() OVER (PARTITION BY AX_CLNT_NBR
#                                       ORDER BY data_received_utc_dttm DESC) AS rn
#             FROM dev_bronze.poc`.idr_src
#         ) t
#         WHERE rn = 1
#     ''')
#     final = deduped.drop("rn")
#     final.createOrReplaceTempView("final")
#     display(final)
#     spark.sql(f"""
#         ALTER TABLE dev_bronze.poc`.{tgt_tbl}
#         SET TBLPROPERTIES (delta.columnMapping.mode = "name",
#                            delta.schema.autoMerge.enabled = true)
#     """)
#     # Merge with schema evolution (allow new columns)
#     spark.sql(f"""
#         INSERT OVERWRITE
#         INTO {g_ucBronze}.`{tgt_schema}`.{tgt_tbl} 
#         AS(
#             SELECT *
#             FROM final
#         )

#     """)

# Deduplicate source
deduped = spark.sql(f'''
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY AX_CLNT_NBR
                                  ORDER BY data_received_utc_dttm DESC) AS rn
        FROM dev_bronze.`{tgt_schema}`.{src_tbl}
    ) t
    WHERE rn = 1
''')
final = deduped.drop("rn")
display(final)

# Ensure table properties for schema evolution
spark.sql(f'''
    ALTER TABLE dev_bronze.`{tgt_schema}`.{tgt_tbl}
    SET TBLPROPERTIES ('delta.columnMapping.mode' = "name",
                       'delta.schema.autoMerge.enabled '= true)
''')

# Overwrite target with deduped data, allowing schema evolution
spark.sql(f'''
    INSERT OVERWRITE TABLE dev_bronze.`{tgt_schema}`.{tgt_tbl}
    SELECT * FROM final
''')\
    

# COMMAND ----------

# MAGIC %sql
# MAGIC     ALTER TABLE dev_bronze.poc`.idr_tgt_02
# MAGIC     SET TBLPROPERTIES (delta.columnMapping.mode = "name",
# MAGIC                        'delta.merge.enabled'= true)

# COMMAND ----------

display(spark.sql(f"""SELECT * FROM dev_bronze.poc`.idr_src"""))
display(spark.sql(f"""SELECT * FROM dev_bronze.poc`.idr_tgt_02"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table dev_bronze.poc`.idr_tgt_01

# COMMAND ----------

spark.sql(f"""
INSERT INTO dev_bronze.poc`.idr_src (data_received_utc_dttm, source_file_path, AX_CLNT_NBR, is_expired)
VALUES (current_timestamp(), 'sample_path.csv', 1001, false)
""")
