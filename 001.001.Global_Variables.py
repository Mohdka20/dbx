# Databricks notebook source
# DBTITLE 1,Get Environment
g_env = ''
# vWID = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId");
from dbruntime.databricks_repl_context import get_context
vWID = get_context().workspaceId

if  vWID == '1041975724377735' :
    g_env = 'DEV'
elif vWID == '1041975724377735':
    g_env = 'TST'    
elif vWID == '1041975724377735':
    g_env = "PRD"
else:
    g_env = ''



# COMMAND ----------

# DBTITLE 1,Set Global Variables

if g_env == 'DEV':
    #######################
    ## UC Catalog 
    #######################
    g_ucBronze = 'dev_hub_bronze'
    g_ucBronze_rto = 'dev_hub_bronze_rto'
    g_ucSilver = 'dev_hub_silver'
    g_ucGold = 'dev_hub_gold'

    #######################
    ## Storage Account 
    #######################
    g_01_saBronze = 'cbdchubraw01.dfs.core.windows.net'
    g_01_saSilver = 'cbdchubrefined01.dfs.core.windows.net'
    g_01_saGold = 'cbdchubaggregated01.dfs.core.windows.net'
    g_01_saLand = 'cbdchublanding01.dfs.core.windows.net'
    g_01_saLandAWS = 'cbdcawshublanding01.dfs.core.windows.net'
    g_01_saLandDS = 'cbdcdshublanding01.dfs.core.windows.net'

    #######################
    ## Data Engineer AD Group 
    #######################
    g_ad_group = 'BSF-Azure-ANL-Dev1-DataEng'

    #######################
    ## SQL information 
    #######################
    source_schema_across = 'ax@@yob1'
    connection_name_across = 'dev_sql_zos'

    source_schema_mdm = 'LOMTYO1'
    connection_name_mdm = 'dev_sql_mdm'

elif g_env == 'TST':
    #######################
    ## UC Catalog 
    #######################
    g_ucBronze = 'tst_hub_bronze'
    g_ucBronze_rto = 'tst_hub_bronze_rto'
    g_ucSilver = 'tst_hub_silver'
    g_ucGold = 'tst_hub_gold'

    g_ucBronze1 = 'tst1_hub_bronze'
    g_ucSilver1 = 'tst1_hub_silver'
    g_ucGold1 = 'tst1_hub_gold'  

    g_ucBronze2 = 'tst2_hub_bronze'
    g_ucSilver2 = 'tst2_hub_silver'
    g_ucGold2 = 'tst2_hub_gold'  

    g_ucBronze3 = 'tst3_hub_bronze'
    g_ucSilver3 = 'tst3_hub_silver'
    g_ucGold3 = 'tst3_hub_gold'    

    #######################
    ## Storage Account 
    #######################
    g_01_saBronze = 'cbtchubraw01.dfs.core.windows.net'
    g_01_saSilver = 'cbtchubrefined01.dfs.core.windows.net'
    g_01_saGold = 'cbtchubaggregated01.dfs.core.windows.net'
    g_01_saLand = 'cbtchublanding01.dfs.core.windows.net'
    g_01_saLandAWS = 'cbtcawshublanding01.dfs.core.windows.net'
    g_01_saLandDS = 'cbtcdshublanding01.dfs.core.windows.net'

    #######################
    ## Data Engineer AD Group 
    #######################
    g_ad_group = 'BSF-Azure-ANL-Test1-DataEng'

    #######################
    ## SQL information 
    #######################
    source_schema_across = 'AX@@HO1A' 
    connection_name_across = 'tst_sql_zos'

    source_schema_mdm = 'LOMTHO1'
    connection_name_mdm = 'tst_sql_mdm'

elif g_env == 'PRD':
    #######################
    ## UC Catalog 
    #######################
    g_ucBronze = 'hub_bronze'
    g_ucBronze_rto = 'hub_bronze_rto'
    g_ucSilver = 'hub_silver'
    g_ucGold = 'hub_gold'

    #######################
    ## Storage Account 
    #######################
    g_01_saBronze = 'cbpchubraw01.dfs.core.windows.net'
    g_01_saSilver = 'cbpchubrefined01.dfs.core.windows.net'
    g_01_saGold = 'cbpchubaggregated01.dfs.core.windows.net'
    g_01_saLand = 'cbpchublanding01.dfs.core.windows.net'
    g_01_saLandAWS = 'cbpcawshublanding01.dfs.core.windows.net'
    g_01_saLandDS = 'cbpcdshublanding01.dfs.core.windows.net'

    #######################
    ## Data Engineer AD Group 
    #######################
    g_ad_group = 'BSF-Azure-ANL-prod1-DataEng'

    #######################
    ## SQL information 
    #######################
    source_schema_across = 'AX@@PO#1'
    connection_name_across = 'sql_zos' 

    source_schema_mdm = 'Null'
    connection_name_mdm = 'sql_mdm'


# COMMAND ----------

# DBTITLE 1,Global Variables
print("********** Environment **********")
print("g_env: ", g_env)
print("********** UC Catalogs **********")
print("g_ucBronze: ", g_ucBronze)
print("g_ucBronze_rto: ", g_ucBronze_rto)
print("source_schema_across:", source_schema_across)
print("connection_name_across:", connection_name_across)
print("source_schema_mdm:", source_schema_mdm)
print("connection_name_mdm:", connection_name_mdm)
print("g_ucSilver: ", g_ucSilver)
print("g_ucGold: ", g_ucGold)
if g_env == 'TST':
    print("g_ucBronze1: ", g_ucBronze1)
    print("g_ucSilver1: ", g_ucSilver1)
    print("g_ucGold1: ", g_ucGold1)  
    print("g_ucBronze2: ", g_ucBronze2)
    print("g_ucSilver2: ", g_ucSilver2)
    print("g_ucGold2: ", g_ucGold2)  
    print("g_ucBronze3: ", g_ucBronze3)
    print("g_ucSilver3: ", g_ucSilver3)
    print("g_ucGold3: ", g_ucGold3)  
print("********** Storage Accounts **********")
print("g_01_saBronze: ", g_01_saBronze)
print("g_01_saSilver: ", g_01_saSilver)
print("g_01_saGold: ", g_01_saGold)
print("g_01_saLand: ", g_01_saLand) 
print("g_01_saLandAWS: ", g_01_saLandAWS) 
print("g_01_saLandDS: ", g_01_saLandDS)
print("********** DataEngineer Group **********")
print("g_ad_group: ", g_ad_group)

# COMMAND ----------

data = [
    ("DW_AX", "TCRRHDR", "DRV_REQ_NBR", 1),
    ("DW_AX", "TCRRHDR", "AX_CLNT_NBR", 2),
    ("DW_AX", "TCRRHDR", "SBRN_PGM_ID", 3),
    ("DW_AX", "TCRRHDR", "SBRN_PGM_ACNT_NBR", 4),
    ("DW_AX", "TCRRHDR", "SO_ID", 5),
    ("DW_AX", "TCRRLCL", "DRV_REQ_NBR", 1),
    ("DW_AX", "TCRRLCL", "AX_CLNT_NBR", 2),
    ("DW_AX", "TCRRLCL", "SBRN_PGM_ID", 3),
    ("DW_AX", "TCRRLCL", "SBRN_PGM_ACNT_NBR", 4),
    ("DW_AX", "TCRRLCL", "SO_ID", 5),
    ("DW_AX", "TCRRLCL", "REQ_VERS_NBR", 6),
    ("DW_AX", "TCRRLCL", "CLNT_FUNC_CDE", 7),
    ("DW_AX", "TCRRLCL", "EXT_CUS_CLNT_ID", 8),
    ("DW_AX", "TCRROO1", "DRV_REQ_NBR", 1),
    ("DW_AX", "TCRROO1", "AX_CLNT_NBR", 2),
    ("DW_AX", "TCRROO1", "SBRN_PGM_ID", 3),
    ("DW_AX", "TCRROO1", "SBRN_PGM_ACNT_NBR", 4),
    ("DW_AX", "TCRROO1", "SO_ID", 5),
    ("DW_AX", "TCRROO1", "REQ_VERS_NBR", 6),
    ("DW_AX", "TCRSTAT", "DRV_REQ_NBR", 1),
    ("DW_AX", "TCRSTAT", "AX_CLNT_NBR", 2),
    ("DW_AX", "TCRSTAT", "SBRN_PGM_ID", 3),
    ("DW_AX", "TCRSTAT", "SBRN_PGM_ACNT_NBR", 4),
    ("DW_AX", "TCRSTAT", "SO_ID", 5),
    ("DW_AX", "TCRSTAT", "REQ_VERS_NBR", 6),
    ("DW_AX", "TCRSTAT", "CLNT_REQ_SCDE", 7),
    ("DW_AX", "TCRSTAT", "AUDT_INS_TSTMP", 8)
]

columns = ["SCHEMA_NM", "TABLE_NM", "PK_COL_NM", "KEYSEQ"]
df_meta = spark.createDataFrame(data, columns)

df_meta.write.mode("overwrite").saveAsTable("dev_bronze.poc._meta")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_bronze.poc._meta
