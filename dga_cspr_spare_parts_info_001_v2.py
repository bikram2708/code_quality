# Databricks notebook source
import json

dbutils.widgets.text("environment", "")
env = dbutils.widgets.get("environment")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Importing all the config variables

# COMMAND ----------

# to be un-commented only when this notebook is run for testing
# env="D"

# COMMAND ----------

print(env)

# COMMAND ----------

config = dbutils.notebook.run("dga_guardian_config_000_v2", 60, {"environment": env})
configs = json.loads(config)
pwd = configs["pwd"]
blob_pwd = configs["blob_pwd"]
user = configs["user"]
SDTB_HOST = configs["SDTB_HOST"]
LOGMECH = configs["LOGMECH"]
driver = configs["driver"]
url = configs["url"]
fetchsize = configs["fetchsize"]
jdbcUsername = configs["jdbcUsername"]
jdbcHostname = configs["jdbcHostname"]
jdbcDatabase = configs["jdbcDatabase"]

# COMMAND ----------

if env == "D":
    scope = "sdtbguardianreportingl"
    pwd = dbutils.secrets.get(scope=scope, key="projectaccountpwd")
    blob_pwd = dbutils.secrets.get(scope=scope, key="projectstorageaccountaccesskey")
    user = dbutils.secrets.get(scope=scope, key="projectaccountuser")
    projectstorageaccount = dbutils.secrets.get(
        scope=scope, key="projectstorageaccount"
    )
    jdbcUsername = dbutils.secrets.get(scope=scope, key="projectaccountemail")


elif env == "P":
    scope = "sdtbguardianreportingp"
    pwd = dbutils.secrets.get(scope=scope, key="projectaccountpwd")
    blob_pwd = dbutils.secrets.get(scope=scope, key="projectstorageaccountaccesskey")
    user = dbutils.secrets.get(scope=scope, key="projectaccountuser")
    projectstorageaccount = dbutils.secrets.get(
        scope=scope, key="projectstorageaccount"
    )
    jdbcUsername = dbutils.secrets.get(scope=scope, key="projectaccountemail")

else:
    sys.exit("Please pass either of these two values 'D' for lab or 'P' for prod")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Importing the utilities

# COMMAND ----------

# MAGIC %run ./dga_guardian_functions_001_v2

# COMMAND ----------

# MAGIC %run ./dga_guardian_logging_006_v2

# COMMAND ----------

logger = logging.getLogger("cspr_spare_parts_info_v2")
logger.setLevel(logging.INFO)
logger.addHandler(FileHandler)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Extract Required Columns from Spare Parts View (SDTB)

# COMMAND ----------

spare_parts_qry = (
    """
(select
SYS_MAT_ID,
SYS_SERIAL_ID,
SP_MAT_ID as EQ_MAT_ID,
SP_MAT_TEXT as EQ_MAT_TEXT,
SP_QUANTITY as SP_QUANTITY_CONSUMED,
SP_DOC_DT as DOC_DATE,
NOTI_ID,
cast(TABLE_LOAD_DT_TM as date) as BAS_LOAD_DT
from 
SHS_"""
    + env
    + """_SDM_SERVICE_PARTS.SDTB_DP_SPA_V_PARTS_CONSUMED_V2
where SYS_MAT_ID is not null and SYS_SERIAL_ID is not null)
qry
"""
)

# COMMAND ----------

spare_parts_df_new = query_td(spare_parts_qry).dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ######Test SQL Storage Connection

# COMMAND ----------

testSqlConnection()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import row_number, lit
from pyspark.sql.window import Window

w = Window().orderBy(lit("A"))

df_final = (
    spare_parts_df_new.withColumn("InsertDateTime", current_timestamp())
    .withColumn("UpdateDateTime", current_timestamp())
    .withColumn("CreatedBy", lit("DatabricksUser"))
    .withColumn("ROW_ID", row_number().over(w))
)
df_final = df_final.select(
    "ROW_ID",
    "SYS_MAT_ID",
    "SYS_SERIAL_ID",
    "EQ_MAT_ID",
    "EQ_MAT_TEXT",
    "SP_QUANTITY_CONSUMED",
    "DOC_DATE",
    "NOTI_ID",
    "BAS_LOAD_DT",
    "InsertDateTime",
    "UpdateDateTime",
)

df_final.cache()
total_rows_cnt = df_final.count()

logger.info("Total Number of Rows to be written : " + str(total_rows_cnt))

# COMMAND ----------

# MAGIC %md
# MAGIC ######Writing Info to SQL storage

# COMMAND ----------

try:
    write_sql(df_final, "dga_cspr_" + env + ".SDTB_SPR2_SPARE_PRTS_CONSUM")
except Exception as e:
    logger.error(e)
else:
    logger.info("The cSPR Spare Parts Info is refreshed")

# COMMAND ----------

df_final.unpersist()

# COMMAND ----------

dbutils.fs.mv(
    "file:" + finalpath,
    "dbfs:/mnt/logging/guardian_v2" + "_" + env + "/" + partitionpath + logfilename,
)

# COMMAND ----------

spark.sql(
    """create table if not exists guardian_logging_v2_{env} \
using text \
options (path '{PATH}',header=true)""".format(
        PATH="""/mnt/logging/guardian_v2_""" + env + """/*/*/*/""", env=env
    )
)

# COMMAND ----------

spark.sql("refresh table guardian_logging_v2_{env}".format(env=env))

# COMMAND ----------

from pyspark.sql.functions import split

info_df = spark.table("guardian_logging_v2_{env}".format(env=env))
info_split_df = (
    info_df.withColumn("datetime", split(info_df["value"], "-").getItem(0))
    .withColumn("job", split(info_df["value"], "-").getItem(1))
    .withColumn("status", split(info_df["value"], "-").getItem(2))
    .drop("value")
)

table_name = "dga_guardian_" + env + ".logging_info_v2_t"

write_sql(info_split_df, table_name)
# display(info_split_df)

# COMMAND ----------

import pymsteams


def sendMessageToTeams(webHookUrl: str, msg: str):
    msg = msg.replace("_", "\_")
    teams_msg = pymsteams.connectorcard(webHookUrl)
    teams_msg.text(f"{msg}")
    teams_msg.send()


# COMMAND ----------

sendMessageToTeams(
    "https://healthineers.webhook.office.com/webhookb2/f7ea226b-dcc6-49d3-8dee-886187541fac@5dbf1add-202a-4b8d-815b-bf0fb024e033/IncomingWebhook/db88360a82d348c5ba2824dedbaf27eb/de5f7111-94c9-4ffc-b16c-342b8b8ea7d0",
    "All the latest Spare Parts info are available in azure sql now for"
    + " "
    + env
    + "",
)
