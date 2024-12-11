# Databricks notebook source
# MAGIC %md
# MAGIC # 04_prepare_gdppr
# MAGIC
# MAGIC **Description** This notebook prepares a cleaned General Practice Extraction Service (GPES) Data for pandemic planning and research (GDPPR) dataset.
# MAGIC
# MAGIC **Authors** Robert Fletcher

# COMMAND ----------

# DBTITLE 1,Load libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
import re
import sys

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window as w

# COMMAND ----------

# DBTITLE 1,Source functions
# MAGIC %run "/Repos/raf69@cam.ac.uk/ccu005_04/nicor/src/utils"

# COMMAND ----------

# DBTITLE 1,Define variables
# Databases
live = 'dars_nic_391419_j3w9t'
archive = 'dars_nic_391419_j3w9t_collab'
new_db = 'dsa_391419_j3w9t_collab'

# Tables
gdppr = 'gdppr_dars_nic_391419_j3w9t'

# Most recent archive dates
date = '2023-06-21'

# Writable table name
write_tbl = 'ccu005_04_gdppr_select'

# Pipeline version
ver = 'v2'

# COMMAND ----------

# DBTITLE 1,Load data
gdppr_raw = load(db_name=archive, table_name=gdppr, db_type='archive', archive_date=date)

# COMMAND ----------

# DBTITLE 1,Display raw data
display(gdppr_raw)

# COMMAND ----------

# DBTITLE 1,Select and rename columns
# Select and rename variable in HES APC
gdppr_select = (
    gdppr_raw
    .select(
        f.col('NHS_NUMBER_DEID').alias('subject_id'), 
        f.col('REPORTING_PERIOD_END_DATE').alias('reporting_period_end_date'), 
        f.col('LSOA').alias('lsoa'),
        f.col('PRACTICE').alias('practice'),
        f.col('DATE').alias('date'), 
        f.col('RECORD_DATE').alias('record_date'), 
        f.col('CODE').alias('snomed_code'), 
        f.col('EPISODE_CONDITION').alias('episode_condition'), 
        f.col('EPISODE_PRESCRIPTION').alias('episode_prescription'), 
        f.col('VALUE1_CONDITION').alias('value1_condition'), 
        f.col('VALUE2_CONDITION').alias('value2_condition'), 
        f.col('VALUE1_PRESCRIPTION').alias('value1_prescription'),
        f.col('VALUE2_PRESCRIPTION').alias('value2_prescription'), 
        f.col('LINKS').alias('links')
    )
)

# COMMAND ----------

# DBTITLE 1,Write data
gdppr_select.write.mode('overwrite').saveAsTable(f'{new_db}.{write_tbl}_{ver}')