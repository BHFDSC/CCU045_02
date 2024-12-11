# Databricks notebook source
# MAGIC %md
# MAGIC # 01_prepare_skinny
# MAGIC
# MAGIC **Description** This notebook prepares the skinny patient table, which includes key patient characteristics.
# MAGIC
# MAGIC **Authors** Robert Fletcher

# COMMAND ----------

# DBTITLE 1,Load libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window
import re

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
hes_apc = 'hes_apc_all_years'
hes_op = 'hes_op_all_years'
hes_ae = 'hes_ae_all_years'

# Most recent archive dates
date = '2023-06-21'

# Writable table name
write_tbl = 'ccu005_04_skinny_selected'

# Pipeline version
ver = 'v2'

# COMMAND ----------

# DBTITLE 1,Load data
gdppr_raw = load(db_name=archive, table_name=gdppr, db_type='archive', archive_date=date)
hes_apc_raw = load(db_name=archive, table_name=hes_apc, db_type='archive', archive_date=date)
hes_op_raw = load(db_name=archive, table_name=hes_op, db_type='archive', archive_date=date)
hes_ae_raw = load(db_name=archive, table_name=hes_ae, db_type='archive', archive_date=date)

# COMMAND ----------

# DBTITLE 1,Harmonise data
kpc_harmonised = key_patient_characteristics_harmonise(gdppr=gdppr_raw, hes_apc=hes_apc_raw, hes_ae=hes_ae_raw, hes_op=hes_op_raw)

# COMMAND ----------

# DBTITLE 1,Select relevant data
kpc_selected = key_patient_characteristics_select(harmonised=kpc_harmonised)

# COMMAND ----------

# DBTITLE 1,Write data
kpc_selected.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.{write_tbl}_{ver}')