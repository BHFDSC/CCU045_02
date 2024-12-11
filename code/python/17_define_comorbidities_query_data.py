# Databricks notebook source
# MAGIC %md
# MAGIC # 17_define_comorbidities_query_data
# MAGIC
# MAGIC **Description** This notebook prepares long format data from GDPPR, HES-APC, and HES-OP for the patient cohort that can be used to define history of comorbidities.
# MAGIC
# MAGIC **Authors** Robert Fletcher

# COMMAND ----------

# DBTITLE 1,Load libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
import re

from pyspark.sql.window import Window as w
from functools import reduce

# COMMAND ----------

# DBTITLE 1,Source functions
# MAGIC %run "/Repos/raf69@cam.ac.uk/ccu005_04/nicor/src/utils"

# COMMAND ----------

# DBTITLE 1,Define variables
# Databases
live    = 'dars_nic_391419_j3w9t'
archive = 'dars_nic_391419_j3w9t_collab'
new_db  = 'dsa_391419_j3w9t_collab'

# Tables
subclass    = 'ccu005_04_incident_hf_subclass_skinny_imd_char'
hes_apc_cln = 'ccu005_04_hes_apc_clean'
hes_op_cln  = 'ccu005_04_hes_op_clean'
gdppr_cln   = 'ccu005_04_gdppr_select'

# Pipeline version
ver = 'v2'

# COMMAND ----------

# DBTITLE 1,Load data
cohort  = load(db_name=new_db, table_name=f'{subclass}_{ver}', db_type='ccu005_04')
hes_apc = load(db_name=new_db, table_name=f'{hes_apc_cln}_{ver}', db_type='ccu005_04')
hes_op  = load(db_name=new_db, table_name=f'{hes_op_cln}_{ver}', db_type='ccu005_04')
gdppr   = load(db_name=new_db, table_name=f'{gdppr_cln}_{ver}', db_type='ccu005_04')

# COMMAND ----------

# DBTITLE 1,Select columns
# Admission dates and subject identifiers for the whole cohort
cohort_select = cohort.select('subject_id', 'admission_date').distinct()

# Select required columns from HES-APC
hes_apc_select = hes_apc.select('subject_id', f.col('admission_date').alias('date'), 'diagnosis_code', 'diagnosis_position').distinct().withColumn('source', f.lit('HES-APC'))

# Select required columns from HES-OP
hes_op_select = hes_op.select('subject_id', f.col('appointment_date').alias('date'), 'diagnosis_code', 'diagnosis_position').distinct().withColumn('source', f.lit('HES-OP'))

# Select required_columns from GDPPR
gdppr_select = gdppr.select('subject_id', 'date', f.col('snomed_code').alias('diagnosis_code')).distinct().withColumn('diagnosis_position', f.lit('1')).withColumn('source', f.lit('GDPPR'))

# COMMAND ----------

# DBTITLE 1,Restrict to those in the cohort
# HES-APC
hes_apc_select2 = cohort_select.join(hes_apc_select, on='subject_id', how='left')

# HES-OP
hes_op_select2 = cohort_select.join(hes_op_select, on='subject_id', how='left')

# GDPPR
gdppr_select2 = cohort_select.join(gdppr_select, on='subject_id', how='left')

# COMMAND ----------

# DBTITLE 1,Combine all
dfs = [hes_apc_select2, hes_op_select2, gdppr_select2]
comorb = reduce(f.DataFrame.unionByName, dfs).where(f.col('date') <= f.col('admission_date'))

# COMMAND ----------

# DBTITLE 1,Check
display(comorb)

# COMMAND ----------

# DBTITLE 1,Write data
comorb.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.ccu005_04_comorbidities_query_data_{ver}')