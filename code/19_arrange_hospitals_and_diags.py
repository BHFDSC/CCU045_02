# Databricks notebook source
# MAGIC %md
# MAGIC # 19_arrange_hospitals_and_diags
# MAGIC
# MAGIC **Description** This notebook puts NHS treatment site codes and heart failure ICD-10 codes into wide format.
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
subclass  = 'ccu005_04_incident_hf_subclass_skinny_imd_char_comorb'
hes_apc   = 'ccu005_04_hes_apc_clean'
linked    = 'ccu005_04_hes_nhfa_linked' # Linkage of NHFA and HES from much earlier in the pipeline

# Pipeline version
ver = 'v2'

# COMMAND ----------

# DBTITLE 1,Load data
cohort = load(db_name=new_db, table_name=f'{subclass}_{ver}', db_type='ccu005_04')
hes    = load(db_name=new_db, table_name=f'{hes_apc}_{ver}', db_type='ccu005_04')

# For QC
qc = load(db_name=new_db, table_name=f'{linked}_{ver}', db_type='ccu005_04')

# COMMAND ----------

# check uniqueness
print(cohort.select("subject_id").count())
print(cohort.select("subject_id").distinct().count())

# COMMAND ----------

# DBTITLE 1,Join
joined = (
    cohort
    .select('subject_id', 'spell_id')
    .distinct()
    .join(hes, on=['subject_id', 'spell_id'], how='inner')
    .where((f.col('diagnosis_code').rlike('^I110|^I255|^I42[09]|^I50[019]')) & (f.col('diagnosis_position') == 1))
    .select(
        'subject_id', 'spell_id', 'episode_id', 'spell_begin', 'spell_end', 'episode_start', 'episode_order', 
        'diagnosis_code', 'discharge_destination', 'discharge_method', 'healthcare_provider_code', 'hospital_site_code'
    )
)
display(joined)

# COMMAND ----------

# prepare
# add row numbers for diag_code, treatment_site, and healthcare provider
win = w.partitionBy('subject_id', 'spell_id').orderBy('episode_order')
win_site = w.partitionBy('subject_id', 'spell_id', 'hospital_site_code').orderBy('episode_order')
win_prov = w.partitionBy('subject_id', 'spell_id', 'healthcare_provider_code').orderBy('episode_order')
tmp1 = (
  joined  
  .withColumn('rownum_diag_code', f.row_number().over(win))
  .withColumn('rownum_site', f.row_number().over(win_site))
  .withColumn('rownum_prov', f.row_number().over(win_prov))
  .orderBy('subject_id', 'spell_id', 'episode_order')
)
print('# Check tmp1'); print()
print(tmp1.limit(10).toPandas().to_string()); print()

# reshape diag_code 
tmp2 = (
  tmp1
  .withColumn('col_name', f.concat(f.lit('diag_code_'), f.col('rownum_diag_code')))
  .select('subject_id', 'spell_id', f.col('diagnosis_code').alias('diag_code'), 'col_name')  
  .groupBy('subject_id', 'spell_id')
  .pivot('col_name')
  .agg(f.first('diag_code'))
)
print('# Check tmp2'); print()
print(tmp2.limit(10).toPandas().to_string()); print()

# reshape site
tmp3 = (
  tmp1
  .where(f.col('rownum_site') == 1) # limit to the row for each site per id and spell_id to avoid repeated site when reshaping
  .select('subject_id', 'spell_id', 'episode_order', 'hospital_site_code')  
  .withColumn('rownum_site', f.row_number().over(win))
  .withColumn('col_name', f.concat(f.lit('site_'), f.col('rownum_site')))
  .groupBy('subject_id', 'spell_id')
  .pivot('col_name')
  .agg(f.first('hospital_site_code'))
)
print('# Check tmp3'); print()
print(tmp3.limit(10).toPandas().to_string()); print()

# reshape provider
tmp4 = (
  tmp1
  .where(f.col('rownum_prov') == 1) # limit to the row for each site per id and spell_id to avoid repeated site when reshaping
  .select('subject_id', 'spell_id', 'episode_order', 'healthcare_provider_code')  
  .withColumn('rownum_prov', f.row_number().over(win))
  .withColumn('col_name', f.concat(f.lit('provider_'), f.col('rownum_prov')))
  .groupBy('subject_id', 'spell_id')
  .pivot('col_name')
  .agg(f.first('healthcare_provider_code'))
)
print('# Check tmp4'); print()
print(tmp4.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# join reshaped diag_code, site, and provider
tmp5 = (
  tmp2
  .join(tmp3, on=['subject_id', 'spell_id'], how='outer')
  .join(tmp4, on=['subject_id', 'spell_id'], how='outer')
)
print('# Check tmp5'); print()
print(tmp5.limit(10).toPandas().to_string()); print()

# COMMAND ----------

final = cohort.join(tmp5, on=['subject_id', 'spell_id'], how='left')

# Add discharge method for in-hospital death
dm = (
    hes
    .select('subject_id', 'spell_id', 'spell_end', 'discharge_method')
    .where((f.col('spell_end') == 'Y') & (f.col('discharge_method') == '4'))
    .distinct()
)
final2 = final.join(dm, on=['subject_id', 'spell_id'], how='left')

display(final2)

# COMMAND ----------

# check uniqueness
print(final2.select("subject_id").count())
print(final2.select("subject_id").distinct().count())

# COMMAND ----------

# DBTITLE 1,Write data
final2.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.ccu005_04_baseline_{ver}')