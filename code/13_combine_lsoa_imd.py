# Databricks notebook source
# MAGIC %md
# MAGIC # 10_combine_lsoa_imd
# MAGIC
# MAGIC **Description** This notebook uses combines LSOA/IMD data to the incident heart failure subclasses data.
# MAGIC
# MAGIC **Authors** Robert Fletcher

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

# DBTITLE 1,Load libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
import re
import sys
import datetime

from pyspark.sql.window import Window as w
from functools import reduce

_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow: {_datetimenow}")

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
hes_apc_cln     = 'ccu005_04_hes_apc_clean'
gdppr_cln       = 'ccu005_04_gdppr_select'
hes_nhfa        = 'ccu005_04_hes_nhfa_linked'
subclass_skinny = 'ccu005_04_incident_hf_subclass_skinny'
lsoa_imd        = 'ccu005_04_cur_lsoa_imd_lookup'

# Pipeline version
ver = 'v2'

# COMMAND ----------

hes      = load(db_name=new_db, table_name=f'{hes_apc_cln}_{ver}', db_type='ccu005_04')
gdppr    = load(db_name=new_db, table_name=f'{gdppr_cln}_{ver}', db_type='ccu005_04')
combined = load(db_name=new_db, table_name=f'{hes_nhfa}_{ver}', db_type='ccu005_04')
sub      = load(db_name=new_db, table_name=f'{subclass_skinny}_{ver}', db_type='ccu005_04')
imd      = load(db_name=new_db, table_name=f'{lsoa_imd}_{ver}', db_type='ccu005_04')

# COMMAND ----------

# Rename IMD cols
imd2 = imd.withColumnRenamed('LSOA', 'lsoa_residence').withColumnRenamed('IMD_2019_DECILES', 'imd_2019_deciles').withColumnRenamed('IMD_2019_QUINTILES', 'imd_2019_quintiles')

# COMMAND ----------

# Join LSOA codes to heart failure hospitalisations
window = w.partitionBy('subject_id', 'spell_id').orderBy(*[f.asc_nulls_last(c) for c in ['episode_order']])
hes_lsoa = hes\
    .select('subject_id', 'spell_id', 'admission_date', 'episode_order', 'lsoa_residence')\
    .withColumn('row', f.row_number().over(window))\
    .where(f.col('row') == 1)\
    .drop('row')\
    .distinct()

# Join patients in heart failure subclasses data to HES with LSOA data
spell_lsoa = sub.select('subject_id', 'spell_id').join(hes_lsoa, on=['subject_id', 'spell_id'], how='left').distinct()

# Join to IMD look-up table
spell_imd = spell_lsoa.join(imd2, on='lsoa_residence', how='left').distinct().withColumn('imd_source', f.lit('same spell'))

# COMMAND ----------

# DBTITLE 1,Get missing IMD from other hospital spells
# Patients with missing IMD
na = spell_imd.where(f.col('imd_2019_quintiles').isNull())

# Patients with more than one IMD
dup = spell_imd.groupBy('subject_id').count().where(f.col('count') > 1).select('subject_id').distinct()
dup2 = spell_imd.join(dup, 'subject_id', how='inner').where(f.col('imd_2019_quintiles').isNotNull())

# Join
dfs = [na, dup2]
no_imd = reduce(f.DataFrame.unionByName, dfs).distinct().drop('imd_source')

# 
window = w.partitionBy('subject_id').orderBy(*[f.asc_nulls_last('diff')])
spell_imd_oth = hes\
   .select('subject_id', 'spell_id', 'admission_date', 'episode_order', 'lsoa_residence')\
   .where(f.col('episode_order') == 1)\
   .join(imd2, on='lsoa_residence', how='left')\
   .withColumnRenamed('spell_id', 'spell_id2')\
   .withColumnRenamed('admission_date', 'admission_date2')\
   .withColumnRenamed('episode_order', 'episode_order2')\
   .withColumnRenamed('lsoa_residence', 'lsoa_residence2')\
   .withColumnRenamed('imd_2019_deciles', 'imd_2019_deciles2')\
   .withColumnRenamed('imd_2019_quintiles', 'imd_2019_quintiles2')\
   .distinct()\
   .join(no_imd, on='subject_id', how='inner')\
   .where(f.col('imd_2019_quintiles2').isNotNull())\
   .withColumn('diff', f.abs(f.datediff('admission_date', 'admission_date2')))\
   .where((f.col('diff') <= 365.25) & (f.col('diff') != 0))\
   .withColumn('row', f.row_number().over(window))\
   .where(f.col('row') == 1)\
   .select(
       'subject_id', 'spell_id', 'admission_date', 'episode_order', f.col('lsoa_residence2').alias('lsoa_residence'), f.col('imd_2019_deciles2').alias('imd_2019_deciles'), f.col('imd_2019_quintiles2').alias('imd_2019_quintiles')
    )\
    .withColumn('imd_source', f.lit('adjacent spell'))

# COMMAND ----------

# DBTITLE 1,Get missing IMD from other hospital spells
pat = spell_imd_oth.select('subject_id').distinct()
no_imd2 = no_imd.join(pat, on='subject_id', how='left_anti')

gdppr_imd = gdppr\
   .select('subject_id', 'lsoa', 'date')\
   .join(imd2.withColumnRenamed('lsoa_residence', 'lsoa'), on='lsoa', how='left')\
   .distinct()\
   .withColumnRenamed('imd_2019_deciles', 'imd_2019_deciles2')\
   .withColumnRenamed('imd_2019_quintiles', 'imd_2019_quintiles2')\
   .join(no_imd2, on='subject_id', how='inner')\
   .where(f.col('imd_2019_quintiles2').isNotNull())\
   .withColumn('diff', f.abs(f.datediff('admission_date', 'date')))\
   .where(f.col('diff') <= 365.25)\
   .withColumn('row', f.row_number().over(window))\
   .where(f.col('row') == 1)\
   .select(
       'subject_id', 'spell_id', 'admission_date', 'episode_order', f.col('lsoa').alias('lsoa_residence'), 
       f.col('imd_2019_deciles2').alias('imd_2019_deciles'), f.col('imd_2019_quintiles2').alias('imd_2019_quintiles')
    )\
   .withColumn('imd_source', f.lit('gdppr'))

# COMMAND ----------

# DBTITLE 1,Combine all data
# Remove patients from "same spell IMD" previously identified
spell_imd2 = spell_imd.join(no_imd.select('subject_id').distinct(), on='subject_id', how='left_anti')

dfs = [spell_imd2, spell_imd_oth, gdppr_imd]
cohort_imd = reduce(f.DataFrame.unionByName, dfs).distinct()

# COMMAND ----------

pats2 = cohort_imd.groupBy('subject_id').count().where(f.col('count') > 1).select('subject_id').distinct()
dups = cohort_imd.join(pats2, 'subject_id', how='inner')
display(dups)

# COMMAND ----------

cohort_imd2 = cohort_imd.drop('imd_source').distinct()

# COMMAND ----------

provide_counts(cohort_imd2, provide_hospitals=False, provide_subtype=False, addition_details='') 

# COMMAND ----------

cohort_imd3 = (
    cohort_imd2
    .select('subject_id', 'spell_id', 'lsoa_residence', 'imd_2019_deciles', 'imd_2019_quintiles')
    .distinct()
)
sub_imd = sub.join(cohort_imd3, on=['subject_id', 'spell_id'], how='left')

# COMMAND ----------

# check uniqueness
print(cohort_imd3.select("subject_id").count())
print(cohort_imd3.select("subject_id").distinct().count())

# COMMAND ----------

sub_imd.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.ccu005_04_incident_hf_subclass_skinny_imd_{ver}')

# COMMAND ----------

# DBTITLE 1, Check
check = load(db_name=new_db, table_name=f'ccu005_04_incident_hf_subclass_skinny_imd_{ver}', db_type='ccu005_04')
provide_counts(check, provide_hospitals=False, provide_subtype=True, addition_details='') 