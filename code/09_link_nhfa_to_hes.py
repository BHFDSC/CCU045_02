# Databricks notebook source
# MAGIC %md
# MAGIC # 09_link_nhfa_to_hes
# MAGIC
# MAGIC **Description** This notebook links the National Heart Failure Audit to heart failure hospitalisations defined in HES APC.
# MAGIC
# MAGIC **Authors** Robert Fletcher

# COMMAND ----------

# DBTITLE 1,Load libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
import re
import sys

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
nhfa_c = 'ccu005_04_nhfa_clean'
hf_adm = 'ccu005_04_hf_admissions_simple_2010_onwards'
hes_apc_cln = 'ccu005_04_hes_apc_clean'

# Writable table names
write_tbl = 'ccu005_04_hes_nhfa_linked'

# Pipeline version
ver = 'v2'

# COMMAND ----------

# DBTITLE 1,Load data
nhfa_comb = load(db_name=new_db, table_name=f'{nhfa_c}_{ver}', db_type='ccu005_04')
hf_admiss = load(db_name=new_db, table_name=f'{hf_adm}_{ver}', db_type='ccu005_04')
hes_cln = load(db_name=new_db, table_name=f'{hes_apc_cln}_{ver}', db_type='ccu005_04')

# COMMAND ----------

# DBTITLE 1,Restrict datasets
# Assign a unique identifier to each admission in the NHFA
window_spec = w.partitionBy("subject_id").orderBy("admission_date")
nhfa_admis = (
    nhfa_comb
    .select('subject_id', 'admission_date', 'nicor_id')
    .distinct()
)

# Restrict HES HF admissions to desired columns
hf_adm_select = (
    hf_admiss
    .select(
        'subject_id', 'spell_id', 'admission_method', 'admission_date', 'discharge_date',
        'hf_diagnosis_type', 'healthcare_provider_code',
        'healthcare_provider_name', 'hospital_site_code', 'hospital_site_name', 'standardised_name',
        'town', 'district', 'constituency', 'county', 'country', 'postcode', 'legal_start_date',
        'legal_end_date', 'legal_status', 'managed_by_code', 'managed_by_name',
        'hospital_submits_to_nhfa', 'admission_length'
    )
)

# Restrict HES (clean) to desired columns
hes_select = (
    hes_cln
    .select('subject_id', 'spell_id', 'episode_id', 'episode_start')
    .distinct()
)

# COMMAND ----------

# DBTITLE 1,Linkage by subject identifier and admission date
# Link
linkage_1 = hf_adm_select.join(nhfa_admis, ['subject_id', 'admission_date'], how='left')

# Get rows where 1:1 linkage was achieved
linkage_1_date = linkage_1.filter(f.col('nicor_id').isNotNull()).sort('subject_id', 'spell_id')

# COMMAND ----------

# DBTITLE 1,Linkage by subject identifier alone (+/- 30 days)
# Rename `admission_date` column in NHFA admissions data (to not duplicate column names)
nhfa_admis2 = (
    nhfa_admis
    .select(
        'subject_id', 
        f.col('admission_date').alias('admission_date_nhfa'), 
        'nicor_id'
    )
    # Remove NHFA admission identifiers that have already been linked by date
    .join(linkage_1_date.select('subject_id', 'nicor_id').distinct(), ['subject_id', 'nicor_id'], how='leftanti')
)

# Link by subject identifier alone within a +/- 30 day window
linkage_2_subj = (
    linkage_1
    .filter(f.col('nicor_id').isNull())
    .drop('nicor_id')
    .join(nhfa_admis2, ['subject_id'], how='left')
    .filter(f.col('nicor_id').isNotNull())
    .withColumn('diff', f.abs(f.datediff(f.col('admission_date'), f.col('admission_date_nhfa'))))
    .filter((f.col('diff') <= 30))
)

# Select records which are the closest in proximity to each other
w2 = w.partitionBy('subject_id', 'spell_id')
linkage_2_subj2 = linkage_2_subj \
    .withColumn('min', f.min('diff').over(w2)) \
    .where(f.col('diff') == f.col('min')) \
    .drop('min')

# COMMAND ----------

# DBTITLE 1,Linkage by subject identifier alone (+/- 6 months)
# Aggregate all records currently linked ()
date_rdc = linkage_1_date.select('subject_id', 'spell_id', 'nicor_id').distinct().withColumn('linkage_type', f.lit('date matched'))
subj_rdc = linkage_2_subj2.select('subject_id', 'spell_id', 'nicor_id').distinct().withColumn('linkage_type', f.lit('1 month matched'))
linked_rdc = date_rdc.union(subj_rdc)

# Remove NHFA admission identifiers that have already been linked by date or +/- 30 day window
nhfa_admis3 = nhfa_admis2.join(linked_rdc.select('subject_id', 'nicor_id').distinct(), ['subject_id', 'nicor_id'], how='leftanti')

# Link by subject identifier alone within a +/- 6 month window
linkage_3_6mo = (
    hf_adm_select
    .join(linked_rdc, ['subject_id', 'spell_id'], how='leftanti')
    .join(nhfa_admis3, ['subject_id'], how='left')
    .filter(f.col('nicor_id').isNotNull())
    .withColumn('diff', f.abs(f.datediff(f.col('admission_date'), f.col('admission_date_nhfa'))))
    .filter((f.col('diff') <= 183))
)

# Select records which are the closest in proximity to each other
linkage_3_6mo2 = linkage_3_6mo.withColumn('min', f.min('diff').over(w2)) \
    .where(f.col('diff') == f.col('min')) \
    .drop('min')

# Records linked within a window of 6 months
mo6_rdc = linkage_3_6mo2.select('subject_id', 'spell_id', 'nicor_id').distinct().withColumn('linkage_type', f.lit('6 month matched'))
linked_rdc2 = mo6_rdc.union(linked_rdc).distinct()

# Select relevant columns
comb = (
    hf_adm_select
    .join(linked_rdc2, ['subject_id', 'spell_id'], how='left')
    .join(nhfa_comb.withColumnRenamed('admission_date', 'admission_date_nhfa') , ['subject_id', 'nicor_id'], how='left')
    .filter(f.col('admission_date') >= '2019-01-01')
    .distinct()
) 
display(comb)

# COMMAND ----------

# DBTITLE 1,Check
adm_min = comb.agg({'admission_date': 'min'}).collect()[0][0]
adm_max = comb.agg({'admission_date': 'max'}).collect()[0][0]
print(f'Admission date range: {adm_min} to {adm_max}')

# COMMAND ----------

# DBTITLE 1,Write data
comb.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.{write_tbl}_{ver}')