# Databricks notebook source
# MAGIC %md
# MAGIC # 08_define_hf_admissions
# MAGIC
# MAGIC **Description** This notebook prepares a reduced HES APC dataset which includes hospitalisations for heart failure to map to the National Heart Failure Audit (NHFA), and a dataset which indicates whether a patient has a historic record of heart failure.
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
hes_apc_raw = 'hes_apc_all_years'
hes_apc_cln = 'ccu005_04_hes_apc_clean'

# Most recent archive dates
date = '2023-06-21'

# Writable table name
write_tbl_1 = 'ccu005_04_hf_admissions_simple_2010_onwards'
write_tbl_2 = 'ccu005_04_first_hes_apc_hf_record'

# Pipeline version
ver = 'v2'

# Heart failure ICD-10 regex
hf_icd10_regex = '^I110|^I13[02]|^I255|^I42[09]|^I50[019]'

# Reduced heart failure ICD-10 regex (excluding I13 codes)
reduced_icd10_regex = '^I110|^I255|^I42[09]|^I50[019]'

# COMMAND ----------

# DBTITLE 1,Load data
hes_raw = load(db_name=archive, table_name=hes_apc_raw, db_type='archive', archive_date=date)
hes_cln = load(db_name=new_db, table_name=f'{hes_apc_cln}_{ver}', db_type='ccu005_04')

# COMMAND ----------

# DBTITLE 1,Simplify heart failure hospitalisations
# All heart failure admissions
hf_admissions = (
    hes_cln
    .filter(f.col('diagnosis_code').rlike(reduced_icd10_regex))
)

# Define first instance of heart failure code
w2 = w.partitionBy('subject_id').orderBy('episode_start')
first_hf = (
    hf_admissions
    .withColumn('row', f.row_number().over(w2))
    .filter(f.col('row') == 1)
    .drop("row")
)

# COMMAND ----------

# DBTITLE 1,Define first HF hospitalisations (2010 onwards)
simple_hf = (
    hes_cln 
    .filter(
       (f.col('diagnosis_code').rlike(reduced_icd10_regex)) & 
       (f.col('admission_date') >= '2010-01-01')
     )
    .withColumn(
        'hf_diagnosis_type',
        f.when(f.col('diagnosis_position') == 1, "primary")
         .when(f.col('diagnosis_position') >= 2, "secondary")
         .otherwise(None)
    )
    .select(
        'subject_id', 'spell_id', 'spell_id_otr', 'admission_method', 'admission_date', 
        'discharge_date', 'hf_diagnosis_type', 'healthcare_provider_code',
        'healthcare_provider_name', 'hospital_site_code', 'hospital_site_name', 
        'standardised_name', 'town', 'district', 'constituency', 'county', 
        'country', 'postcode', 'legal_start_date', 'legal_end_date',
        'legal_status', 'managed_by_code', 'managed_by_name',
        'hospital_submits_to_nhfa'
    )
    .distinct()
    .withColumn(
        'discharge_date',
        f.when(f.col('discharge_date') == "1800-01-01", None)
         .otherwise(f.col('discharge_date'))
    )
    .withColumn(
        'admission_length',
        f.when(f.col('discharge_date').isNull(), 0)
        .otherwise(f.datediff(f.col('discharge_date'), f.col('admission_date')) + 1)
    )
)

w3 = w.partitionBy('subject_id', 'spell_id')
simple_hf2 = simple_hf.withColumn('max', f.max('admission_length').over(w3))\
    .where(f.col('admission_length') == f.col('max'))\
    .drop('max')

# COMMAND ----------

# DBTITLE 1,Check
display(simple_hf2)

# COMMAND ----------

# DBTITLE 1,Indentify duplicates
# Identify 
dupes = (
    simple_hf2
    .select('subject_id', 'spell_id', 'hf_diagnosis_type')
    .distinct()
    .groupby(['subject_id', 'spell_id'])
    .count()
    .where(f.col('count') > 1)
    .drop('count')
    .join(simple_hf2, ['subject_id', 'spell_id'], how='inner')
    .distinct()
)

# Duplicates to remove (where one is `diagnosis_type` == 'primary' and one is `diagnosis_type` == 'secondary') 
dupes_remove = (
    dupes
    .select('subject_id', 'spell_id')
    .distinct()
)

# Rows to add back once duplicates are removed with `diagnosis_type` == 'both'
dupes_rejoin = (
    dupes
    .withColumn('hf_diagnosis_type', f.lit('both'))
    .distinct()
)

# COMMAND ----------

display(dupes)

# COMMAND ----------

# DBTITLE 1,Remove duplicates and add back rows
simple_hf3 = simple_hf2.join(dupes_remove, ['subject_id', 'spell_id'], how='left_anti')
simple_hf4 = simple_hf3.union(dupes_rejoin)

# COMMAND ----------

# DBTITLE 1,Quality control
# Check rows with primary diagnosis actually do have a primary HF diagnosis
hes_hf = hes_cln.where((f.col('diagnosis_code').rlike('^I110|^I255|^I42[09]|^I50[019]')) & (f.col('diagnosis_position') == 1)).select('subject_id', 'spell_id', 'diagnosis_code', 'diagnosis_position')
qc = (
    simple_hf4
    .join(hes_hf, on=['subject_id', 'spell_id'], how='left')
    .where(f.col('hf_diagnosis_type').rlike('both|pri'))
    .select('subject_id', 'spell_id', 'hf_diagnosis_type', 'hospital_site_code', 'diagnosis_code', 'diagnosis_position')
)
print('# Check qc'); print()
print(qc.limit(10).toPandas().to_string()); print()

# Check for missing diags
qc2 = qc.where(f.col('diagnosis_code').isNull())
print('# Check qc2'); print()
print(qc2.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# DBTITLE 1,Write data
simple_hf4.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.{write_tbl_1}_{ver}')
first_hf.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.{write_tbl_2}_{ver}')