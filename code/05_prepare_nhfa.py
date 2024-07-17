# Databricks notebook source
# MAGIC %md
# MAGIC # 05_prepare_nhfa
# MAGIC
# MAGIC **Description** This notebook prepares a cleaned dataset for the combined National Heart Failure Audit (v4.2.1 & v5.0).
# MAGIC
# MAGIC **Authors** Robert Fletcher

# COMMAND ----------

# DBTITLE 1,Load libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
import re

from pyspark.sql.window import Window as w

# COMMAND ----------

# DBTITLE 1,Source functions
# MAGIC %run "/Repos/raf69@cam.ac.uk/ccu005_04/nicor/src/utils"

# COMMAND ----------

# DBTITLE 1,Source function to phenotype heart failure
# MAGIC %run "/Repos/raf69@cam.ac.uk/ccu005_04/nicor/src/define_hf_subtypes_v2"

# COMMAND ----------

# DBTITLE 1,Define variables
# Databases
live = 'dars_nic_391419_j3w9t'
archive = 'dars_nic_391419_j3w9t_collab'
new_db = 'dsa_391419_j3w9t_collab'

# Tables
nhfa_v4 = 'nicor_hf_dars_nic_391419_j3w9t'
nhfa_v5 = 'nicor_hf_v5_dars_nic_391419_j3w9t'

# Most recent archive date
date = '2023-06-21'

# Writable table name
write_tbl = 'ccu005_04_nhfa_clean'

# Pipeline version
ver = 'v2'

# COMMAND ----------

dates = spark.table('dars_nic_391419_j3w9t_collab.nicor_hf_v5_dars_nic_391419_j3w9t_archive').select(f.col('archived_on')).distinct().sort(f.col('archived_on').desc())
dates.show()

# COMMAND ----------

# DBTITLE 1,Load data
nhfa_v4_raw = load(db_name=archive, table_name=nhfa_v4, db_type='archive', archive_date=date)
# nhfa_v5_raw = spark.table('dsa_391419_j3w9t_collab.nicor_hf_v5_dars_nic_391419_j3w9t_archive')
nhfa_v5_raw = load(db_name=archive, table_name=nhfa_v5, db_type='archive', archive_date=date)

# COMMAND ----------

display(nhfa_v5_raw.where((f.col('11_47_SGLT2_INHIBITORS_DISCHARGE').isNotNull()) | (f.col('7_47_SGLT2_INHIBITORS_ADMISSION').isNotNull())).select('PERSON_ID_DEID', '2_00_DATE_OF_ADMISSION','7_47_SGLT2_INHIBITORS_ADMISSION', '11_47_SGLT2_INHIBITORS_DISCHARGE').where(f.col('2_00_DATE_OF_ADMISSION') < '2021-08-01').where((f.col('7_47_SGLT2_INHIBITORS_ADMISSION').rlike('flozin')) | (f.col('11_47_SGLT2_INHIBITORS_DISCHARGE').rlike('flozin'))))

# COMMAND ----------

# DBTITLE 1,Select and rename columns
# v4
nhfa_v4 = (
  select_nhfa_columns(nhfa_v4_raw, version='4.2.1')
  .withColumn("version", f.lit("v4"))
)

# v5
nhfa_v5 = (
  select_nhfa_columns(nhfa_v5_raw, version='5.0')
  .withColumn("version", f.lit("v5"))
)

# COMMAND ----------

# DBTITLE 1,Join v4.2.1 and v5.0
# Add missing columns to v4
for column in [column for column in nhfa_v5.columns if column not in nhfa_v4.columns]:
    nhfa_v4 = nhfa_v4.withColumn(column, f.lit(None))
    
# Combine data
nhfa = nhfa_v4.unionByName(nhfa_v5)

# COMMAND ----------

# DBTITLE 1,Remove rows with missing IDs and admission dates
# Remove rows with missing `subject_id`
nhfa2 = nhfa.na.drop(subset = ['subject_id'])

# Remove rows with missing admission dates
nhfa3 = nhfa2.na.drop(subset = ['admission_date'])

# Identify rows with missing `subject_id` to characterise these patients
missing_id = nhfa.filter(nhfa.subject_id.isNull())

# COMMAND ----------

# DBTITLE 1,Create unique admission identifier
# Assign a unique identifier to each admission in the NHFA
window_spec = w.partitionBy("subject_id").orderBy("admission_date")
nhfa_id = (
    nhfa3
    .select('subject_id', 'admission_date')
    .distinct()
    .withColumn('nicor_id', f.row_number().over(window_spec))
    .withColumn(
        'nicor_id', 
        f.when(f.col('nicor_id').rlike('^[0-9]$'), f.concat(f.lit('00'), f.col('nicor_id')))
        .when(f.col('nicor_id').rlike('^[0-9]{2}$'), f.concat(f.lit('0'), f.col('nicor_id')))
        .otherwise(f.col('nicor_id'))
    )
    .withColumn('nicor_id', f.concat(f.lit('NHFA'), f.col('nicor_id'), f.lit('_'), f.col('subject_id')))
)

# COMMAND ----------

# DBTITLE 1,Recode columns
nhfa4 = ( 
    nhfa3
    .join(nhfa_id, ['subject_id', 'admission_date'], how='left')
    .withColumn(
        'hf_diag_nicor',
        f.when(f.col('hf_diag_nicor').contains('Yes'), f.regexp_replace(f.col('hf_diag_nicor'), '^.*$', 'yes'))
        .when(f.col('hf_diag_nicor').contains('No'), f.regexp_replace(f.col('hf_diag_nicor'), '^.*$', 'no'))
        .otherwise(None)
    )
    .withColumn(
        'hf_diag_nicor_order',
        f.when(f.col('hf_diag_nicor_order').contains('New'), f.regexp_replace(f.col('hf_diag_nicor_order'), '^.*$', 'new diag'))
        .when(f.col('hf_diag_nicor_order').contains('Known'), f.regexp_replace(f.col('hf_diag_nicor_order'), '^.*$', 'known prev diag'))
        .otherwise(None)
    )
    .withColumn(
        'hf_admission_emergency',
        f.when(f.col('hf_admission_emergency').contains('Yes'), f.regexp_replace(f.col('hf_admission_emergency'), '^.*$', 'yes'))
        .when(f.col('hf_admission_emergency').contains('No'), f.regexp_replace(f.col('hf_admission_emergency'), '^.*$', 'no'))
        .otherwise(None)
    )
    .withColumn(
        'hf_admission_elective',
        f.when(f.col('hf_admission_elective').contains('Yes'), f.regexp_replace(f.col('hf_admission_elective'), '^.*$', 'yes'))
        .when(f.col('hf_admission_elective').contains('No'), f.regexp_replace(f.col('hf_admission_elective'), '^.*$', 'no'))
        .otherwise(None)
    )
    .withColumn(
        'sex',
        f.when(f.col('sex').contains('Female'), f.regexp_replace(f.col('sex'), '^.*$', 'female'))
        .when(f.col('sex').contains('Male'), f.regexp_replace(f.col('sex'), '^.*$', 'male'))
        .otherwise(None)
    )
    .withColumn(
        'ethnicity',
        f.when(f.col('ethnicity').contains('White'), f.regexp_replace(f.col('ethnicity'), '^.*$', 'white'))
        .when(f.col('ethnicity').contains('Black'), f.regexp_replace(f.col('ethnicity'), '^.*$', 'black'))
        .when(f.col('ethnicity').contains('Asian'), f.regexp_replace(f.col('ethnicity'), '^.*$*', 'asian'))
        .when(f.col('ethnicity').contains('Mixed'), f.regexp_replace(f.col('ethnicity'), '^.*$', 'mixed'))
        .when(f.col('ethnicity').contains('Asian'), f.regexp_replace(f.col('ethnicity'), '^.*$', 'asian'))
        .when(f.col('ethnicity').contains('Other'), f.regexp_replace(f.col('ethnicity'), '^.*$', 'other'))
        .otherwise(None)
    )
    .withColumn(
        'breathlessness',
        f.when(f.col('breathlessness').contains('1.'), f.regexp_replace(f.col('breathlessness'), '^.*$', 'no limitation of physical activity (nyha I)'))
        .when(f.col('breathlessness').contains('2.'), f.regexp_replace(f.col('breathlessness'), '^.*$', 'slight limitation of ordinary physical activity (nyha II)'))
        .when(f.col('breathlessness').contains('3.'), f.regexp_replace(f.col('breathlessness'), '^.*$', 'marked limitation of ordinary physical activity (nyha III)'))
        .when(f.col('breathlessness').contains('4.'), f.regexp_replace(f.col('breathlessness'), '^.*$', 'symptoms at rest or minimal activity (nyha IV)'))
        .otherwise(None)
    )
    .withColumn(
        'height_cm',
        f.when(f.col('height_cm') == -1, None)
        .when(f.col('height_cm') == 0, None)
        .otherwise(f.col('height_cm'))
    )
    .withColumn(
        'weight_kg_admission',
        f.when(f.col('weight_kg_admission') == -1, None)
        .when(f.col('weight_kg_admission') == 0, None)
        .otherwise(f.col('weight_kg_admission'))
    )
    .withColumn(
        'weight_kg_discharge',
        f.when(f.col('weight_kg_discharge') == -1, None)
        .when(f.col('weight_kg_discharge') == 0, None)
        .otherwise(f.col('weight_kg_discharge'))
    )
    .withColumn(
        'heart_rate_admission',
        f.when(f.col('heart_rate_admission') == -1, None)
        .when(f.col('heart_rate_admission') == 0, None)
        .otherwise(f.col('heart_rate_admission'))
    )
    .withColumn(
        'heart_rate_discharge',
        f.when(f.col('heart_rate_discharge') == -1, None)
        .when(f.col('heart_rate_discharge') == 0, None)
        .otherwise(f.col('heart_rate_discharge'))
    )
    .withColumn(
        'sbp_admission',
        f.when(f.col('sbp_admission') == -1, None)
        .when(f.col('sbp_admission') == 0, None)
        .otherwise(f.col('sbp_admission'))
    )
    .withColumn(
        'sbp_discharge',
        f.when(f.col('sbp_discharge') == -1, None)
        .when(f.col('sbp_discharge') == 0, None)
        .otherwise(f.col('sbp_discharge'))
    )
    .withColumn(
        'creat_admission',
        f.when(f.col('creat_admission') == -1, None)
        .when(f.col('creat_admission') == 0, None)
        .otherwise(f.col('creat_admission'))
    )
    .withColumn(
        'creat_discharge',
        f.when(f.col('creat_discharge') == -1, None)
        .when(f.col('creat_discharge') == 0, None)
        .otherwise(f.col('creat_discharge'))
    )
    .withColumn(
        'bnp',
        f.when(f.col('bnp') == -1, None)
        .when(f.col('bnp') == 0, None)
        .otherwise(f.col('bnp'))
    )
    .withColumn(
        'nt_pro_bnp',
        f.when(f.col('nt_pro_bnp') == -1, None)
        .when(f.col('nt_pro_bnp') == 0, None)
        .otherwise(f.col('nt_pro_bnp'))
    )
    .withColumn(
        'hf_specialist_input',
        f.when(f.col('hf_specialist_input').rlike('[0-9]{1,2}[.]'), f.regexp_replace(f.col('hf_specialist_input'), '[0-9]{1,2}[.] ', ''))
        .otherwise(f.col('hf_specialist_input'))
    )
    .withColumn(
        'hf_specialist_input',
        f.lower(f.col('hf_specialist_input'))
    )
    .withColumn(
        'ecg_rhythm',
        f.when(f.col('ecg_rhythm').rlike('[0-9]{1,2}[.]'), f.regexp_replace(f.col('ecg_rhythm'), '[0-9]{1,2}[.] ', ''))
        .otherwise(f.col('ecg_rhythm'))
    )
    .withColumn(
        'ecg_rhythm',
        f.lower(f.col('ecg_rhythm'))
    )
    .withColumn(
        'echo_mri_cag_result_12m',
        f.when(f.col('echo_mri_cag_result_12m').rlike('[0-9]{1,2}[.]'), f.regexp_replace(f.col('echo_mri_cag_result_12m'), '[0-9]{1,2}[.] ', ''))
        .otherwise(f.col('echo_mri_cag_result_12m'))
    )
    .withColumn(
        'echo_mri_cag_result_12m',
        f.lower(f.col('echo_mri_cag_result_12m'))
    )
    .withColumn(
        'lvef_most_recent',
        f.when(f.col('lvef_most_recent').rlike('[0-9]{1,2}[.]'), f.regexp_replace(f.col('lvef_most_recent'), '[0-9]{1,2}[.] ', ''))
        .otherwise(f.col('lvef_most_recent'))
    )
    .withColumn(
        'lvef_most_recent',
        f.lower(f.col('lvef_most_recent'))
    )
    .withColumn('admission_year', f.year(f.col('admission_date')))
    .sort('subject_id', 'admission_date')
)

# Remove additional bracketed text from column with LVEF (only works when run separately)
nhfa5 = (
  nhfa4
  .withColumn(
      'lvef_most_recent',      
      f.when(f.col('lvef_most_recent').rlike(' \\(.+\\)$'), f.regexp_replace(f.col('lvef_most_recent'), ' \\(.+\\)$', '')) 
      .otherwise(f.col('lvef_most_recent'))
  )
)

# COMMAND ----------

# DBTITLE 1,Define column order
col_order = [
  'subject_id', 'nicor_id', 'admission_date', 'admission_year', 'hf_diag_nicor', 'hf_diag_nicor_order', 
  'hf_specialist_input', 'hf_admission_emergency', 'hf_admission_elective', 'sex', 'ethnicity', 'smoking',
  'breathlessness', 'height_cm', 'weight_kg_admission', 'weight_kg_discharge', 'heart_rate_admission', 
  'heart_rate_discharge', 'sbp_admission', 'sbp_discharge', 'creat_admission', 'creat_discharge', 
  'bnp', 'nt_pro_bnp', 'potassium_discharge', 'ecg_rhythm', 'echo_mri_cag_result_12m', 'lvef_most_recent', 
  'echo_most_recent_date', 'mri_systolic_dysf', 'version'
]

# Sort columns
nhfa_final = nhfa5.select(col_order)

# Assign phenotypes (using version 2 of the algorithm)
nhfa_final = define_hf_subtypes(nhfa_final, add_v5_only=True, qc_passed=True)

# COMMAND ----------

duplicates = nhfa_final.groupBy('subject_id', 'admission_date').count().where(f.col('count') > 1)
duplicates2 = duplicates.join(nhfa_final, on=['subject_id', 'admission_date'], how='inner')
display(duplicates2)

# COMMAND ----------

# DBTITLE 1,Write data
nhfa_final.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.{write_tbl}_{ver}')