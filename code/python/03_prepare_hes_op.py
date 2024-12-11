# Databricks notebook source
# MAGIC %md
# MAGIC # 03_prepare_hes_op
# MAGIC
# MAGIC **Description** This notebook prepares a cleaned Hospital Episode Statistics for outpatient appointments (HES OP) dataset.
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
hes_apc = 'hes_apc_all_years'
hes_op = 'hes_op_all_years'
hes_otr = 'hes_apc_otr_all_years'

# Most recent archive dates
date = '2023-06-21'

# Writable table name
write_tbl = 'ccu005_04_hes_op_clean'

# Pipeline version
ver = 'v2'

# COMMAND ----------

# DBTITLE 1,Load data
hes_op_raw  = load(db_name=archive, table_name=hes_op, db_type='archive', archive_date=date)

# COMMAND ----------

display(hes_op_raw)

# COMMAND ----------

# DBTITLE 1,Create long format HES episodes data
# Select relevant columns
_hes_op = (
    hes_op_raw 
    .select(['PERSON_ID_DEID', 'ATTENDKEY', 'APPTDATE'] 
        + [col for col in list(hes_op_raw.columns) if re.match(r'^DIAG_(3|4)_\d\d$', col)])
    .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')
    .orderBy('PERSON_ID', 'ATTENDKEY')
)

# Reshape table to long format
hes_op_long = (
    reshape_wide_to_long_multi(_hes_op, i=['PERSON_ID', 'ATTENDKEY', 'APPTDATE'], j='POSITION', stubnames=['DIAG_4_', 'DIAG_3_'])
    .withColumn('_tmp', f.substring(f.col('DIAG_4_'), 1, 3))
    .withColumn('_chk', udf_null_safe_equality('DIAG_3_', '_tmp').cast(t.IntegerType()))
    .withColumn('_DIAG_4_len', f.length(f.col('DIAG_4_')))
    .withColumn('_chk2', f.when((f.col('_DIAG_4_len').isNull()) | (f.col('_DIAG_4_len') <= 4), 1).otherwise(0))
)

# Drop columns
hes_op_long = (
     hes_op_long
    .drop('_tmp', '_chk')
)

# Clean table and order
hes_op_long = (
    reshape_wide_to_long_multi(hes_op_long, i=['PERSON_ID', 'ATTENDKEY', 'APPTDATE', 'POSITION'], j='DIAG_DIGITS', stubnames=['DIAG_'])
    .withColumnRenamed('POSITION', 'DIAG_POSITION')
    .withColumn('DIAG_POSITION', f.regexp_replace('DIAG_POSITION', r'^[0]', ''))
    .withColumn('DIAG_DIGITS', f.regexp_replace('DIAG_DIGITS', r'[_]', ''))
    .withColumn('DIAG_', f.regexp_replace('DIAG_', r'X$', '')) \
    .withColumn('DIAG_', f.regexp_replace('DIAG_', r'[.,\-\s]', ''))
    .withColumnRenamed('DIAG_', 'CODE')
    .where((f.col('CODE').isNotNull()) & (f.col('CODE') != ''))
    .orderBy(['PERSON_ID', 'ATTENDKEY', 'DIAG_DIGITS', 'DIAG_POSITION'])
)

# COMMAND ----------

# DBTITLE 1,Rename columns for long format data
# Rename columns to be consistent with project format
hes_op_long_renamed = hes_op_long \
    .where(f.col('DIAG_DIGITS') != 3) \
    .select(
        f.col('PERSON_ID').alias('subject_id'),
        f.col('ATTENDKEY').alias('appointment_id'),
        f.col('APPTDATE').alias('appointment_date'),
        f.col('CODE').alias('diagnosis_code'),
        f.col('DIAG_POSITION').alias('diagnosis_position'),
        f.col('DIAG_DIGITS').alias('diagnosis_digits')
    )

# COMMAND ----------

# DBTITLE 1,Display and quality control data
# Show data
display(hes_op_long_renamed)

# Count rows/patients/spells
rows = hes_op_long_renamed.count()
pats = hes_op_long_renamed.agg(f.countDistinct(f.col('subject_id'))).collect()[0][0]
appt = hes_op_long_renamed.agg(f.countDistinct(f.col('appointment_id'))).collect()[0][0]

print('QC INFORMATION')
print('-------------------------------')
print('General')
print(f'  Total rows: {rows}')
print(f'  Unique patients: {pats}')
print(f'  Unique appointments: {appt}')

# COMMAND ----------

# DBTITLE 1,Write data
hes_op_long_renamed.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.{write_tbl}_{ver}')