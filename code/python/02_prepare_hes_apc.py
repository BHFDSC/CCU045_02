# Databricks notebook source
# MAGIC %md
# MAGIC # 02_prepare_hes_apc
# MAGIC
# MAGIC **Description** This notebook prepares a cleaned Hospital Episode Statistics for admitted patient care (HES APC) dataset.
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

# DBTITLE 1,Source site code mappings (v1 - contains information on which hospitals (should) submit to the NHFA)
# MAGIC %run "/Repos/raf69@cam.ac.uk/ccu005_04/nicor/src/map_site_codes_v1"

# COMMAND ----------

# DBTITLE 1,Source site code mappings (v2 - more up-to-date, and contains standardised hospital names, postcodes, etc.)
# MAGIC %run "/Repos/raf69@cam.ac.uk/ccu005_04/nicor/src/map_site_codes_v2"

# COMMAND ----------

# DBTITLE 1,Define variables
# Databases
live    = 'dars_nic_391419_j3w9t'
archive = 'dars_nic_391419_j3w9t_collab'
new_db  = 'dsa_391419_j3w9t_collab'

# Tables
hes_apc = 'hes_apc_all_years'
hes_op  = 'hes_op_all_years'
hes_otr = 'hes_apc_otr_all_years'

# Most recent archive date
date = '2024-03-27'

# Writable table name
write_tbl = 'ccu005_04_hes_apc_clean'

# Pipeline version
ver = 'v2'

# COMMAND ----------

dates = spark.table('dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive').select(f.col('archived_on')).distinct().sort(f.col('archived_on').desc())
dates.show()

# COMMAND ----------

hes = spark.table('dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive')
display(hes.select('archived_on').distinct().sort('archived_on'))

# COMMAND ----------

# DBTITLE 1,Load data
hes_apc_raw = load(db_name=archive, table_name=hes_apc, db_type='archive', archive_date=date)
hes_otr_raw = load(db_name=archive, table_name=hes_otr, db_type='archive', archive_date=date)
# Currently commented out as not required
# hes_op_raw  = load(db_name=archive, table_name=hes_op, db_type='archive', archive_date=date)

# COMMAND ----------

# DBTITLE 1,Prepare hospital site codes
hosp_v1 = hosp_v1.select('hospital_site_code', 'hospital_submits_to_nhfa')
hosp = hosp_v2.join(hosp_v1, ['hospital_site_code'], how='left')
hosp.show()

# COMMAND ----------

# DBTITLE 1,Select and rename columns
# Select and rename variable in HES APC
hes_apc_select = (
    hes_apc_raw
    .select(
        f.col('PERSON_ID_DEID').alias('subject_id'), 
        f.col('EPIKEY').alias('episode_id'), 
        f.col('ADMIDATE').alias('admission_date'), 
        f.col('DISDATE').alias('discharge_date'), 
        f.col('EPISTART').alias('episode_start'), 
        f.col('EPIEND').alias('episode_end'), 
        f.col('EPIORDER').alias('episode_order'),
        f.col('SPELBGIN').alias('spell_begin'), 
        f.col('SPELEND').alias('spell_end'), 
        f.col('ADMIMETH').alias('admission_method'), 
        f.col('DISDEST').alias('discharge_destination'), 
        f.col('DISMETH').alias('discharge_method'),
        f.col('DIAG_4_CONCAT').alias('icd10_concat'), 
        f.col('GPPRAC').alias('gp_practice_code'), 
        f.col('MAINSPEF').alias('consult_main_specialty'), 
        f.col('OPERTN_4_CONCAT').alias('opcs4_concat'),
        f.col('PCTCODE_HIS').alias('primary_care_trust'), 
        f.col('PROCODE3').alias('healthcare_provider_code'), 
        f.col('SITETRET').alias('hospital_site_code'),
        f.col('LSOA01').alias('lsoa_residence')
    )
)

# COMMAND ----------

# DBTITLE 1,Check dates
adm_min = hes_apc_select.agg({'admission_date': 'min'}).collect()[0][0]
adm_max = hes_apc_select.agg({'admission_date': 'max'}).collect()[0][0]
print(f'Admission date range: {adm_min} to {adm_max}')

# COMMAND ----------

# DBTITLE 1,Get unique spell identifier
# Get spell identifier ('SUSPELLID') from the other HES APC data (NOTE: many of these values are missing so they are not currently being used to this pipeline)
spells_otr = (
    hes_otr_raw
    .filter(f.col('SUSSPELLID').isNotNull())
    .select(
         f.col('PERSON_ID_DEID').alias('subject_id'),
         f.col('SUSSPELLID').alias('spell_id_otr'),
         f.col('EPIKEY').alias('episode_id')
    )
    .distinct()
)

# Rejoin to HES APC selected dataframe
hes_apc_select2 = hes_apc_select.join(spells_otr, ['subject_id', 'episode_id'], how='left')

# COMMAND ----------

# DBTITLE 1,Create unique spell identifier
# Create unique spell identifier for each patient for each admission date
window_spec = w.partitionBy("subject_id").orderBy("admission_date")
spells_df = (
    hes_apc_select2
    .select('subject_id', 'admission_date')
    .distinct()
    .withColumn('spell_id', f.row_number().over(window_spec))
    .withColumn(
        'spell_id', 
        f.when(f.col('spell_id').rlike('^[0-9]$'), f.concat(f.lit('000'), f.col('spell_id')))
        .when(f.col('spell_id').rlike('^[0-9]{2}$'), f.concat(f.lit('00'), f.col('spell_id')))
        .when(f.col('spell_id').rlike('^[0-9]{3}$'), f.concat(f.lit('0'), f.col('spell_id')))
        .otherwise(f.col('spell_id'))
    )
    .withColumn('spell_id', f.concat(f.lit('SPELL'), f.col('spell_id'), f.lit('_'), f.col('subject_id')))
)

# Rejoin to HES APC selected dataframe
hes_apc_select3 = hes_apc_select2.join(spells_df, ['subject_id', 'admission_date'], how='left')

# COMMAND ----------

# DBTITLE 1,Create long format HES episodes data
# Select relevant columns
_hes_apc = (
  hes_apc_raw 
  .select(['PERSON_ID_DEID', 'EPIKEY', 'EPISTART'] 
          + [col for col in list(hes_apc_raw.columns) if re.match(r'^DIAG_(3|4)_\d\d$', col)])
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')
  .orderBy('PERSON_ID', 'EPIKEY')
)

# Reshape table to long format
hes_apc_long = (
  reshape_wide_to_long_multi(_hes_apc, i=['PERSON_ID', 'EPIKEY', 'EPISTART'], j='POSITION', stubnames=['DIAG_4_', 'DIAG_3_'])
  .withColumn('_tmp', f.substring(f.col('DIAG_4_'), 1, 3))
  .withColumn('_chk', udf_null_safe_equality('DIAG_3_', '_tmp').cast(t.IntegerType()))
  .withColumn('_DIAG_4_len', f.length(f.col('DIAG_4_')))
  .withColumn('_chk2', f.when((f.col('_DIAG_4_len').isNull()) | (f.col('_DIAG_4_len') <= 4), 1).otherwise(0))
)

# Drop columns
hes_apc_long = (
  hes_apc_long
  .drop('_tmp', '_chk')
)

# Clean table and order
hes_apc_long = (
  reshape_wide_to_long_multi(hes_apc_long, i=['PERSON_ID', 'EPIKEY', 'EPISTART', 'POSITION'], j='DIAG_DIGITS', stubnames=['DIAG_'])
  .withColumnRenamed('POSITION', 'DIAG_POSITION')
  .withColumn('DIAG_POSITION', f.regexp_replace('DIAG_POSITION', r'^[0]', ''))
  .withColumn('DIAG_DIGITS', f.regexp_replace('DIAG_DIGITS', r'[_]', ''))
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'X$', '')) \
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'[.,\-\s]', ''))
  .withColumnRenamed('DIAG_', 'CODE')
  .where((f.col('CODE').isNotNull()) & (f.col('CODE') != ''))
  .orderBy(['PERSON_ID', 'EPIKEY', 'DIAG_DIGITS', 'DIAG_POSITION'])
)

# COMMAND ----------

# DBTITLE 1,Rename columns for long format data
# Rename columns to be consistent with project format
hes_apc_long_renamed = hes_apc_long \
    .where(f.col('DIAG_DIGITS') != 3) \
    .select(
        f.col('PERSON_ID').alias('subject_id'),
        f.col('EPIKEY').alias('episode_id'),
        f.col('EPISTART').alias('episode_start'),
        f.col('CODE').alias('diagnosis_code'),
        f.col('DIAG_POSITION').alias('diagnosis_position'),
        f.col('DIAG_DIGITS').alias('diagnosis_digits')
    )

# COMMAND ----------

# DBTITLE 1,Join all data
# Truncate HES APC selected dataframe
spells_all = (
    hes_apc_select3
    .select(
        'subject_id', 'spell_id', 'spell_id_otr', 'episode_id', 'admission_date', 'discharge_date', 
        'spell_begin', 'spell_end', 'episode_start', 'episode_order', 'admission_method', 
        'discharge_method', 'discharge_destination', 'healthcare_provider_code', 
        'hospital_site_code'
    )
    .distinct()
)

# Join data and reorder columns
hes_apc_long_spells = (
    hes_apc_long_renamed
    .join(spells_all, on=['subject_id', 'episode_id', 'episode_start'], how='left')
    .select(
        'subject_id', 'spell_id', 'spell_id_otr', 'episode_id', 'admission_date', 'discharge_date',
        'spell_begin', 'spell_end', 'episode_start', 'episode_order', 'diagnosis_code', 
        'diagnosis_position', 'admission_method', 'discharge_method', 'discharge_destination',
        'healthcare_provider_code', 'hospital_site_code'
    )
)

# Recode invalid discharde dates and code diagnosis position as integer
hes_apc_long_spells = (
    hes_apc_long_spells
    .withColumn(
        'discharge_date',
        f.when(hes_apc_long_spells.discharge_date.rlike('1801-01-01'), None) \
        .otherwise(hes_apc_long_spells.discharge_date)
    )
    .withColumn('diagnosis_position', hes_apc_long_spells['diagnosis_position'].cast(IntegerType()))
)

hes_apc_long_spells = (
    hes_apc_long_spells
    .withColumn(
        'discharge_date',
        f.first(f.col('discharge_date'), ignorenulls=True) \
        .over(
            w.partitionBy('subject_id', 'spell_id') \
            .orderBy('subject_id', 'spell_id', 'episode_order', 'diagnosis_position') \
            .rowsBetween(0, sys.maxsize)
        ) 
    )
    # Add LSOA
    .join(hes_apc_select.select('subject_id', 'episode_id', 'lsoa_residence').distinct(), on=['subject_id', 'episode_id'], how='left')
    # Add Trust names
    .join(trust, on='healthcare_provider_code', how='left')
    # Add Hospital names
    .join(hosp, on='hospital_site_code', how='left')
    # Reorder columns again
    .select(
        'subject_id', 'spell_id', 'spell_id_otr', 'episode_id', 'admission_date', 'discharge_date',
        'spell_begin', 'spell_end', 'episode_start', 'episode_order', 'diagnosis_code', 
        'diagnosis_position', 'admission_method', 'discharge_method', 'discharge_destination', 'lsoa_residence',
        'healthcare_provider_code', 'healthcare_provider_name', 'hospital_site_code',
        'hospital_site_name', 'standardised_name', 'town', 'district', 'constituency', 'county',
        'country', 'postcode', 'legal_start_date', 'legal_end_date', 'legal_status', 'managed_by_code',
        'managed_by_name', 'hospital_submits_to_nhfa'
    )
)

# COMMAND ----------

# DBTITLE 1,Display and quality control data
# Show data
# display(hes_apc_long_spells)

# Count rows/patients/spells
# rows = hes_apc_long_spells.count()
# pats = hes_apc_long_spells.agg(f.countDistinct(f.col('subject_id'))).collect()[0][0]
# spel = hes_apc_long_spells.agg(f.countDistinct(f.col('spell_id'))).collect()[0][0]
# epis = hes_apc_long_spells.agg(f.countDistinct(f.col('episode_id'))).collect()[0][0]

# print('QC INFORMATION')
# print('-------------------------------')
# print('General')
# print(f'  Total rows: {rows}')
# print(f'  Unique patients: {pats}')
# print(f'  Unique spells: {spel}')
# print(f'  Unique episodes: {epis}')

# COMMAND ----------

# DBTITLE 1,Write data
hes_apc_long_spells.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.{write_tbl}_{ver}')