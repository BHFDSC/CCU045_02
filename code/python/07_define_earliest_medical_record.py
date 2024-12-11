# Databricks notebook source
# MAGIC %md
# MAGIC # 07_define_earliest_medical_record
# MAGIC
# MAGIC **Description** This defines patients' earliest available medical record in GDPPR, HES-APC, and HES-OP.
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
hes_apc_cln = 'ccu005_04_hes_apc_clean'
gdppr_cln = 'ccu005_04_gdppr_select'
hes_op = 'hes_op_all_years'

# Most recent archive dates
date = '2023-06-21'

# Writable table name
write_tbl = 'ccu005_04_earliest_medical_record'

# Pipeline version
ver = 'v2'

# COMMAND ----------

# DBTITLE 1,Load data
hes_cln = load(db_name=new_db, table_name=f'{hes_apc_cln}_{ver}', db_type='ccu005_04')
gdppr_cln = load(db_name=new_db, table_name=f'{gdppr_cln}_{ver}', db_type='ccu005_04')
hes_op_raw  = load(db_name=archive, table_name=hes_op, db_type='archive', archive_date=date)

# COMMAND ----------

# Provide summary of dates in a GDPPR dataset
def summarise_dates_gdppr(data):
    # Minimum and maximum dates
    min = data.agg({'date': 'min'}).collect()[0][0]
    max = data.agg({'date': 'max'}).collect()[0][0]
    # Number of years that the data span according to dates
    diff = max - min
    diff = round(diff.days / 365.25, 2)
    # Number of patient visits by calendar year
    counts = (
        data
        .withColumn('year', f.year('date'))
        .groupBy(f.col('year'))
        .agg(f.countDistinct(f.col('subject_id')))
    )
    total = data.agg(f.countDistinct(f.col('subject_id'))).collect()[0][0]
    yr1 = counts.where(f.col('year') == 2018).collect()[0][1]
    yr2 = counts.where(f.col('year') == 2019).collect()[0][1]
    yr3 = counts.where(f.col('year') == 2020).collect()[0][1]
    yr4 = counts.where(f.col('year') == 2021).collect()[0][1]
    yr5 = counts.where(f.col('year') == 2022).collect()[0][1]
    yr6 = counts.where(f.col('year') == 2023).collect()[0][1]
    yr1_perc = round((yr1 / total * 100), 1)
    yr2_perc = round((yr2 / total * 100), 1)
    yr3_perc = round((yr3 / total * 100), 1)
    yr4_perc = round((yr4 / total * 100), 1)
    yr5_perc = round((yr5 / total * 100), 1)
    yr6_perc = round((yr6 / total * 100), 1)
    print('DATE INFORMATION')
    name = [x for x in globals() if globals()[x] is data][0]
    print(f'Dataframe: `{name}`')
    print('----------------------------------------------')
    print('Date ranges and data coverage:')
    print(f'  Date range: {min} to {max}')
    print(f'  Difference: {diff} years')
    print('---')
    print('Patient visits by calendar year:')
    print(f'  2018: {yr1}/{total} ({yr1_perc}%)')
    print(f'  2019: {yr2}/{total} ({yr2_perc}%)')
    print(f'  2020: {yr3}/{total} ({yr3_perc}%)')
    print(f'  2021: {yr4}/{total} ({yr4_perc}%)')
    print(f'  2022: {yr5}/{total} ({yr5_perc}%)')
    print(f'  2023: {yr6}/{total} ({yr6_perc}%)')
    print('---')
    print(' ')

# COMMAND ----------

# Summarise HES-APC
# provide_counts(hes_cln, provide_hospitals=True, provide_subtype=False, addition_details='None')
# summarise_dates(hes_cln, provide_mapping_coverage=False)

# COMMAND ----------

# summarise_dates_gdppr(gdppr_cln)

# COMMAND ----------

# DBTITLE 1,Define first available medical record for HES-APC
# Define dataset ordering
window = w.partitionBy('subject_id').orderBy('hes_apc_earliest_date')

# Prepare data
hes_apc_dates = (
    hes_cln
    .select('subject_id', 'spell_id', f.col('admission_date').alias('hes_apc_earliest_date'))
    .filter(f.col('hes_apc_earliest_date') >= '1900-01-01')
    .distinct()
    .withColumn('row', f.row_number().over(window))
    .filter(f.col('row') == 1)
    .drop('spell_id', 'row')
    .distinct()
)

# Quality control
# provide_counts(hes_apc_dates, provide_hospitals=False, provide_subtype=False, addition_details='None')
adm_min = hes_apc_dates.agg({'hes_apc_earliest_date': 'min'}).collect()[0][0]
adm_max = hes_apc_dates.agg({'hes_apc_earliest_date': 'max'}).collect()[0][0]
print(f'Admission date range: {adm_min} to {adm_max}')

# COMMAND ----------

# DBTITLE 1,Define first available medical record for GDPPR
# Define dataset ordering
window2 = w.partitionBy('subject_id').orderBy('gdppr_earliest_date')

# Prepare data
gdppr_dates = (
    gdppr_cln
    .select('subject_id', f.col('date').alias('gdppr_earliest_date'))
    .filter(f.col('gdppr_earliest_date') >= '1900-01-01')
    .distinct()
    .withColumn('row', f.row_number().over(window2))
    .filter(f.col('row') == 1)
    .drop("row")
)

# min = gdppr_dates.agg({'date': 'min'}).collect()[0][0]
# max = gdppr_dates.agg({'date': 'max'}).collect()[0][0]
# print(f'Date range: {min} to {max}')

# COMMAND ----------

# DBTITLE 1,Define first available medical record for HES-OP
# Define dataset ordering
window3 = w.partitionBy('subject_id').orderBy('hes_op_earliest_date')

# Prepare data
hes_op_dates = (
    hes_op_raw
    .select(f.col('PERSON_ID_DEID').alias('subject_id'), f.col('APPTDATE').alias('hes_op_earliest_date'))
    .filter(f.col('hes_op_earliest_date') >= '1900-01-01')
    .distinct()
    .withColumn('row', f.row_number().over(window3))
    .filter(f.col('row') == 1)
    .drop("row")
)

# COMMAND ----------

# DBTITLE 1,Combine data
# Use HES-APC as a backbone
combined_dates = (
    hes_apc_dates
    .join(gdppr_dates, on='subject_id', how='left')
    .join(hes_op_dates, on='subject_id', how='left')
    .withColumn(
        'earliest_date',
        f.when((f.col('hes_apc_earliest_date').isNotNull()) & (f.col('gdppr_earliest_date').isNull()) & (f.col('hes_op_earliest_date').isNull()), f.col('hes_apc_earliest_date'))
        .when((f.col('gdppr_earliest_date').isNotNull()) & (f.col('hes_apc_earliest_date').isNull()) & (f.col('hes_op_earliest_date').isNull()), f.col('gdppr_earliest_date'))
        .when((f.col('hes_op_earliest_date').isNotNull()) & (f.col('gdppr_earliest_date').isNull()) & (f.col('hes_apc_earliest_date').isNull()), f.col('hes_op_earliest_date'))
        .when((f.col('hes_apc_earliest_date') <= f.col('gdppr_earliest_date')) & (f.col('hes_apc_earliest_date') <= f.col('hes_op_earliest_date')), f.col('hes_apc_earliest_date'))
        .when((f.col('gdppr_earliest_date') < f.col('hes_apc_earliest_date')) & (f.col('gdppr_earliest_date') < f.col('hes_op_earliest_date')), f.col('gdppr_earliest_date'))
        .when((f.col('hes_op_earliest_date') < f.col('hes_apc_earliest_date')) & (f.col('hes_op_earliest_date') < f.col('gdppr_earliest_date')), f.col('hes_op_earliest_date'))
        .when((f.col('hes_apc_earliest_date') <= f.col('hes_op_earliest_date')) & (f.col('gdppr_earliest_date').isNull()), f.col('hes_apc_earliest_date'))
        # Adding this line in because the `=` isn't registering for some reason
        .when((f.col('hes_apc_earliest_date') == f.col('hes_op_earliest_date')) & (f.col('gdppr_earliest_date').isNull()), f.col('hes_apc_earliest_date'))
        # Adding this line in because the `=` isn't registering for some reason
        .when((f.col('hes_apc_earliest_date') <= f.col('gdppr_earliest_date')) & (f.col('hes_op_earliest_date').isNull()), f.col('hes_apc_earliest_date'))
        .when((f.col('hes_apc_earliest_date') == f.col('gdppr_earliest_date')) & (f.col('hes_op_earliest_date').isNull()), f.col('hes_apc_earliest_date'))
        .when((f.col('gdppr_earliest_date') < f.col('hes_apc_earliest_date')) & (f.col('hes_op_earliest_date').isNull()), f.col('gdppr_earliest_date'))
        .when((f.col('gdppr_earliest_date') < f.col('hes_op_earliest_date')) & (f.col('hes_apc_earliest_date').isNull()), f.col('gdppr_earliest_date'))
        .when((f.col('gdppr_earliest_date') == f.col('hes_op_earliest_date')) & (f.col('hes_apc_earliest_date').isNull()), f.col('gdppr_earliest_date'))
        .when((f.col('hes_op_earliest_date') < f.col('hes_apc_earliest_date')) & (f.col('gdppr_earliest_date').isNull()), f.col('hes_op_earliest_date'))
        .when((f.col('hes_op_earliest_date') < f.col('gdppr_earliest_date')) & (f.col('hes_apc_earliest_date').isNull()), f.col('hes_op_earliest_date'))
        .when((f.col('gdppr_earliest_date') < f.col('hes_apc_earliest_date')) & (f.col('hes_op_earliest_date') < f.col('hes_apc_earliest_date')) & (f.col('gdppr_earliest_date') == f.col('hes_op_earliest_date')), f.col('gdppr_earliest_date'))
        .otherwise(None)
    )
)

# COMMAND ----------

# DBTITLE 1,QC combined dataframe
rows = combined_dates.count()
pats = combined_dates.agg(f.countDistinct(f.col('subject_id'))).collect()[0][0]
na_hes_apc = combined_dates.filter(f.col('hes_apc_earliest_date').isNull()).agg(f.countDistinct(f.col('subject_id'))).collect()[0][0]
na_gdppr = combined_dates.filter(f.col('gdppr_earliest_date').isNull()).agg(f.countDistinct(f.col('subject_id'))).collect()[0][0]
na_hes_op = combined_dates.filter(f.col('hes_op_earliest_date').isNull()).agg(f.countDistinct(f.col('subject_id'))).collect()[0][0]
na_combined = combined_dates.filter(f.col('earliest_date').isNull()).agg(f.countDistinct(f.col('subject_id'))).collect()[0][0]

print('QC INFORMATION')
print('-------------------------------')
print('General')
print(f'  Total rows: {rows}')
print(f'  Unique patients: {pats}')
print('---')
print('Missing data')
print(f'  Missing HES-APC dates: {na_hes_apc}')
print(f'  Missing GDPPR dates: {na_gdppr}')
print(f'  Missing HES-OP dates: {na_hes_op}')
print(f'  Missing combined dates: {na_combined}')

# COMMAND ----------

# DBTITLE 1,Write data
combined_dates.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.{write_tbl}_{ver}')