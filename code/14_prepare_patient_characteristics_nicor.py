# Databricks notebook source
# MAGIC %md
# MAGIC # 14_prepare_patient_characteristics_nicor
# MAGIC
# MAGIC **Description** This notebook prepares patient characteristics derived from all NICOR datasets.
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
subclass        = 'ccu005_04_incident_hf_subclass_skinny'
nicor_nhfa      = 'ccu005_04_nhfa_clean'
nicor_acs       = 'nicor_acs_combined_dars_nic_391419_j3w9t'
nicor_cong      = 'nicor_congenital_dars_nic_391419_j3w9t'
nicor_crm_eps   = 'nicor_crm_eps_dars_nic_391419_j3w9t'
nicor_crm_pmicd = 'nicor_crm_pmicd_dars_nic_391419_j3w9t'
nicor_minap     = 'nicor_minap_dars_nic_391419_j3w9t'
nicor_pci       = 'nicor_pci_dars_nic_391419_j3w9t'
nicor_tavi      = 'nicor_tavi_dars_nic_391419_j3w9t'

# Most recent archive dates
date = '2023-06-21'

# Writable table name
write_tbl = 'nicor_combined_lvef'

# Pipeline version
ver = 'v2'

# COMMAND ----------

# DBTITLE 1,Define functions
# Function to define NICOR identifiers
def assign_ids(table, date_var:str, id_prefix:str):
    window_spec = w.partitionBy("subject_id").orderBy(date_var)
    id = (
        table
        .select('subject_id', date_var)
        .distinct()
        .withColumn('nicor_id', f.row_number().over(window_spec))
        .withColumn(
            'nicor_id', 
            f.when(f.col('nicor_id').rlike('^[0-9]$'), f.concat(f.lit('00'), f.col('nicor_id')))
            .when(f.col('nicor_id').rlike('^[0-9]{2}$'), f.concat(f.lit('0'), f.col('nicor_id')))
            .otherwise(f.col('nicor_id'))
        )
        .withColumn('nicor_id', f.concat(f.lit(id_prefix), f.col('nicor_id'), f.lit('_'), f.col('subject_id')))
    )
    joined = table.join(id, ['subject_id', date_var], how='left')
    return(joined)

# COMMAND ----------

# DBTITLE 1,Load data
cohort = load(db_name=new_db, table_name=f'{subclass}_{ver}', db_type='ccu005_04')
nhfa = load(db_name=new_db, table_name=f'{nicor_nhfa}_{ver}', db_type='ccu005_04')
acs = load(db_name=archive, table_name=nicor_acs, db_type='archive', archive_date=date)
cong = load(db_name=archive, table_name=nicor_cong, db_type='archive', archive_date=date)
crm_eps = load(db_name=archive, table_name=nicor_crm_eps, db_type='archive', archive_date=date)
crm_pmicd = load(db_name=archive, table_name=nicor_crm_pmicd, db_type='archive', archive_date=date)
minap = load(db_name=archive, table_name=nicor_minap, db_type='archive', archive_date=date)
pci = load(db_name=archive, table_name=nicor_pci, db_type='archive', archive_date=date)
# Loading this table with my function does not work as there is no 'archived_on' column
tavi = spark.table('dars_nic_391419_j3w9t_collab.nicor_tavi_dars_nic_391419_j3w9t_archive')

# COMMAND ----------

nhfa_select = (
    nhfa
    .select(
        'subject_id', 'nicor_id', 'admission_date', 'sex', 'ethnicity', 'smoking', 'breathlessness', 'height_cm', 'weight_kg_admission', 'weight_kg_discharge', 'heart_rate_admission', 'heart_rate_discharge', 'sbp_admission', 'sbp_discharge', 'creat_admission', 'creat_discharge', 'bnp', 'nt_pro_bnp', 'potassium_discharge', 'ecg_rhythm'
    )
    .na.drop(subset = 'subject_id')
    .withColumn('operation_date', f.lit(None))
    .withColumn('discharge_date', f.lit(None))
)

# COMMAND ----------

# DBTITLE 1,Select and rename ACS columns
acs_select = (
    acs
    .select(
        f.col('PERSON_ID_DEID').alias('subject_id'),
        f.col('SEX').alias('sex'),
        f.col('HEIGHT').alias('height_cm'),
        f.col('WEIGHT').alias('weight_kg_admission'),
        f.col('CIGARETTE_SMOKING_HISTORY').alias('smoking'),
        f.col('ADMISSION_DATE').alias('admission_date'),
        f.col('DATE_DISCHARGE_OR_HOSP_DEATH').alias('discharge_date'),
        f.col('DATE_AND_TIME_OF_OPERATION').alias('operation_date'),
        f.col('ACTUAL_CREATININE_AT_SURGERY').alias('creat_admission'),
        f.col('PA_SYSTOLIC').alias('sbp_admission'),
        f.col('DYSPNOEA_STATUS_PRE_SURGERY').alias('breathlessness'),
        f.col('PRE_OPERATIVE_HEART_RHYTHM').alias('ecg_rhythm')
    )
    .withColumn('ethnicity', f.lit(None))
    .withColumn('weight_kg_discharge', f.lit(None))
    .withColumn('heart_rate_admission', f.lit(None))
    .withColumn('heart_rate_discharge', f.lit(None))
    .withColumn('sbp_discharge', f.lit(None))
    .withColumn('creat_discharge', f.lit(None))
    .withColumn('bnp', f.lit(None))
    .withColumn('nt_pro_bnp', f.lit(None))
    .withColumn('potassium_discharge', f.lit(None))
    .na.drop(subset = ['subject_id'])
)

# Assign a unique identifier to each record
acs_select2 = assign_ids(acs_select, date_var='operation_date', id_prefix='ACS')

# COMMAND ----------

# DBTITLE 1,Join NHFA and ACS
# Combine data
nhfa_acs = nhfa_select.unionByName(acs_select2)
display(nhfa_acs)

# COMMAND ----------

# DBTITLE 1,Select and rename congenital heart disease columns
cong_select = (
    cong
    .select(
        f.col('1_03_NHS_NUMBER_DEID').alias('subject_id'),
        f.col('1_07_GENDER').alias('sex'),
        f.col('1_08_ETHNIC_GROUP').alias('ethnicity'),
        f.col('2_03B_HEIGHT').alias('height_cm'),
        f.col('2_03_WEIGHT').alias('weight_kg_admission'),   
        f.col('6_02_PRE_PROCEDURE_SMOKING_STATUS').alias('smoking'),
        f.col('3_01_DATE_OF_VISIT').alias('admission_date'),
        f.col('4_01_DATE_OF_DISCHARGE').alias('discharge_date'),
        f.col('6_01_PRE_PROCEDURE_NYHA_STATUS').alias('breathlessness')
    )
    .na.drop(subset = ['subject_id'])
)

# Assign a unique identifier to each record
cong_select2 = assign_ids(cong_select, date_var='admission_date', id_prefix='CONG')

# Add missing columns
for column in [column for column in nhfa_acs.columns if column not in cong_select2.columns]:
    cong_select2 = cong_select2.withColumn(column, f.lit(None))

# COMMAND ----------

# DBTITLE 1,Select and rename CRM columns
crm_eps_select = (
    crm_eps
    .select(
        f.col('Person_ID_DEID').alias('subject_id'),
        f.col('4_07_FUNCTIONAL_STATUS_NYHA').alias('breathlessness'),
        f.col('3_01_PROCEDURE_DATE').alias('operation_date')
    )
)
crm_pmicd_select = (
    crm_pmicd
    .select(
        f.col('PERSON_ID_DEID').alias('subject_id'),
        f.col('1_07_SEX').alias('sex'),
        f.col('3_01_PROCEDURE_DATE_TIME').alias('operation_date'),
        f.col('2_06_NYHA_STABLE_FUNCTIONAL_STATUS').alias('breathlessness'),
        f.col('2_05_ATRIAL_RHYTHM_AT_IMPLANT').alias('ecg_rhythm')
    )
    .na.drop(subset = ['subject_id'])
)

# Assign a unique identifier to each record
crm_eps_select2 = assign_ids(crm_eps_select, date_var='operation_date', id_prefix='CRM_EPS')
crm_pmicd_select2 = assign_ids(crm_pmicd_select, date_var='operation_date', id_prefix='CRM_PMICD')

# Add missing columns
for column in [column for column in nhfa_acs.columns if column not in crm_eps_select2.columns]:
    crm_eps_select2 = crm_eps_select2.withColumn(column, f.lit(None))
for column in [column for column in nhfa_acs.columns if column not in crm_pmicd_select2.columns]:
    crm_pmicd_select2 = crm_pmicd_select2.withColumn(column, f.lit(None))

# COMMAND ----------

# DBTITLE 1,Select and rename MINAP columns
minap_select = (
    minap
    .select(
        f.col('1_03_NHS_NUMBER_DEID').alias('subject_id'),
        f.col('GENDER').alias('sex'),
        f.col('HEIGHT').alias('height_cm'),
        f.col('WEIGHT').alias('weight_kg_admission'),
        f.col('SMOKING_STATUS').alias('smoking'),
        f.col('ARRIVAL_AT_HOSPITAL').alias('admission_date'),
        f.col('SYSTOLIC_BP').alias('sbp_admission'),
        f.col('HEART_RATE').alias('heart_rate_admission'),
        f.col('CREATININE').alias('creat_admission')
    )
    .na.drop(subset = ['subject_id'])
)

# Assign a unique identifier to each record
minap_select2 = assign_ids(minap_select, date_var='admission_date', id_prefix='MINAP')

# Add missing columns
for column in [column for column in nhfa_acs.columns if column not in minap_select2.columns]:
    minap_select2 = minap_select2.withColumn(column, f.lit(None))

# COMMAND ----------

# DBTITLE 1,Select and rename PCI columns
pci_select = (
    pci
    .select(
        f.col('1_03_NHS_NUMBER_DEID').alias('subject_id'),
        f.col('DATETIME_OF_OPERATION').alias('operation_date'),
        f.col('GENDER').alias('sex'),
        f.col('HEIGHT').alias('height_cm'),
        f.col('WEIGHT').alias('weight_kg_admission'),
        f.col('SMOKING_STATUS').alias('smoking'),
        f.col('CREATININE').alias('creat_admission'),
        f.col('NYHA_DYSPNOEA_STATUS_PREPROC').alias('breathlessness')
    )
    .na.drop(subset = ['subject_id', 'operation_date'])
)

# Assign a unique identifier to each record
pci_select2 = assign_ids(pci_select, date_var='operation_date', id_prefix='PCI')

# Add missing columns
for column in [column for column in nhfa_acs.columns if column not in pci_select2.columns]:
    pci_select2 = pci_select2.withColumn(column, f.lit(None))

# COMMAND ----------

# DBTITLE 1,Select and rename TAVI columns
tavi_select = (
    tavi
    .select(
        f.col('PERSON_ID_DEID').alias('subject_id'),
        f.col('1_07_SEX').alias('sex'),
        f.col('1_08_ETHNIC_ORIGIN').alias('ethnicity'),
        f.col('5_01_HEIGHT').alias('height_cm'),
        f.col('5_02_WEIGHT').alias('weight_kg_admission'),
        f.col('3_02_SMOKING_STATUS').alias('smoking'),
        f.col('5_06_ADMISSION_DATE_FOR_PRO_1ST_HOSPITAL').alias('admission_date'),
        f.col('7_01_DATE_AND_TIME_OF_OPERATION').alias('operation_date'),
        f.col('6_012_PA_SYSTOLIC_PRESSURE_MMHG').alias('sbp_admission'),
        f.col('3_03_CREATININE').alias('creat_admission'),
        f.col('12_03_IF_ALIVE_NYHA_DYSPNOEA_STATUS_3Y').alias('breathlessness'),
        f.col('3_11_PRE_OPERATIVE_HEART_RHYTHM').alias('ecg_rhythm')
    )
    .na.drop(subset = ['subject_id', 'operation_date'])
)

# Assign a unique identifier to each record
tavi_select2 = assign_ids(tavi_select, date_var='operation_date', id_prefix='TAVI')

# Add missing columns
for column in [column for column in nhfa_acs.columns if column not in tavi_select2.columns]:
    tavi_select2 = tavi_select2.withColumn(column, f.lit(None))

# COMMAND ----------

# DBTITLE 1,Combine all datasets
dfs = [nhfa_acs, cong_select2, crm_eps_select2, crm_pmicd_select2, minap_select2, pci_select2, tavi_select2]
nicor_char = reduce(f.DataFrame.unionByName, dfs) 
nicor_char2 = cohort.select('subject_id').distinct().join(nicor_char, on='subject_id', how='inner')
display(nicor_char2)

# COMMAND ----------

# DBTITLE 1,Combine dates into one column
nicor_char3 = (
    nicor_char2
    .withColumn(
        'date_nicor',
        f.when(f.col('admission_date').isNotNull(), f.to_date('admission_date'))
        .when(f.col('admission_date').isNull(),     f.to_date('operation_date'))
        .otherwise(None)
    )
)
display(nicor_char3)

# COMMAND ----------

# DBTITLE 1,Write data
nicor_char3.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.ccu005_04_nicor_characteristics_{ver}')