# Databricks notebook source
# MAGIC %md
# MAGIC # 06_prepare_nicor_other
# MAGIC
# MAGIC **Description** This notebook prepares a cleaned dataset for other NICOR CV audits that have data on left ventricular ejection fraction (LVEF).
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
live = 'dars_nic_391419_j3w9t'
archive = 'dars_nic_391419_j3w9t_collab'
new_db = 'dsa_391419_j3w9t_collab'

# Tables
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
write_tbl = 'ccu005_04_nicor_combined_lvef'

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
acs = load(db_name=archive, table_name=nicor_acs, db_type='archive', archive_date=date)
cong = load(db_name=archive, table_name=nicor_cong, db_type='archive', archive_date=date)
crm_eps = load(db_name=archive, table_name=nicor_crm_eps, db_type='archive', archive_date=date)
crm_pmicd = load(db_name=archive, table_name=nicor_crm_pmicd, db_type='archive', archive_date=date)
minap = load(db_name=archive, table_name=nicor_minap, db_type='archive', archive_date=date)
pci = load(db_name=archive, table_name=nicor_pci, db_type='archive', archive_date=date)
# Loading this table with my function does not work as there is no 'archived_on' column
tavi = spark.table('dars_nic_391419_j3w9t_collab.nicor_tavi_dars_nic_391419_j3w9t_archive')

# COMMAND ----------

# DBTITLE 1,Select and rename ACS columns
acs_select = (
    acs
    .select(
        f.col('PERSON_ID_DEID').alias('subject_id'),
        f.col('VERSION_FILE_SUBMITTED_IN').alias('version'),
        f.col('SEX').alias('sex'),
        f.col('HEIGHT').alias('height'),
        f.col('WEIGHT').alias('weight'),
        f.col('CIGARETTE_SMOKING_HISTORY').alias('smoking'),
        f.col('HOSPITAL_IDENTIFIER').alias('hosp_id'),
        f.col('PRESENTATION').alias('presentation'),
        f.col('ADMISSION_DATE').alias('admission_date'),
        f.col('DATE_DISCHARGE_OR_HOSP_DEATH').alias('discharge_date'),
        f.col('DATE_AND_TIME_OF_OPERATION').alias('operation_date'),
        f.col('OPERATIVE_URGENCY').alias('operation_urgency'),
        f.col('CATEGORY_OF_AVS').alias('avs_category'),
        f.col('AORTIC_VALVE_PROCEDURE').alias('aortic_valve_category'),
        f.col('AORTIC_VALVE_TYPE').alias('aortic_valve_type'),
        f.col('CABG').alias('cabg'),
        f.col('OTHER_ACTUAL_CARDIAC_PROCS').alias('other_cardiac_proc'),
        f.col('PATIENT_STATUS_AT_DISCHARGE').alias('discharge_status'),
        f.col('NUMBER_OF_PREVIOUS_MIS').alias('prev_mi_num'),
        f.col('INTERVAL_SURGERY_AND_LAST_MI').alias('last_mi_interval_surg'),
        f.col('LEFT_VENTRICULAR_FUNCTION').alias('lvef'),
        f.col('EJECTION_FRACTION_CATEGORY').alias('lvef_grp'),
        f.col('ACTUAL_CREATININE_AT_SURGERY').alias('scr'),
        f.col('PA_SYSTOLIC').alias('pa_sbp'),
        f.col('PRE_OPERATIVE_HEART_RHYTHM').alias('heart_rhythm_pre_op'),
        f.col('DIABETES_MANAGEMENT').alias('diabetes'),
        f.col('HISTORY_OF_HYPERTENSION').alias('htn'),
        f.col('PREVIOUS_PCI').alias('pci'),
        f.col('PREVIOUS_CARDIAC_SURGERY').alias('cardiac_surg'),
        f.col('RENAL_FUNCTION_DIALYSIS').alias('renal_function_dialysis')
    )
)

# Remove rows with missing `subject_id`
acs_select2 = acs_select.na.drop(subset = ['subject_id'])

# Assign a unique identifier to each record
acs_select3 = assign_ids(acs_select2, date_var='operation_date', id_prefix='ACS')

# COMMAND ----------

# DBTITLE 1,Select and rename congenital heart disease columns
cong_select = (
    cong
    .select(
        f.col('1_03_NHS_NUMBER_DEID').alias('subject_id'),
        f.col('1_07_GENDER').alias('sex'),
        f.col('1_08_ETHNIC_GROUP').alias('ethnicity'),
        f.col('2_03B_HEIGHT').alias('height'),
        f.col('2_03_WEIGHT').alias('weight'),   
        f.col('6_02_PRE_PROCEDURE_SMOKING_STATUS').alias('smoking'),
        f.col('3_01_DATE_OF_VISIT').alias('visit_date'),
        f.col('4_01_DATE_OF_DISCHARGE').alias('discharge_date'),
        f.col('2_04_ANTENATAL_DIAGNOSIS').alias('antenatal_diag'),
        f.col('2_06B_COMORBIDITY_PRESENT').alias('comorbidity_present'),
        f.col('2_07_COMORBID_CONDITIONS').alias('comorbidity_diags'),
        f.col('3_01B_PROCEDURE_URGENCY').alias('procedure_urgency'),
        f.col('3_07_TYPE_OF_PROCEDURE').alias('procedure_type'),
        f.col('3_09_OPERATION_PERFORMED').alias('operation_performed'),
        f.col('4_03_DISCHARGE_STATUS').alias('discharge_status'),
        f.col('4_04_DISCHARGE_DESTINATION').alias('discharge_destination'),
        f.col('2_08_PRE_PROCEDURE_SYSTEMIC_VENTRICULAR_EJECTION_FRACTION').alias('systemic_ef_grp'),
        f.col('2_09_PRE_PROCEDURE_SUB_PULMONARY_VENTRICULAR_EJECTION_FRACTION').alias('sub_pulm_ef_grp'),
        f.col('6_03_PRE_PROCEDURE_DIABETES').alias('diabetes'),
        f.col('6_04_HISTORY_OF_PULMONARY_DISEASE').alias('asthma_copd'),
        f.col('6_06_PRE_PROCEDURAL_ISCHAEMIC_HEART_DISEASE').alias('ihd')
    )
)

# Remove rows with missing `subject_id`
cong_select2 = cong_select.na.drop(subset = ['subject_id'])

# Assign a unique identifier to each record
cong_select3 = assign_ids(cong_select2, date_var='visit_date', id_prefix='CONG')

# COMMAND ----------

# DBTITLE 1,Select and rename CRM columns
crm_pmicd_select = (
    crm_pmicd
    .select(
        f.col('PERSON_ID_DEID').alias('subject_id'),
        f.col('1_07_SEX').alias('sex'),
        f.col('3_01_PROCEDURE_DATE_TIME').alias('procedure_date'),
        f.col('2_01_DATE_OF_FIRST_IMPLANT').alias('first_implant_date'),
        f.col('2_02_PREDEVICE_AETIOLOGY').alias('predevice_aetiology'),
        f.col('2_03_PREDEVICE_SYMPTOMS').alias('predevice_symptoms'),
        f.col('2_04_ECG_DIAGNOSIS_INDICATION').alias('ecg_indication'),
        f.col('2_05_ATRIAL_RHYTHM_AT_IMPLANT').alias('implant_atrial_rhythm'),
        f.col('2_07_LV_FUNCTION').alias('lvef_grp'),
        f.col('2_08_ICD_INDICATION').alias('icd_indication')
    )
)

# Remove rows with missing `subject_id`
crm_pmicd_select2 = crm_pmicd_select.na.drop(subset = ['subject_id'])

# Assign a unique identifier to each record
crm_pmicd_select3 = assign_ids(crm_pmicd_select2, date_var='procedure_date', id_prefix='CRM_PMICD')

# COMMAND ----------

# DBTITLE 1,Select and rename MINAP columns
minap_select = (
    minap
    .select(
        f.col('1_03_NHS_NUMBER_DEID').alias('subject_id'),
        f.col('GENDER').alias('sex'),
        f.col('HEIGHT').alias('height'),
        f.col('WEIGHT').alias('weight'),
        f.col('SMOKING_STATUS').alias('smoking'),
        f.col('ARRIVAL_AT_HOSPITAL').alias('admission_date'),
        f.col('ADMISSION_DIAGNOSIS').alias('admission_diag'),
        f.col('DISCHARGE_DIAGNOSIS').alias('discharge_diag'),
        f.col('DEATH_IN_HOSPITAL').alias('hosp_death'),
        f.col('SITE_OF_INFARCTION').alias('infarct_site'),
        f.col('CORONARY_INTERVENTION').alias('coronary_intervention'),
        f.col('PROC_PERFORMED').alias('procedure_performed'),
        f.col('SYSTOLIC_BP').alias('sbp'),
        f.col('HEART_RATE').alias('heart_rate'),
        f.col('CREATININE').alias('scr'),
        f.col('LVEF').alias('lvef_grp'),
        f.col('HEART_FAILURE').alias('heart_failure'),
        f.col('DIABETES').alias('diabetes'),
        f.col('HYPERTENSION').alias('htn'),
        f.col('HYPERCHOLESTEROLAEMIA').alias('hchol'),
        f.col('CHRONIC_RENAL_FAILURE').alias('ckd'),
        f.col('CEREBROVASCULAR_DISEASE').alias('cerebro_vasc_dis'),
        f.col('ASTHMA_OR_COPD').alias('asthma_copd'),
        f.col('PREVIOUS_ANGINA').alias('angina'), 
        f.col('PREVIOUS_CABG').alias('cabg'),
        f.col('PREVIOUS_PCI').alias('pci'),
        f.col('BETA_BLOCKER').alias('bb'),
        f.col('DISCHARGED_ON_BETA_BLOCKER').alias('bb_discharge'),
        f.col('ANGIOTENSIN').alias('ras'),
        f.col('ACE_I_OR_ARB').alias('ras_prior'),
        f.col('DISCHARGED_ON_ACE_I').alias('ras_discharge')
    )
)

# Remove rows with missing `subject_id`
minap_select2 = minap_select.na.drop(subset = ['subject_id'])

# Assign a unique identifier to each record
minap_select3 = assign_ids(minap_select2, date_var='admission_date', id_prefix='MINAP')

# COMMAND ----------

# DBTITLE 1,Select and rename PCI columns
pci_select = (
    pci
    .select(
        f.col('1_03_NHS_NUMBER_DEID').alias('subject_id'),
        f.col('DATETIME_OF_OPERATION').alias('operation_date'),
        f.col('STATUS_AT_DISCHARGE').alias('discharge_status'),
        f.col('GENDER').alias('sex'),
        f.col('HEIGHT').alias('height'),
        f.col('WEIGHT').alias('weight'),
        f.col('SMOKING_STATUS').alias('smoking'),
        f.col('CLINICAL_SYNDROME_PCI').alias('clinical_syndrome'),
        f.col('INDICATION_FOR_INTERVENTION').alias('indication_for_intervention'),
        f.col('PROCEDURE_URGENCY').alias('procedure_urgency'),
        f.col('CARDIOGENIC_SHOCK_PREPROC').alias('cardiogenic_shock_preproc'),
        f.col('LV_EJECTION_FRACTION').alias('lvef'), 
        f.col('LV_EJECTION_FRACTION_CATEGORY').alias('lvef_grp'),
        f.col('CREATININE').alias('scr'),
        f.col('PREVIOUS_MI').alias('mi'),
        f.col('PREVIOUS_CABG').alias('cabg'),
        f.col('PREVIOUS_PCI').alias('pci'),
        f.col('DIABETES').alias('diabetes')
    )
)

# Remove rows with missing `subject_id`
pci_select2 = pci_select.na.drop(subset = ['subject_id'])

# Remove rows with missing `operation_date`
pci_select2 = pci_select2.na.drop(subset = ['operation_date'])

# Assign a unique identifier to each record
pci_select3 = assign_ids(pci_select2, date_var='operation_date', id_prefix='PCI')

# COMMAND ----------

# DBTITLE 1,Select and rename TAVI columns
tavi_select = (
    tavi
    .select(
        f.col('PERSON_ID_DEID').alias('subject_id'),
        f.col('1_07_SEX').alias('sex'),
        f.col('1_08_ETHNIC_ORIGIN').alias('ethnicity'),
        f.col('5_01_HEIGHT').alias('height'),
        f.col('5_02_WEIGHT').alias('weight'),
        f.col('3_02_SMOKING_STATUS').alias('smoking'),
        f.col('5_06_ADMISSION_DATE_FOR_PRO_1ST_HOSPITAL').alias('admission_date'),
        f.col('7_01_DATE_AND_TIME_OF_OPERATION').alias('operation_date'),
        f.col('7_06_PROCEDURE_URGENCY').alias('procedure_urgency'),
        f.col('6_012_PA_SYSTOLIC_PRESSURE_MMHG').alias('sbp'),
        f.col('3_03_CREATININE').alias('scr'),
        f.col('6_08_LV_FUNCTION').alias('lvef_grp'),
        f.col('6_071_MITRAL_REGURGITATION').alias('mitral_regurg'),
        f.col('7_121_USE_OF_CARDIOPULMONARY_BYPASS').alias('cardiopulmonary_bypass_used'),
        f.col('3_01_DIABETES').alias('diabetes'),
        f.col('3_041_ON_DIALYSIS').alias('dialysis'),
        f.col('3_06_HISTORY_OF_PULMONARY_DISEASE').alias('asthma_copd'),
        f.col('3_071_SEVERE_LIVER_DISEASE').alias('severe_liver_disease'),
        f.col('3_08_HISTORY_OF_NEUROLOGICAL_DISEASE').alias('neuro_disease'),
        f.col('3_09_EXTRACARDIAC_ARTERIOPATHY').alias('extracardiac_arteriopathy'),
        f.col('4_023_PREVIOUS_TAVI').alias('tavi'),
        f.col('4_03_PREVIOUS_PCI').alias('pci'),
    )
)

# Remove rows with missing `subject_id`
tavi_select2 = tavi_select.na.drop(subset = ['subject_id'])

# Assign a unique identifier to each record
tavi_select3 = assign_ids(tavi_select2, date_var='operation_date', id_prefix='TAVI')

# COMMAND ----------

# DBTITLE 1,Aggregate LVEF
# Adult cardiac surgery audit
acs_lvef = (
    acs_select3
    .select('subject_id', 'nicor_id', f.col('operation_date').alias('date'), 'lvef_grp', 'lvef')
    .withColumn(
        'lvef',
        f.when(f.col('lvef') == 0, None)
        .otherwise(f.col('lvef'))
    )
    .withColumn(
        'subtype',
        f.when((f.col('lvef').isNotNull()) & (f.col('lvef') >= 50), 'hfpef')
        .when((f.col('lvef').isNotNull()) & (f.col('lvef') > 40) & (f.col('lvef') < 50),  'hfpef')
        .when((f.col('lvef').isNotNull()) & (f.col('lvef') <= 40), 'hfref')
        .when((f.col('lvef').isNull()) & (f.col('lvef_grp').rlike('> 50%')), 'hfpef')
        .when((f.col('lvef').isNull()) & (f.col('lvef_grp').rlike('21 - 30%|< 30%|<21%')), 'hfref')
        .otherwise(None)
    )
)

# Congenital heart disease
cong_lvef = (
    cong_select3
    .select('subject_id', 'nicor_id', f.col('visit_date').alias('date'), f.col('sub_pulm_ef_grp').alias('lvef_grp'))
    .withColumn('lvef', f.lit(None))
    .withColumn(
        'subtype',
        f.when((f.col('lvef').isNull()) & (f.col('lvef_grp').rlike('Good')), 'hfpef')
        .when((f.col('lvef').isNull()) & (f.col('lvef_grp').rlike('Poor')), 'hfref')
        .otherwise(None)
    )
)

# Cardiac rhythm management
crm_lvef = (
    crm_pmicd_select3
    .select('subject_id', 'nicor_id', f.col('procedure_date').alias('date'), 'lvef_grp')
    .withColumn('lvef', f.lit(None))
    .withColumn(
        'subtype',
        f.when((f.col('lvef').isNull()) & (f.col('lvef_grp').rlike('Good')), 'hfpef')
        .when((f.col('lvef').isNull()) & (f.col('lvef_grp').rlike('Poor')), 'hfref')
        .otherwise(None)
    )
)

# Myocardial ischaemia
minap_lvef = (
    minap_select3
    .select('subject_id', 'nicor_id', f.col('admission_date').alias('date'), 'lvef_grp')
    .withColumn('lvef', f.lit(None))
    .withColumn(
        'subtype',
        f.when((f.col('lvef').isNull()) & (f.col('lvef_grp').rlike('Good')), 'hfpef')
        .when((f.col('lvef').isNull()) & (f.col('lvef_grp').rlike('Poor')), 'hfref')
        .otherwise(None)
    )
)
pci_lvef = (
    pci_select3
    .select('subject_id', 'nicor_id', f.col('operation_date').alias('date'), 'lvef_grp', 'lvef')
    .withColumn(
        'lvef',
        f.when(f.col('lvef') == 0, None)
        .otherwise(f.col('lvef'))
    )
    .withColumn(
        'subtype',
        f.when((f.col('lvef').isNotNull()) & (f.col('lvef') >= 50), 'hfpef')
        .when((f.col('lvef').isNotNull()) & (f.col('lvef') > 40) & (f.col('lvef') < 50),  'hfpef')
        .when((f.col('lvef').isNotNull()) & (f.col('lvef') <= 40), 'hfref')
        .when((f.col('lvef').isNull()) & (f.col('lvef_grp').rlike('Good')), 'hfpef')
        .when((f.col('lvef').isNull()) & (f.col('lvef_grp').rlike('Poor')), 'hfref')
        .otherwise(None)
    )
)
tavi_lvef = (
    tavi_select3
    .select('subject_id', 'nicor_id', f.col('operation_date').alias('date'), 'lvef_grp')
    .withColumn('lvef', f.lit(None))
    .withColumn(
        'subtype',
        f.when((f.col('lvef').isNull()) & (f.col('lvef_grp').rlike('Good')), 'hfpef')
        .when((f.col('lvef').isNull()) & (f.col('lvef_grp').rlike('Poor')), 'hfref')
        .otherwise(None)
    )
)

dfs = [acs_lvef, cong_lvef, crm_lvef, minap_lvef, pci_lvef, tavi_lvef]
lvef = (
    reduce(f.DataFrame.unionByName, dfs) 
    .withColumn(
        'lvef_grp',
        f.when(f.col('lvef_grp').rlike('Unknown|Not applicable|Not assessed|Not measured'), None)
        .when(f.col('lvef_grp').rlike('^[0-9]{1,2}[.]'), f.regexp_replace(f.col('lvef_grp'), '^[0-9]{1,2}[.] ', ''))
        .otherwise(f.col('lvef_grp'))
    )
)
missing = lvef.filter((f.col('lvef_grp').isNull()) & (f.col('lvef').isNull())).select('subject_id', 'nicor_id', 'date').distinct()
lvef2 = lvef.join(missing, ['subject_id', 'nicor_id', 'date'], how = 'left_anti')

# COMMAND ----------

# DBTITLE 1,Write data
lvef2.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.{write_tbl}_{ver}')