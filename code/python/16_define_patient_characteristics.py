# Databricks notebook source
# MAGIC %md
# MAGIC # 16_define_patient_characteristics
# MAGIC
# MAGIC **Description** This notebook prepares patient characteristics derived from NICOR data and GDPPR.
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
subclass  = 'ccu005_04_incident_hf_subclass_skinny_imd'
nicor_ch  = 'ccu005_04_nicor_characteristics'
gdppr_hf  = 'ccu005_04_gdppr_characteristics'

# Pipeline version
ver = 'v2'

# COMMAND ----------

# DBTITLE 1,Load data
cohort = load(db_name=new_db, table_name=f'{subclass}_{ver}', db_type='ccu005_04')
nicor =  load(db_name=new_db, table_name=f'{nicor_ch}_{ver}', db_type='ccu005_04')
gdppr =  load(db_name=new_db, table_name=f'{gdppr_hf}_{ver}', db_type='ccu005_04')

# COMMAND ----------

# check uniqueness
print(cohort.select("subject_id").count())
print(cohort.select("subject_id").distinct().count())

# COMMAND ----------

# DBTITLE 1,Height
# Prepare NICOR height
height_nicor = (
    nicor
    .withColumn(
        'height_cm',
        f.when(f.col('height_cm') == 0, None)
        .when((f.col('height_cm') > 0) & (f.col('height_cm') <= 3), f.col('height_cm') * 100)
        .otherwise(f.col('height_cm'))
    )
    .where((f.col('nicor_id').isNotNull()) & (f.col('height_cm').isNotNull()))
    .select('subject_id', 'nicor_id', 'date_nicor', 'height_cm')
    .distinct()
)

# Prepare GDPPR height
height_gdppr = (
    gdppr
    .where((f.col('snomed_code') == '50373000') & (f.col('value1_condition').isNotNull()))
    .select('subject_id', 'date', f.col('value1_condition').alias('height_cm'))
    .distinct()
    .withColumn(
        'height_cm',
        f.when(f.col('height_cm') == 0, None)
        .when((f.col('height_cm') > 0) & (f.col('height_cm') <= 3), f.col('height_cm') * 100)
        .otherwise(f.col('height_cm'))
    )
    .where(f.col('height_cm').isNotNull())
)

# Join heights
height_nicor2 = height_nicor.select('subject_id', f.col('date_nicor').alias('date'), 'height_cm').distinct()
height_combined = height_nicor2.unionByName(height_gdppr).distinct()

# Get most recent date measurement to the heart failure hospitalisation
window_spec = w.partitionBy('subject_id').orderBy(f.col('diff')) 
cohort_height = (
    cohort
    .join(height_combined, on='subject_id', how='left')
    .withColumn('diff', f.abs(f.datediff(f.col('admission_date'), f.col('date'))))
    .withColumn('row', f.row_number().over(window_spec))
    .where(f.col('row') == 1)
    .select('subject_id', 'height_cm')
)
display(cohort_height)

# COMMAND ----------

# DBTITLE 1,Weight
# Prepare NICOR
weight_nicor = (
    nicor
    .where((f.col('nicor_id').isNotNull()) & ((f.col('weight_kg_admission').isNotNull()) | (f.col('weight_kg_discharge').isNotNull())))
    .withColumn(
        'weight_kg',
        f.when(f.col('weight_kg_discharge').isNotNull(), f.col('weight_kg_discharge'))
        .when((f.col('weight_kg_discharge').isNull()) & (f.col('weight_kg_admission').isNotNull()), f.col('weight_kg_admission'))
        .otherwise(None)
    )
    .withColumn(
        'weight_kg',
        f.when((f.col('weight_kg') == 0) | (f.col('weight_kg') >= 1000), None)
        .otherwise(f.col('weight_kg'))
    )
    .select('subject_id', 'nicor_id', 'date_nicor', 'weight_kg')
    .where(f.col('weight_kg').isNotNull())
    .distinct()
)

# Prepare GDPPR
weight_gdppr = (
    gdppr
    .where((f.col('snomed_code') == '27113001') & (f.col('value1_condition').isNotNull()))
    .select('subject_id', 'date', f.col('value1_condition').alias('weight_kg'))
    .distinct()
    .withColumn(
        'weight_kg',
        f.when((f.col('weight_kg') == 0) | (f.col('weight_kg') >= 1000), None)
        .otherwise(f.col('weight_kg'))
    )
    .where(f.col('weight_kg').isNotNull())
)

# Join
weight_nicor2 = weight_nicor.select('subject_id', f.col('date_nicor').alias('date'), 'weight_kg').distinct()
weight_combined = weight_nicor2.unionByName(weight_gdppr).distinct()

# Get most recent date measurement to the heart failure hospitalisation (within 1 year)
window_spec = w.partitionBy('subject_id').orderBy(f.col('diff')) 
cohort_weight = (
    cohort
    .join(weight_combined, on='subject_id', how='left')
    .withColumn('diff', f.abs(f.datediff(f.col('admission_date'), f.col('date'))))
    .where((f.col('date') <= f.col('admission_date')) & (f.col('diff') <= 365.25)) # 365.25 days is 1 year
    .withColumn('row', f.row_number().over(window_spec))
    .where(f.col('row') == 1)
    .select('subject_id', f.col('date').alias('weight_date'), 'weight_kg')
)
cohort_weight.count()

# COMMAND ----------

# DBTITLE 1,Body-mass index
cohort_hw = (
    cohort
    .join(cohort_height, on='subject_id', how='left')
    .join(cohort_weight, on='subject_id', how='left')
    .withColumn(
        'bmi',
        f.when((f.col('height_cm').isNotNull()) & (f.col('weight_kg').isNotNull()), f.col('weight_kg') / pow(f.col('height_cm') / 100, 2))
        .otherwise(None)
    )
    .withColumn(
        'bmi',
        f.when(f.col('bmi') < 10, None)
        .otherwise(f.col('bmi'))
    )
)
na_bmi = cohort_hw.where(f.col('bmi').isNull()).select('subject_id', 'admission_date').distinct()

# Prepare GDPPR BMI
bmi_gdppr = (
    gdppr
    .where((f.col('snomed_code') == '60621009') & (f.col('value1_condition').isNotNull()))
    .select('subject_id', 'date', f.col('value1_condition').alias('bmi'))
    .distinct()
    .withColumn(
        'bmi',
        f.when(f.col('bmi') < 10, None)
        .otherwise(f.col('bmi'))
    )
    .where(f.col('bmi').isNotNull())
    .join(cohort, on='subject_id', how='inner')
    .withColumn('diff', f.abs(f.datediff(f.col('admission_date'), f.col('date'))))
    .where((f.col('date') <= f.col('admission_date')) & (f.col('diff') <= 365.25)) # 365.25 days is 1 year
    .withColumn('row', f.row_number().over(window_spec))
    .where(f.col('row') == 1)
    .select('subject_id', f.col('date').alias('bmi_date'), 'bmi')
)

# Join BMI from NICOR and BMI from GDPPR
# tmpt1 = cohort_hw.where(f.col('bmi').isNotNull()).select('subject_id', 'bmi').distinct()
cohort_bmi = bmi_gdppr# .unionByName(tmpt1).distinct().cache()

# display(cohort_bmi.groupBy('subject_id').count().where(f.col('count') > 1).join(cohort_bmi, on = 'subject_id', how = 'inner'))
# display(cohort_bmi.groupBy('subject_id').count().where(f.col('count') > 1))

# COMMAND ----------

# DBTITLE 1,Systolic blood pressure
sbp_nicor = (
    nicor
    .where((f.col('nicor_id').isNotNull()) & ((f.col('sbp_admission').isNotNull()) | (f.col('sbp_discharge').isNotNull())))
    .withColumn(
        'sbp',
        f.when(f.col('sbp_discharge').isNull(), f.col('sbp_admission'))
         .when(f.col('sbp_admission').isNull(), f.col('sbp_discharge'))
         .when((f.col('sbp_discharge').isNotNull()) & (f.col('sbp_admission').isNotNull()), ((f.col('sbp_admission') + f.col('sbp_discharge')) / 2))
         .otherwise(None)
    )
    .withColumn(
        'sbp',
        f.when((f.col('sbp') < 80) | (f.col('sbp') > 300), None)
        .otherwise(f.col('sbp'))
    )
    .select('subject_id', 'nicor_id', 'date_nicor', 'sbp')
    .where(f.col('sbp').isNotNull())
    .distinct()
)

# Prepare GDPPR systolic blood pressure
sbp_gdppr = (
    gdppr
    .where((f.col('snomed_code').rlike('271649006|1162737008|213041000000101|198081000000101|251070002|314438006|314439003|314440001|314464000|407556006|407554009|399304008|413606001|716579001|72313002|945871000000107|314449000|400974009')) & (f.col('value1_condition').isNotNull()))
    .select('subject_id', 'date', f.col('value1_condition').alias('sbp'), 'term')
    .distinct()
    .withColumn(
        'sbp',
        f.when((f.col('sbp') < 80) | (f.col('sbp') > 300), None)
        .otherwise(f.col('sbp'))
    )
    .where(f.col('sbp').isNotNull())
    .drop('term')
)

# Join
sbp_nicor2 = sbp_nicor.select('subject_id', f.col('date_nicor').alias('date'), 'sbp').distinct()
sbp_combined = sbp_nicor2.unionByName(sbp_gdppr).distinct()

# Get most recent date measurement to the heart failure hospitalisation (6 months)
window_spec = w.partitionBy('subject_id').orderBy(f.col('diff')) 
cohort_sbp = (
    cohort
    .join(sbp_combined, on='subject_id', how='left')
    .withColumn('diff', f.abs(f.datediff(f.col('admission_date'), f.col('date'))))
    .where((f.col('date') <= f.col('admission_date')) & (f.col('diff') <= 365.25)) # 365.25 days is 1 year
    .withColumn('row', f.row_number().over(window_spec))
    .where(f.col('row') == 1)
    .select('subject_id', 'sbp')
)
display(cohort_sbp.groupBy('subject_id').count().where(f.col('count') > 1))

# COMMAND ----------

# DBTITLE 1,eGFR
scr_nicor = (
    nicor
    .withColumn(
        'creat_admission',
        f.when(f.col('creat_admission') == 0, None)
        .when(f.col('creat_admission') >= 10000, f.col('creat_admission') / 1000)
        .otherwise(f.col('creat_admission'))
    )
    .withColumn(
        'creat_discharge',
        f.when(f.col('creat_discharge') == 0, None)
        .when(f.col('creat_discharge') >= 10000, f.col('creat_discharge') / 1000)
        .otherwise(f.col('creat_discharge'))
    )
    .where((f.col('nicor_id').isNotNull()) & ((f.col('creat_admission').isNotNull()) | (f.col('creat_discharge').isNotNull())))
    .withColumn(
        'scr',
        f.when(f.col('creat_discharge').isNull(), f.col('creat_admission'))
         .when(f.col('creat_admission').isNull(), f.col('creat_discharge'))
         .when((f.col('creat_discharge').isNotNull()) & (f.col('creat_admission').isNotNull()), ((f.col('creat_admission') + f.col('creat_discharge')) / 2))
        .otherwise(None)
    )
    .select('subject_id', 'nicor_id', 'date_nicor', 'scr')
    .where(f.col('scr').isNotNull())
    .distinct()
)
scr_nicor2 = scr_nicor.select('subject_id', f.col('date_nicor').alias('date'), 'scr').distinct()

# Prepare GDPPR SCR
scr_gdppr = (
    gdppr
    .where((f.col('term').rlike('creatin')) & (f.col('value1_condition').isNotNull()))
    .select('subject_id', 'date', f.col('value1_condition').alias('scr'), 'term')
    .distinct()
    .withColumn(
        'scr',
        f.when((f.col('scr') == 0 ), None)
        .otherwise(f.col('scr'))
    )
    .where(f.col('scr').isNotNull())
    .drop('term')
)

# Join SCR
scr = scr_nicor2.unionByName(scr_gdppr)

# Prepare GDPPR eGFR
gfr_gdppr = (
    gdppr
    .where((f.col('name').rlike('egfr')) & (f.col('value1_condition').isNotNull()))
    .select('subject_id', 'date', f.col('value1_condition').alias('egfr'), 'term')
    .distinct()
    .withColumn(
        'egfr',
        f.when((f.col('egfr') == 0 ), None)
        .otherwise(f.col('egfr'))
    )
    .where(f.col('egfr').isNotNull())
    .drop('term')
)

# Get most recent date measurement to the heart failure hospitalisation (3 months)
window_spec = w.partitionBy('subject_id').orderBy(f.col('diff')) 
cohort_scr = (
    cohort
    .join(scr, on='subject_id', how='left')
    .withColumn('diff', f.abs(f.datediff(f.col('admission_date'), f.col('date'))))
    .where(f.col('diff') <= 365.25) # 365.25 days is approx 1 year
    .withColumn('row', f.row_number().over(window_spec))
    .where(f.col('row') == 1)
    .select('subject_id', f.col('date').alias('scr_date'), 'scr')
)

cohort_gfr = (
    cohort
    .join(gfr_gdppr, on='subject_id', how='left')
    .withColumn('diff', f.abs(f.datediff(f.col('admission_date'), f.col('date'))))
    .where((f.col('date') <= f.col('admission_date')) & (f.col('diff') <= 365.25)) # 365.25 days is 1 year
    .withColumn('row', f.row_number().over(window_spec))
    .where(f.col('row') == 1)
    .select('subject_id', f.col('date').alias('egfr_date'), 'egfr')
)

cohort_sg = (
    cohort
    .select('subject_id')
    .distinct()
    .join(cohort_scr, on='subject_id', how='left')
    .join(cohort_gfr, on='subject_id', how='left')
)
display(cohort_sg)

# COMMAND ----------

# DBTITLE 1,Serum potassium
k_nicor = (
    nicor
    .withColumn(
        'potassium_discharge',
        f.when(f.col('potassium_discharge') <= 0, None)
        .otherwise(f.col('potassium_discharge'))
    )
    .where((f.col('nicor_id').isNotNull()) & (f.col('potassium_discharge').isNotNull()))
    .select('subject_id', 'nicor_id', 'date_nicor', f.col('potassium_discharge').alias('k'))
    .where(f.col('k').isNotNull())
    .distinct()
)

# Prepare GDPPR potassium
k_gdppr = (
    gdppr
    .where((f.col('name').rlike('potassium')) & (f.col('value1_condition').isNotNull()))
    .select('subject_id', 'date', f.col('value1_condition').alias('k'), 'term')
    .distinct()
    .withColumn(
        'k',
        f.when((f.col('k') < 0 ), None)
        .otherwise(f.col('k'))
    )
    .where(f.col('k').isNotNull())
    .drop('term')
)

# Join
k_nicor2 = k_nicor.select('subject_id', f.col('date_nicor').alias('date'), 'k').distinct()
k_combined = k_nicor2.unionByName(k_gdppr).distinct()

# Get most recent date measurement to the heart failure hospitalisation (3 months)
window_spec = w.partitionBy('subject_id').orderBy(f.col('diff')) 
cohort_k = (
    cohort
    .join(k_combined, on='subject_id', how='left')
    .withColumn('diff', f.abs(f.datediff(f.col('admission_date'), f.col('date'))))
    .where((f.col('date') <= f.col('admission_date')) & (f.col('diff') <= 365.25)) # 365.25 days is 1 year
    .withColumn('row', f.row_number().over(window_spec))
    .where(f.col('row') == 1)
    .select('subject_id', 'k')
)
display(cohort_k)

# COMMAND ----------

# DBTITLE 1,BNP and NT-pro-BNP
bnp_nicor = (
    nicor
    .where((f.col('bnp') != 1) | (f.col('nt_pro_bnp') != 1))
    .where((f.col('nicor_id').isNotNull()) & ((f.col('bnp').isNotNull()) | (f.col('nt_pro_bnp').isNotNull())))
    .select('subject_id', 'nicor_id', 'date_nicor', 'bnp', 'nt_pro_bnp')
    .distinct()
)
# cohort_bnp = cohort.join(bnp_nicor, on=['subject_id', 'nicor_id'], how='left').drop('date_nicor')
# na_bnp = cohort_bnp.where(((f.col('bnp').isNotNull()) & (f.col('nt_pro_bnp').isNotNull())))
# na_bnp_nicor = na_bnp.select('subject_id').distinct().join(bnp_nicor, on='subject_id', how='inner')

# Separate
bnp_nicor2 = bnp_nicor.select('subject_id', f.col('date_nicor').alias('date'), 'bnp').where((f.col('bnp').isNotNull()) & (f.col('bnp') != 1))
ntprobnp_nicor = bnp_nicor.select('subject_id', f.col('date_nicor').alias('date'), 'nt_pro_bnp').where((f.col('bnp').isNotNull()) & (f.col('nt_pro_bnp') != 1))

# Get most recent date measurement to the heart failure hospitalisation
window_spec = w.partitionBy('subject_id').orderBy(f.col('diff')) 
cohort_bnp = (
    cohort
    .join(bnp_nicor2, on='subject_id', how='left')
    .withColumn('diff', f.abs(f.datediff(f.col('admission_date'), f.col('date'))))
    .where(f.col('diff') <= 365.25)
    .withColumn('row', f.row_number().over(window_spec))
    .where(f.col('row') == 1)
    .select('subject_id', 'bnp')
)

cohort_ntprobnp = (
    cohort
    .join(ntprobnp_nicor, on='subject_id', how='left')
    .withColumn('diff', f.abs(f.datediff(f.col('admission_date'), f.col('date'))))
    .where(f.col('diff') <= 365.25)
    .withColumn('row', f.row_number().over(window_spec))
    .where(f.col('row') == 1)
    .select('subject_id', 'nt_pro_bnp')
)

# Join NICOR BNP
# tmpt1 = cohort_bnp.select('subject_id', 'bnp', 'nt_pro_bnp').where((f.col('bnp').isNotNull()) | (f.col('nt_pro_bnp').isNotNull()))
# tmpt2 = cohort_bnp2.select('subject_id', 'bnp', 'nt_pro_bnp')
# cohort_bnp3 = tmpt1.unionByName(tmpt2).distinct().cache()

cohort_bnp_ntprobnp = (
    cohort
    .select('subject_id')
    .distinct()
    .join(cohort_bnp, on='subject_id', how='left')
    .join(cohort_ntprobnp, on='subject_id', how='left')
)

# Check
cohort_bnp_ntprobnp.count()

# display(cohort_bnp3.groupBy('subject_id').count().where(f.col('count') > 1).join(cohort_bnp3, on='subject_id', how='inner').sort('subject_id'))
display(cohort_bnp_ntprobnp)

# COMMAND ----------

# DBTITLE 1,NYHA
nyha_nicor = (
    nicor
    .where((f.col('nicor_id').isNotNull()) & (f.col('breathlessness').isNotNull()) & (~f.col('breathlessness').rlike('Unknown|NA$')))
    .select('subject_id', 'nicor_id', 'date_nicor', f.col('breathlessness').alias('nyha'))
    .distinct()
)

# Prepare GDPPR NYHA
nyha_gdppr = (
    gdppr
    .where((f.col('name').rlike('nyha')))
    .select('subject_id', 'date', f.col('term').alias('nyha'))
    .distinct()
)

# Join
nyha_nicor2 = nyha_nicor.select('subject_id', f.col('date_nicor').alias('date'), 'nyha').distinct()
nyha_combined = nyha_nicor2.unionByName(nyha_gdppr).distinct()

# Get most recent date measurement to the heart failure hospitalisation (6 months)
window_spec = w.partitionBy('subject_id').orderBy(f.col('diff')) 
cohort_nyha = (
    cohort
    .join(nyha_combined, on='subject_id', how='left')
    .withColumn('diff', f.abs(f.datediff(f.col('admission_date'), f.col('date'))))
    .where((f.col('date') <= f.col('admission_date')) & (f.col('diff') <= 365.25)) # 365.25 days is 1 year
    .withColumn('row', f.row_number().over(window_spec))
    .where(f.col('row') == 1)
    .select('subject_id', 'nyha')
    .withColumn(
        'nyha',
        f.when(f.col('nyha').rlike('[N|n]o limitation|Class I '), '1')
        .when(f.col('nyha').rlike('[S|s]light limitation|Class II '), '2')
        .when(f.col('nyha').rlike('[M|m]arked limitation|Class III '), '3')
        .when(f.col('nyha').rlike('[S|s]ymptoms at rest|Class IV '), '4')
        .otherwise(f.col('nyha'))
    )
)
display(cohort_nyha)

# COMMAND ----------

# DBTITLE 1,Smoking
smok_nicor = (
    nicor
    .where((f.col('nicor_id').isNotNull()) & (f.col('smoking').isNotNull()) & (~f.col('smoking').rlike('Unknown|status unknown$')))
    .select('subject_id', 'nicor_id', 'date_nicor', 'smoking')
    .distinct()
)

# Prepare GDPPR smoking
smok_gdppr = (
    gdppr
    .where((f.col('name').rlike('smoking')))
    .select('subject_id', 'date', f.col('name').alias('smoking'))
    .distinct()
)

# Join
smok_nicor2 = smok_nicor.select('subject_id', f.col('date_nicor').alias('date'), 'smoking').distinct()
smok_combined = smok_nicor2.unionByName(smok_gdppr).distinct()

# Get most recent date measurement to the heart failure hospitalisation (1 year)
window_spec = w.partitionBy('subject_id').orderBy(f.col('diff')) 
cohort_smok = (
    cohort
    .join(smok_combined, on='subject_id', how='left')
    .withColumn('diff', f.abs(f.datediff(f.col('admission_date'), f.col('date'))))
    .where((f.col('date') <= f.col('admission_date')) & (f.col('diff') <= 365.25)) # 365.25 days is 1 year
    .withColumn('row', f.row_number().over(window_spec))
    .where(f.col('row') == 1)
    .select('subject_id', 'smoking')
    .withColumn(
        'smoking',
        f.when(f.col('smoking').rlike('[N|n]ever|Non smoker'), 'never')
        .when(f.col('smoking').rlike('[C|c]urrent|Yes$'), 'current')
        .when(f.col('smoking').rlike('[E|e]x'), 'former')
        .otherwise(f.col('smoking'))
    )
)
display(cohort_smok)

# COMMAND ----------

# DBTITLE 1,Join all together
# Join together
cohort_char = (
    cohort
    .join(cohort_height, on='subject_id', how='left')
    .join(cohort_weight, on='subject_id', how='left')
    .join(cohort_bmi, on='subject_id', how='left')
    .join(cohort_sbp, on='subject_id', how='left')
    .join(cohort_sg, on='subject_id', how='left')
    .join(cohort_k, on='subject_id', how='left')
    .join(cohort_bnp_ntprobnp, on='subject_id', how='left')
    .join(cohort_nyha, on='subject_id', how='left')
    .join(cohort_smok, on='subject_id', how='left')
)
cohort_char.count()

# COMMAND ----------

display(cohort_char)

# COMMAND ----------

# DBTITLE 1,Write data
cohort_char.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.ccu005_04_incident_hf_subclass_skinny_imd_char_{ver}')