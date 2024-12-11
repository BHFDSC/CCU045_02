# Databricks notebook source
# MAGIC %md
# MAGIC # 15_prepare_patient_characteristics_gdppr
# MAGIC
# MAGIC **Description** This notebook prepares patient characteristics derived from GDPPR.
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
subclass  = 'ccu005_04_incident_hf_subclass_skinny'
gdppr_cln = 'ccu005_04_gdppr_select'

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
gdppr = load(db_name=new_db, table_name=f'{gdppr_cln}_{ver}', db_type='ccu005_04')

spark.sql(f'REFRESH TABLE dss_corporate.gdppr_cluster_refset')
gdppr_refset = spark.table('dss_corporate.gdppr_cluster_refset')

# COMMAND ----------

# DBTITLE 1,Combine cohort with GDPPR
gdppr_cohort = cohort.select('subject_id').distinct().join(gdppr, on='subject_id', how='left')
# Write to database
gdppr_cohort.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.ccu005_04_gdppr_hf_cohort_{ver}')

# COMMAND ----------

# DBTITLE 1,Load cohort GDPPR
gdppr_hf = load(db_name=new_db, table_name=f'ccu005_04_gdppr_hf_cohort_{ver}', db_type='ccu005_04')

# COMMAND ----------

# DBTITLE 1,Body-mass index
list_bmi = [
    '50373000', # height
    '27113001', # weight
    '722595002', 
    '914741000000103',
    '914731000000107',
    '914721000000105',
    '35425004',
    '48499001',
    '301331008',
    '6497000',
    '310252000',
    '427090001',
    '408512008',
    '162864005',
    '162863004',
    '412768003',
    '60621009',
    '846931000000101'
]
tmp1_bmi = (
  gdppr_refset
  .select('ConceptId', 'ConceptId_description')
  .where(f.col('ConceptId').isin(list_bmi))
  .dropDuplicates(['ConceptId'])
  .select(f.col('ConceptId').alias('code'), f.col('ConceptId_description').alias('term'))
  .orderBy('code')
)

# Check
print(tmp1_bmi.toPandas().to_string()); print()

# BMIVAL_COD
tmp2_bmi = (
  gdppr_refset
  .select('Cluster_ID', 'Cluster_Desc', 'ConceptId', 'ConceptId_description')
  .where(f.col('Cluster_ID').isin(['BMIVAL_COD']))
  .dropDuplicates(['ConceptId'])
  .select(f.col('ConceptId').alias('code'))
  .orderBy('code')
)

# Check
print(tmp2_bmi.toPandas().to_string()); print()

# Merge
tmp3_bmi = merge(tmp1_bmi, tmp2_bmi, ['code'], validate='1:1', assert_results=['both', 'left_only'])

# Check
print(tmp3_bmi.toPandas().to_string()); print()

# Prepare
codelist_bmi = (
  tmp3_bmi
  .withColumn('name', f.lit('bmi'))
  .withColumn('terminology', f.lit('SNOMED'))
  .select('name', 'terminology', 'code', 'term')
)  

# Check
tmpt = tab(codelist_bmi, 'name', 'terminology'); print()
print(codelist_bmi.orderBy('name', 'terminology', 'code').toPandas().to_string()); print()

# COMMAND ----------

# DBTITLE 1,Serum potassium
list_k = [
    '1000651000000109', # Serum potassium level
    '1012671000000103', # Blood potassium level
    '1017401000000106'  # Plasma potassium level
]

tmp1_k = (
  gdppr_refset
  .select('ConceptId', 'ConceptId_description')
  .where(f.col('ConceptId').isin(list_k))
  .dropDuplicates(['ConceptId'])
  .select(f.col('ConceptId').alias('code'), f.col('ConceptId_description').alias('term'))
  .orderBy('code')
)

# Prepare
codelist_k = (
  tmp1_k
  .withColumn('name', f.lit('potassium'))
  .withColumn('terminology', f.lit('SNOMED'))
  .select('name', 'terminology', 'code', 'term')
)  

# Check
tmpt = tab(codelist_k, 'name', 'terminology'); print()
print(codelist_k.orderBy('name', 'terminology', 'code').toPandas().to_string()); print()

# COMMAND ----------

# DBTITLE 1,NYHA
list_nyha = [
    '420300004', # New York Heart Association Classification - Class I (finding)
    '421704003', # New York Heart Association Classification - Class II (finding)
    '420913000', # New York Heart Association Classification - Class III (finding)
    '422293003'  # New York Heart Association Classification - Class IV (finding)
]

tmp1_nyha = (
  gdppr_refset
  .select('ConceptId', 'ConceptId_description')
  .where(f.col('ConceptId').isin(list_nyha))
  .dropDuplicates(['ConceptId'])
  .select(f.col('ConceptId').alias('code'), f.col('ConceptId_description').alias('term'))
  .orderBy('code')
)

# Prepare
codelist_nyha = (
  tmp1_nyha
  .withColumn('name', f.lit('nyha'))
  .withColumn('terminology', f.lit('SNOMED'))
  .select('name', 'terminology', 'code', 'term')
)  

# Check
tmpt = tab(codelist_nyha, 'name', 'terminology'); print()
print(codelist_nyha.orderBy('name', 'terminology', 'code').toPandas().to_string()); print()

# COMMAND ----------

# DBTITLE 1,Systolic blood pressure
tmp1_sbp = (
  gdppr_refset
  .where(f.col('Cluster_ID') == 'BP_COD')
  .dropDuplicates(['ConceptId'])
  .select('ConceptId', 'ConceptId_description')
  .withColumn('flag_sys', f.when(f.lower(f.col('ConceptId_description')).rlike('systolic'), 1).otherwise(0))
  .withColumn('flag_dia', f.when(f.lower(f.col('ConceptId_description')).rlike('diastolic'), 1).otherwise(0))
  .withColumn('flag', 
              f.when(f.col('flag_sys') == 1, 'sys')
              .when(f.col('flag_dia') == 1, 'dia')
              .otherwise('gen')
             )
)

# check
tmpt = tab(tmp1_sbp, 'flag_sys', 'flag_dia'); print()
assert tmp1_sbp.where((f.col('flag_sys') == 1) & (f.col('flag_dia') == 1)).count() == 0
tmpt = tab(tmp1_sbp, 'flag'); print()
print(tmp1_sbp.drop('flag_sys', 'flag_dia').orderBy('ConceptId').toPandas().to_string()); print()

# filter
tmp2_sbp = (
  tmp1_sbp
  .where(f.col('flag').isin(['sys', 'gen']))
  .drop('flag_sys', 'flag_dia')
)

# check
tmpt = tab(tmp2_sbp, 'flag'); print()

# prepare
codelist_sbp = (
  tmp2_sbp
  .withColumn('name', f.lit('sbp'))
  .withColumn('terminology', f.lit('SNOMED'))
  .select('name', 'terminology', f.col('ConceptId').alias('code'), f.col('ConceptId_description').alias('term'))
)  

# check
tmpt = tab(codelist_sbp, 'name'); print()
print(codelist_sbp.orderBy('name', 'terminology', 'code').toPandas().to_string()); print()

# COMMAND ----------

# DBTITLE 1,Others (cholesterol, eGFR)
list_cod = ['CHOL2_COD', 'HDLCCHOL_COD', 'IFCCHBAM_COD', 'EGFR_COD']
tmp_meas = (
  gdppr_refset
  .select('Cluster_ID', 'Cluster_Desc', 'ConceptId', 'ConceptId_description')
  .where(f.col('Cluster_ID').isin(list_cod))
  .withColumn('name',
    f.when(f.col('Cluster_ID') == 'CHOL2_COD',    f.lit('tchol'))
     .when(f.col('Cluster_ID') == 'HDLCCHOL_COD', f.lit('hdl'))
     .when(f.col('Cluster_ID') == 'IFCCHBAM_COD', f.lit('hba1c'))
     .when(f.col('Cluster_ID') == 'EGFR_COD',     f.lit('egfr'))
     .otherwise(f.col('Cluster_ID'))
  )
  .withColumn('terminology', f.lit('SNOMED'))
)  
  
# Check  
tmpt = tab(tmp_meas, 'name', 'Cluster_ID'); print()
with pd.option_context('display.max_colwidth', None): 
  tmpt = tab(tmp_meas, 'Cluster_ID', 'Cluster_Desc', var2_wide=0); print()
tmpt = tab(tmp_meas, 'name', 'terminology'); print()
print(tmp_meas.drop('Cluster_ID', 'Cluster_Desc').orderBy('name', 'terminology', 'ConceptId').toPandas().to_string()); print()  
    
# Prepare
codelist_meas = (
  tmp_meas
  .select('name', 'terminology', f.col('ConceptId').alias('code'), f.col('ConceptId_description').alias('term'))
)
 
# Check
tmpt = tab(codelist_meas, 'name', 'terminology'); print()
print(codelist_meas.orderBy('name', 'terminology', 'code').toPandas().to_string()); print()

# COMMAND ----------

# Serum creatinine
list_scr = [
    '1000731000000107',	# Serum creatinine level (observable entity)
    '1000991000000106',	# Corrected serum creatinine level (observable entity)
    '1000981000000109',	# Corrected plasma creatinine level (observable entity)
    '1001011000000107',	# Plasma creatinine level (observable entity)
    '1106601000000100',	# Substance concentration of creatinine in plasma (observable entity)
    '1107001000000108',	# Substance concentration of creatinine in serum (observable entity)
    '1109421000000104',	# Substance concentration of creatinine in plasma using colorimetric analysis (observable entity)
    '1109431000000102',	# Substance concentration of creatinine in plasma using enzymatic analysis (observable entity)
    '1109441000000106',	# Substance concentration of creatinine in serum using colorimetric analysis (observable entity)
    '1109451000000109'	# Substance concentration of creatinine in serum using enzymatic analysis (observable entity)
]

tmp1_scr = (
  gdppr_refset
  .select('ConceptId', 'ConceptId_description')
  .where(f.col('ConceptId').isin(list_scr))
  .dropDuplicates(['ConceptId'])
  .select(f.col('ConceptId').alias('code'), f.col('ConceptId_description').alias('term'))
  .orderBy('code')
)

# Prepare
codelist_scr = (
  tmp1_scr
  .withColumn('name', f.lit('scr'))
  .withColumn('terminology', f.lit('SNOMED'))
  .select('name', 'terminology', 'code', 'term')
)  

# Check
tmpt = tab(codelist_scr, 'name', 'terminology'); print()
print(codelist_scr.orderBy('name', 'terminology', 'code').toPandas().to_string()); print()

# COMMAND ----------

# smoking_(current|ex|never)
# 20220707 updated 6 smoking_ex SNOMED codes that had been rounded - tb
# 20230505 below notes relate to codelists excel spreadsheet - fc
# 20230505 removed current status for 160625004 (now only has ex status) - fc
# 20230505 removed ex status for 230058003 (now only has current status) - fc
# 20230505 removed ex status for 230065006 (now only has current status) - fc
# 20230505 removed ex status for 160625004 (now only has current status) - fc

tmp_smoking_status = spark.createDataFrame(
  [
    ("160603005","Light cigarette smoker (1-9 cigs/day) (finding)","Current-smoker","Light"),
    ("160606002","Very heavy cigarette smoker (40+ cigs/day) (finding)","Current-smoker","Heavy"),
    ("160613002","Admitted tobacco consumption possibly untrue (finding)","Current-smoker","Unknown"),
    ("160619003","Rolls own cigarettes (finding)","Current-smoker","Unknown"),
    ("230056004","Cigarette consumption (observable entity)","Current-smoker","Unknown"),
    ("230057008","Cigar consumption (observable entity)","Current-smoker","Unknown"),
    ("230058003","Pipe tobacco consumption (observable entity)","Current-smoker","Unknown"),
    ("230060001","Light cigarette smoker (finding)","Current-smoker","Light"),
    ("230062009","Moderate cigarette smoker (finding)","Current-smoker","Moderate"),
    ("230065006","Chain smoker (finding)","Current-smoker","Heavy"),
    ("266918002","Tobacco smoking consumption (observable entity)","Current-smoker","Unknown"),
    ("446172000","Failed attempt to stop smoking (finding)","Current-smoker","Unknown"),
    ("449868002","Smokes tobacco daily (finding)","Current-smoker","Unknown"),
    ("56578002","Moderate smoker (20 or less per day) (finding)","Current-smoker","Moderate"),
    ("56771006","Heavy smoker (over 20 per day) (finding)","Current-smoker","Heavy"),
    ("59978006","Cigar smoker (finding)","Current-smoker","Unknown"),
    ("65568007","Cigarette smoker (finding)","Current-smoker","Unknown"),
    ("134406006","Smoking reduced (finding)","Current-smoker","Unknown"),
    ("160604004","Moderate cigarette smoker (10-19 cigs/day) (finding)","Current-smoker","Moderate"),
    ("160605003","Heavy cigarette smoker (20-39 cigs/day) (finding)","Current-smoker","Heavy"),
    ("160612007","Keeps trying to stop smoking (finding)","Current-smoker","Unknown"),
    ("160616005","Trying to give up smoking (finding)","Current-smoker","Unknown"),
    ("203191000000107","Wants to stop smoking (finding)","Current-smoker","Unknown"),
    ("225934006","Smokes in bed (finding)","Current-smoker","Unknown"),
    ("230059006","Occasional cigarette smoker (finding)","Current-smoker","Light"),
    ("230063004","Heavy cigarette smoker (finding)","Current-smoker","Heavy"),
    ("230064005","Very heavy cigarette smoker (finding)","Current-smoker","Heavy"),
    ("266920004","Trivial cigarette smoker (less than one cigarette/day) (finding)","Current-smoker","Light"),
    ("266929003","Smoking started (finding)","Current-smoker","Unknown"),
    ("308438006","Smoking restarted (finding)","Current-smoker","Unknown"),
    ("394871007","Thinking about stopping smoking (finding)","Current-smoker","Unknown"),
    ("394872000","Ready to stop smoking (finding)","Current-smoker","Unknown"),
    ("394873005","Not interested in stopping smoking (finding)","Current-smoker","Unknown"),
    ("401159003","Reason for restarting smoking (observable entity)","Current-smoker","Unknown"),
    ("413173009","Minutes from waking to first tobacco consumption (observable entity)","Current-smoker","Unknown"),
    ("428041000124106","Occasional tobacco smoker (finding)","Current-smoker","Light"),
    ("77176002","Smoker (finding)","Current-smoker","Unknown"),
    ("82302008","Pipe smoker (finding)","Current-smoker","Unknown"),
    ("836001000000109","Waterpipe tobacco consumption (observable entity)","Current-smoker","Unknown"),
    ("160603005","Light cigarette smoker (1-9 cigs/day) (finding)","Current-smoker","Light"),
    ("160612007","Keeps trying to stop smoking (finding)","Current-smoker","Unknown"),
    ("160613002","Admitted tobacco consumption possibly untrue (finding)","Current-smoker","Unknown"),
    ("160616005","Trying to give up smoking (finding)","Current-smoker","Unknown"),
    ("160619003","Rolls own cigarettes (finding)","Current-smoker","Unknown"),
    #("160625004","Date ceased smoking (observable entity)","Current-smoker","Unknown"),
    ("225934006","Smokes in bed (finding)","Current-smoker","Unknown"),
    ("230056004","Cigarette consumption (observable entity)","Current-smoker","Unknown"),
    ("230057008","Cigar consumption (observable entity)","Current-smoker","Unknown"),
    ("230059006","Occasional cigarette smoker (finding)","Current-smoker","Light"),
    ("230060001","Light cigarette smoker (finding)","Current-smoker","Light"),
    ("230062009","Moderate cigarette smoker (finding)","Current-smoker","Moderate"),
    ("230063004","Heavy cigarette smoker (finding)","Current-smoker","Heavy"),
    ("230064005","Very heavy cigarette smoker (finding)","Current-smoker","Heavy"),
    ("266920004","Trivial cigarette smoker (less than one cigarette/day) (finding)","Current-smoker","Light"),
    ("266929003","Smoking started (finding)","Current-smoker","Unknown"),
    ("394872000","Ready to stop smoking (finding)","Current-smoker","Unknown"),
    ("401159003","Reason for restarting smoking (observable entity)","Current-smoker","Unknown"),
    ("449868002","Smokes tobacco daily (finding)","Current-smoker","Unknown"),
    ("65568007","Cigarette smoker (finding)","Current-smoker","Unknown"),
    ("134406006","Smoking reduced (finding)","Current-smoker","Unknown"),
    ("160604004","Moderate cigarette smoker (10-19 cigs/day) (finding)","Current-smoker","Moderate"),
    ("160605003","Heavy cigarette smoker (20-39 cigs/day) (finding)","Current-smoker","Heavy"),
    ("160606002","Very heavy cigarette smoker (40+ cigs/day) (finding)","Current-smoker","Heavy"),
    ("203191000000107","Wants to stop smoking (finding)","Current-smoker","Unknown"),
    ("308438006","Smoking restarted (finding)","Current-smoker","Unknown"),
    ("394871007","Thinking about stopping smoking (finding)","Current-smoker","Unknown"),
    ("394873005","Not interested in stopping smoking (finding)","Current-smoker","Unknown"),
    ("401201003","Cigarette pack-years (observable entity)","Current-smoker","Unknown"),
    ("413173009","Minutes from waking to first tobacco consumption (observable entity)","Current-smoker","Unknown"),
    ("428041000124106","Occasional tobacco smoker (finding)","Current-smoker","Light"),
    ("446172000","Failed attempt to stop smoking (finding)","Current-smoker","Unknown"),
    ("56578002","Moderate smoker (20 or less per day) (finding)","Current-smoker","Moderate"),
    ("56771006","Heavy smoker (over 20 per day) (finding)","Current-smoker","Heavy"),
    ("59978006","Cigar smoker (finding)","Current-smoker","Unknown"),
    ("77176002","Smoker (finding)","Current-smoker","Unknown"),
    ("82302008","Pipe smoker (finding)","Current-smoker","Unknown"),
    ("836001000000109","Waterpipe tobacco consumption (observable entity)","Current-smoker","Unknown"),
    ("53896009","Tolerant ex-smoker (finding)","Ex-smoker","Unknown"),
    ("1092041000000104","Ex-very heavy smoker (40+/day) (finding)","Ex-smoker","Unknown"),
    ("1092091000000109","Ex-moderate smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
    ("160620009","Ex-pipe smoker (finding)","Ex-smoker","Unknown"),
    ("160621008","Ex-cigar smoker (finding)","Ex-smoker","Unknown"),
    ("228486009","Time since stopped smoking (observable entity)","Ex-smoker","Unknown"),
    ("266921000","Ex-trivial cigarette smoker (<1/day) (finding)","Ex-smoker","Unknown"),
    ("266922007","Ex-light cigarette smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
    ("266923002","Ex-moderate cigarette smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
    ("266928006","Ex-cigarette smoker amount unknown (finding)","Ex-smoker","Unknown"),
    ("281018007","Ex-cigarette smoker (finding)","Ex-smoker","Unknown"),
    ("735128000","Ex-smoker for less than 1 year (finding)","Ex-smoker","Unknown"),
    ("8517006","Ex-smoker (finding)","Ex-smoker","Unknown"),
    ("1092031000000108","Ex-smoker amount unknown (finding)","Ex-smoker","Unknown"),
    ("1092071000000105","Ex-heavy smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
    ("1092111000000104","Ex-light smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
    ("1092131000000107","Ex-trivial smoker (<1/day) (finding)","Ex-smoker","Unknown"),
    ("160617001","Stopped smoking (finding)","Ex-smoker","Unknown"),
    ("160625004","Date ceased smoking (observable entity)","Ex-smoker","Unknown"),
    ("266924008","Ex-heavy cigarette smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
    ("266925009","Ex-very heavy cigarette smoker (40+/day) (finding)","Ex-smoker","Unknown"),
    ("360890004","Intolerant ex-smoker (finding)","Ex-smoker","Unknown"),
    ("360900008","Aggressive ex-smoker (finding)","Ex-smoker","Unknown"),
    ("48031000119106","Ex-smoker for more than 1 year (finding)","Ex-smoker","Unknown"),
    ("492191000000103","Ex roll-up cigarette smoker (finding)","Ex-smoker","Unknown"),
    ("53896009","Tolerant ex-smoker (finding)","Ex-smoker","Unknown"),
    ("735112005","Date ceased using moist tobacco (observable entity)","Ex-smoker","Unknown"),
    ("228486009","Time since stopped smoking (observable entity)","Ex-smoker","Unknown"),
    ("266921000","Ex-trivial cigarette smoker (<1/day) (finding)","Ex-smoker","Unknown"),
    ("266923002","Ex-moderate cigarette smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
    ("266928006","Ex-cigarette smoker amount unknown (finding)","Ex-smoker","Unknown"),
    ("360900008","Aggressive ex-smoker (finding)","Ex-smoker","Unknown"),
    ("492191000000103","Ex roll-up cigarette smoker (finding)","Ex-smoker","Unknown"),
    ("735112005","Date ceased using moist tobacco (observable entity)","Ex-smoker","Unknown"),
    ("735128000","Ex-smoker for less than 1 year (finding)","Ex-smoker","Unknown"),
    ("160617001","Stopped smoking (finding)","Ex-smoker","Unknown"),
    ("160620009","Ex-pipe smoker (finding)","Ex-smoker","Unknown"),
    ("160621008","Ex-cigar smoker (finding)","Ex-smoker","Unknown"),
    #("230058003","Pipe tobacco consumption (observable entity)","Ex-smoker","Unknown"),
    #("230065006","Chain smoker (finding)","Ex-smoker","Unknown"),
    #("266918002","Tobacco smoking consumption (observable entity)","Ex-smoker","Unknown"),
    ("266922007","Ex-light cigarette smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
    ("266924008","Ex-heavy cigarette smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
    ("266925009","Ex-very heavy cigarette smoker (40+/day) (finding)","Ex-smoker","Unknown"),
    ("281018007","Ex-cigarette smoker (finding)","Ex-smoker","Unknown"),
    ("360890004","Intolerant ex-smoker (finding)","Ex-smoker","Unknown"),
    ("48031000119106","Ex-smoker for more than 1 year (finding)","Ex-smoker","Unknown"),
    ("8517006","Ex-smoker (finding)","Ex-smoker","Unknown"),
    ("221000119102","Never smoked any substance (finding)","Never-smoker","NA"),
    ("266919005","Never smoked tobacco (finding)","Never-smoker","NA"),
    ("221000119102","Never smoked any substance (finding)","Never-smoker","NA"),
    ("266919005","Never smoked tobacco (finding)","Never-smoker","NA")
  ],
  ['code', 'term', 'smoking_status', 'severity']
)
codelist_smoking_status = (
  tmp_smoking_status
  .distinct()
  .withColumn('name',
    f.when(f.col('smoking_status') == 'Current-smoker', f.lit('smoking_current'))
     .when(f.col('smoking_status') == 'Ex-smoker', f.lit('smoking_ex'))
     .when(f.col('smoking_status') == 'Never-smoker', f.lit('smoking_never'))
     .otherwise(f.col('smoking_status'))
  )
  .withColumn('terminology', f.lit('SNOMED'))
  .select('name', 'terminology', 'code', 'term')
)  

# check
tmpt = tab(codelist_smoking_status, 'name', 'terminology'); print()
print(codelist_smoking_status.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# codes with a Current AND Ex status
tmp1 = (
  tmp_smoking_status
  .select("smoking_status","code")
  .distinct()
  .groupBy("code")
  .pivot("smoking_status").agg(f.lit(1)).na.fill(value=0)
  .withColumn('Current_AND_Ex_smoker',f.when((f.col('Current-smoker') == 1) & (f.col('Ex-smoker') == 1), f.lit(1)).otherwise(f.lit(0)))
  .filter(f.col("Current_AND_Ex_smoker")==1)
  .select("code")
) 

tmp2=tmp_smoking_status.select("code","term").distinct()

# previously 4 codes had a current and ex status but in the chunk they have been commented out as at the note for date 20230505
display(tmp1.join(tmp2,tmp1.code==tmp2.code,"left"))

# COMMAND ----------

# DBTITLE 1,Combine
# Append (union) codelists defined above
# Harmonise columns before appending
codelist_all = []
for indx, clist in enumerate([clist for clist in globals().keys() if (bool(re.match('^codelist_.*', clist))) & (clist not in ['codelist_match', 'codelist_match_summ', 'codelist_match_stages_to_run', 'codelist_match_v2_test', 'codelist_tmp', 'codelist_all'])]):
  print(f'{0 if indx<10 else ""}' + str(indx) + ' ' + clist)
  codelist_tmp = globals()[clist]
  if(indx == 0):
    codelist_all = codelist_tmp
  else:
    # pre unionByName
    for col in [col for col in codelist_tmp.columns if col not in codelist_all.columns]:
      # print('  M - adding column: ' + col)
      codelist_all = codelist_all.withColumn(col, f.lit(None))
    for col in [col for col in codelist_all.columns if col not in codelist_tmp.columns]:
      # print('  C - adding column: ' + col)
      codelist_tmp = codelist_tmp.withColumn(col, f.lit(None))
      
    # unionByName  
    codelist_all = (
      codelist_all
      .unionByName(codelist_tmp)
    )
  
# Order  
codelist = (
  codelist_all
  .orderBy('name', 'terminology', 'code')
)

# COMMAND ----------

display(codelist)

# COMMAND ----------

tmpt = tab(codelist, 'name', 'terminology'); print()

# COMMAND ----------

# DBTITLE 1,Reformat
# Remove trailing X's, decimal points, dashes, and spaces
codelist = (
  codelist
  .withColumn('_code_old', f.col('code'))
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'X$', '')).otherwise(f.col('code')))\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'[\.\-\s]', '')).otherwise(f.col('code')))
  .withColumn('_code_diff', f.when(f.col('code') != f.col('_code_old'), 1).otherwise(0))
)

# Check
tmpt = tab(codelist, '_code_diff'); print()
print(codelist.where(f.col('_code_diff') == 1).orderBy('name', 'terminology', 'code').toPandas().to_string()); print()

# Tidy
codelist = codelist.drop('_code_old', '_code_diff')

# COMMAND ----------

display(codelist.where(f.col('terminology') == 'ICD10'))

# COMMAND ----------

codelist2 = codelist.withColumnRenamed('code', 'snomed_code')
display(codelist2)

# COMMAND ----------

gdppr_hf_joined = gdppr_hf.join(codelist2, on='snomed_code', how='inner')
display(gdppr_hf_joined)

# COMMAND ----------

# DBTITLE 1,Write data
gdppr_hf_joined.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.ccu005_04_gdppr_characteristics_{ver}')