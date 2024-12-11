# Databricks notebook source
# MAGIC %md
# MAGIC # 20_prepare_deaths
# MAGIC
# MAGIC **Description** This notebook prepares deaths data for use as outcomes.
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
bl  = 'ccu005_04_baseline'
dth = f'deaths_{live}'

# Most recent archive date
date = '2024-03-27'

# Pipeline version
ver = 'v2'

# COMMAND ----------

dates = spark.table('dars_nic_391419_j3w9t_collab.deaths_dars_nic_391419_j3w9t_archive').select(f.col('archived_on')).distinct().sort(f.col('archived_on').desc())
dates.show()

# COMMAND ----------

# DBTITLE 1,Load data
cohort = load(db_name=new_db, table_name=f'{bl}_{ver}', db_type='ccu005_04')
deaths = load(db_name=archive, table_name=dth, db_type='archive', archive_date=date)

# COMMAND ----------

# Diagnosis codes
_cols = [v for v in list(deaths.columns) if re.match(r'^(' + "S_COD" + ')(.*)$', v)]
_cols

# COMMAND ----------

# DBTITLE 1,Select columns
deaths2 = (
    deaths
  #   .withColumn('REG_DATE_OF_DEATH', f.to_date(f.col('REG_DATE_OF_DEATH'), 'yyyyMMdd'))
  #   .withColumn('REG_DATE', f.to_date(f.col('REG_DATE'), 'yyyyMMdd'))
    .select(
        f.col('DEC_CONF_NHS_NUMBER_CLEAN_DEID').alias('subject_id'), 
        f.col('REG_DATE_OF_DEATH').alias('death_date'),
        f.col('REG_DATE').alias('reg_date'),
        f.col('S_UNDERLYING_COD_ICD10').alias('primary_death_code'),
        f.col('S_INJURY_EXTERNAL').alias('injury_external_death_cause'),
        f.col('S_COD_CODE_1').alias('secondary_death_code1'),
        f.col('S_COD_CODE_2').alias('secondary_death_code2'),
        f.col('S_COD_CODE_3').alias('secondary_death_code3'),
        f.col('S_COD_CODE_4').alias('secondary_death_code4'),
        f.col('S_COD_CODE_5').alias('secondary_death_code5'),
        f.col('S_COD_CODE_6').alias('secondary_death_code6'),
        f.col('S_COD_CODE_7').alias('secondary_death_code7'),
        f.col('S_COD_CODE_8').alias('secondary_death_code8'),
        f.col('S_COD_CODE_9').alias('secondary_death_code9'),
        f.col('S_COD_CODE_10').alias('secondary_death_code10'),
        f.col('S_COD_CODE_11').alias('secondary_death_code11'),
        f.col('S_COD_CODE_12').alias('secondary_death_code12'),
        f.col('S_COD_CODE_13').alias('secondary_death_code13'),
        f.col('S_COD_CODE_14').alias('secondary_death_code14'),
        f.col('S_COD_CODE_15').alias('secondary_death_code15')
    )
)

# COMMAND ----------

display(deaths2)

# COMMAND ----------

# DBTITLE 1,Join to cohort data
deaths_c = cohort.select('subject_id').join(deaths2, on='subject_id', how='left')

# COMMAND ----------

display(deaths_c)

# COMMAND ----------

# DBTITLE 1,Write data
deaths_c.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.ccu005_04_cohort_deaths_{ver}')