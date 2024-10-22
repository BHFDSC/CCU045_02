# Databricks notebook source
# MAGIC %md
# MAGIC # 11_combine_skinny
# MAGIC
# MAGIC **Description** This notebook combines the skinny patient table with previously-prepared data.
# MAGIC
# MAGIC **Authors** Robert Fletcher

# COMMAND ----------

# DBTITLE 1,Load libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
import re

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
skinny = 'ccu005_04_skinny_selected'
hf_subclass = 'ccu005_04_incident_hf_subclass'

# Writable table name
write_tbl = 'ccu005_04_hf_patients_skinny'

# Pipeline version
ver = 'v2'

# COMMAND ----------

# DBTITLE 1,Load data
subclass    = load(db_name=new_db, table_name=f'{hf_subclass}_{ver}', db_type='ccu005_04')
skinny_tbl  = load(db_name=new_db, table_name=f'{skinny}_{ver}', db_type='ccu005_04')

# COMMAND ----------

# DBTITLE 1,Reduce skinny table
skinny_rdc = (
    skinny_tbl
    .select(
        f.col('PERSON_ID').alias('subject_id'),
        f.col('DOB').alias('birth_date'),
        f.col('SEX').alias('sex_skinny'),
        f.col('ETHNIC').alias('ethnicity_skinny'),
        f.col('ETHNIC_DESC').alias('ethnicity_desc_skinny'),
        f.col('ETHNIC_CAT').alias('ethnicity_cat_skinny')
    )
    .distinct()
)
display(skinny_rdc)

# COMMAND ----------

# DBTITLE 1,Join data
joined = (
    subclass
    .join(skinny_rdc, on='subject_id', how='left')
    .distinct()
    .withColumn('age', f.datediff(f.col('admission_date'), f.col('birth_date')) / 365.25)
)
display(joined)

# COMMAND ----------

# DBTITLE 1,Write data
joined.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.ccu005_04_incident_hf_subclass_skinny_{ver}')