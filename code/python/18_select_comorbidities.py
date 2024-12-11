# Databricks notebook source
# MAGIC %md
# MAGIC # 18_select_comorbidities
# MAGIC
# MAGIC **Description** This notebook joins codelists to the list of patients diagnoses to get comorbidities.
# MAGIC
# MAGIC **Authors** Robert Fletcher

# COMMAND ----------

# DBTITLE 1,Load libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
import re

from pyspark.sql.window import Window as w
from functools import reduce
from pyspark.sql import DataFrame

# COMMAND ----------

# DBTITLE 1,Source functions
# MAGIC %run "/Repos/raf69@cam.ac.uk/ccu005_04/nicor/src/utils"

# COMMAND ----------

# DBTITLE 1,Source comorbidity codelists
# MAGIC %run "/Repos/raf69@cam.ac.uk/ccu005_04/nicor/src/comorbidity_codelists"

# COMMAND ----------

# DBTITLE 1,Source autoimmune disease codelists
# MAGIC %run "/Repos/raf69@cam.ac.uk/ccu005_04/nicor/src/autoimmune_disease_codelists"

# COMMAND ----------

# DBTITLE 1,Define variables
# Databases
live    = 'dars_nic_391419_j3w9t'
archive = 'dars_nic_391419_j3w9t_collab'
new_db  = 'dsa_391419_j3w9t_collab'

# Tables
subclass = 'ccu005_04_incident_hf_subclass_skinny_imd_char'
comorb   = 'ccu005_04_comorbidities_query_data'

# Pipeline version
ver = 'v2'

# COMMAND ----------

# DBTITLE 1,Load data
cohort = load(db_name=new_db, table_name=f'{subclass}_{ver}', db_type='ccu005_04')
dis    = load(db_name=new_db, table_name=f'{comorb}_{ver}', db_type='ccu005_04')

# COMMAND ----------

# check uniqueness
print(cohort.select("subject_id").count())
print(cohort.select("subject_id").distinct().count())

# COMMAND ----------

display(dis)

# COMMAND ----------

def select_comorbidity(data, codelist, comorb_var=str, hf=False, time_sensitive=False):
    if hf == True:
        data = data.where(f.col('admission_date') > f.col('date'))

    if time_sensitive == True:
        data = (
            data
            .withColumn('diff', f.datediff(f.col('admission_date'), f.col('date')))
            .where(f.col('diff') <= 1095.75)
        )

    pats = data.select('subject_id').distinct()
    data = (
        data
        .join(codelist, on='diagnosis_code', how='inner')
        .select('subject_id')
        .distinct()
        .withColumn(comorb_var, f.lit('1'))
    )
    data = pats.join(data, on='subject_id', how='left').withColumn(comorb_var, f.when(f.col(comorb_var).isNull(), 0).otherwise(f.col(comorb_var)))
    return data

# COMMAND ----------

dataframe_columns = {
 "hhf":hf, "hcv19": cv19, "hdb": db, "hckd": ckd, "hcanc": canc, "hdem": dem, "hdep": dep, "hobes": obes, "host": ost, "hthy": thy, "hasthm": asthm, "hcopd": copd, "harryth": arry, "haf": af, "hhtn": htn, "hihd": ihd, "hpad": pad, "hstk": stk, "hanae": anae, "hlivd": livd, "hvalvhd": valvhd, "hautoimmune": autoimmune, "hdicard": dicard
}

# Iterate over DataFrames, filter, and overwrite
for comorb_var in dataframe_columns:
    dataframe_columns[comorb_var] = dataframe_columns[comorb_var].where(f.col('exclude') == 0)

time_sensitive_vars = ['hdep', 'hobes', 'harryth', 'haf']

# Function to perform a left join
def join_dfs(left_df, right_df):
    return left_df.join(right_df, on='subject_id', how='left') 

# Output list
dfs_comorb = []
# Get comorbidities for each subject
for comorb_var, df in dataframe_columns.items(): 
    is_hf = (comorb_var == "hhf")
    is_time_sensitive = (comorb_var in time_sensitive_vars)
    df_comorb = select_comorbidity(data=dis, codelist=df, comorb_var=comorb_var, hf=is_hf, time_sensitive=is_time_sensitive)
    dfs_comorb.append(df_comorb)

comorb_res = reduce(lambda left, right: left.join(right, on='subject_id', how='left'), dfs_comorb)

# COMMAND ----------

# Check
display(comorb_res)

# COMMAND ----------

# DBTITLE 1,Check no cancer codes missing
dis.join(canc, on='diagnosis_code', how='left_anti').where(f.col('diagnosis_code').rlike('^C')).select('diagnosis_code').distinct().show()

# COMMAND ----------

# DBTITLE 1,Join to cohort data
joined = cohort.join(comorb_res, on='subject_id', how='left')
print(cohort.count())
print(joined.count())

# COMMAND ----------

# DBTITLE 1,Write data
joined.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.ccu005_04_incident_hf_subclass_skinny_imd_char_comorb_{ver}')