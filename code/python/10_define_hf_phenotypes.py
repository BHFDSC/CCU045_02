# Databricks notebook source
# MAGIC %md
# MAGIC # 10_define_hf_phenotypes
# MAGIC
# MAGIC **Description** This notebook uses the National Heart Failure Audit (NHFA), other NICOR cardiovascular audits, and GDPPR primary care data to define phenotypes of heart failure (HFrEF and HFpEF).
# MAGIC
# MAGIC **Authors** Robert Fletcher

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

# DBTITLE 1,Load libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
import re
import sys
import datetime

from pyspark.sql.window import Window as w
from functools import reduce

_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow: {_datetimenow}")

# COMMAND ----------

# DBTITLE 1,Source functions
# MAGIC %run "/Repos/raf69@cam.ac.uk/ccu005_04/nicor/src/utils"

# COMMAND ----------

# MAGIC %run "/Repos/raf69@cam.ac.uk/ccu005_04/nicor/src/map_hf_snomed_codes"

# COMMAND ----------

# DBTITLE 1,Define variables
# Databases
live = 'dars_nic_391419_j3w9t'
archive = 'dars_nic_391419_j3w9t_collab'
new_db = 'dsa_391419_j3w9t_collab'

# Tables
hes_nhfa     = 'ccu005_04_hes_nhfa_linked'
first_hf     = 'ccu005_04_first_hes_apc_hf_record'
nhfa_c       = 'ccu005_04_nhfa_clean'
nicor_lvef   = 'ccu005_04_nicor_combined_lvef'
gdppr_select = 'ccu005_04_gdppr_select'
earliest_rec = 'ccu005_04_earliest_medical_record'

# Writable table name
write_tbl = 'ccu005_04_incident_hf_subclass'

# Pipeline version
ver = 'v2'

# COMMAND ----------

# DBTITLE 1,Load data
combined  = load(db_name=new_db, table_name=f'{hes_nhfa}_{ver}', db_type='ccu005_04')
hist_hf   = load(db_name=new_db, table_name=f'{first_hf}_{ver}', db_type='ccu005_04')
nhfa_comb = load(db_name=new_db, table_name=f'{nhfa_c}_{ver}', db_type='ccu005_04')
lvef      = load(db_name=new_db, table_name=f'{nicor_lvef}_{ver}', db_type='ccu005_04')
gdppr     = load(db_name=new_db, table_name=f'{gdppr_select}_{ver}', db_type='ccu005_04')
rec       = load(db_name=new_db, table_name=f'{earliest_rec}_{ver}', db_type='ccu005_04')

# COMMAND ----------

# DBTITLE 1,Get incident heart failure for 2020 onwards
# Restrict cases to incident heart failure
w2 = w.partitionBy('subject_id').orderBy('spell_id', 'admission_date')
inc = (
    combined
    .filter((f.col('hf_diagnosis_type').rlike('pri|both')) & (f.col('admission_date') != f.col('discharge_date')))
    .withColumn('row', f.row_number().over(w2))
    .filter(f.col('row') == 1)
    # ADDED THIS ON 06 MAY 2024 TO GET COUNTS WHICH DON'T INCLUDE 2023
    .filter(f.col('admission_date') < '2023-01-01')
    .drop("row")
    .cache()
)
# display(inc)
provide_counts(inc, provide_hospitals=True, provide_subtype=True, addition_details='Incident heart failure cases')
# summarise_dates(inc, provide_mapping_coverage=True)

# COMMAND ----------

# DBTITLE 1,Restrict data to individuals with 1 or more years of medical history
# Patients with less than 1 year of prior medical records
inc_1yr_less = inc.join(rec, on='subject_id', how='left').withColumn('diff', f.datediff(f.col('admission_date'), f.col('earliest_date'))).filter(f.col('diff') < 365.25).drop('diff')
provide_counts(inc_1yr_less, provide_hospitals=True, provide_subtype=True, addition_details='Incident heart failure cases with less than 1 year of prior medical records')

# Patients with more than 1 year of prior medical records
inc_1yr_more = inc.join(rec, on='subject_id', how='left').withColumn('diff', f.datediff(f.col('admission_date'), f.col('earliest_date'))).filter(f.col('diff') >= 365.25).drop('diff')
provide_counts(inc_1yr_more, provide_hospitals=True, provide_subtype=True, addition_details='Incident heart failure cases with 1 or more years of prior medical records')

# COMMAND ----------

# DBTITLE 1,Prepare GDPPR heart failure
hf_pc = (
    gdppr
    .join(inc.select('subject_id').distinct(), 'subject_id', how='inner')
    .join(hf_snomed, 'snomed_code', how='inner')
    .filter(f.col('subtype').rlike('HF'))
    .select('subject_id', 'date', 'snomed_code', 'snomed_desc', 'subtype')
    .withColumnRenamed('subtype', 'subtype_pc')
    .cache()
)

# COMMAND ----------

# DBTITLE 1,Count number of hospitalisations/mappings per hospital
sum_all = (
    inc_1yr_more
    # Select relevant columns
    .select(
        'subject_id', 'spell_id', 'hf_diagnosis_type', 'healthcare_provider_name', 'hospital_site_code', 'hospital_site_name', 
        'standardised_name', 'hospital_submits_to_nhfa', 'nicor_id'
    )
    # Filter for non-elective admissions (removed)
    # .filter(f.col('admission_method').rlike('^2'))
)

# Filter for hospital admissions with a primary discharge diagnosis for heart failure
sum_pri = sum_all.filter(f.col('hf_diagnosis_type').rlike('pri|both'))

# Grouping columns
grp_cols = [
    'healthcare_provider_name', 'hospital_site_code', 'hospital_site_name', 
    'standardised_name', 'hospital_submits_to_nhfa'
]

# Total number of hospitalisations with discharge diagnoses in either the primary or secondary position
total_all = sum_all.groupBy(grp_cols).count().withColumnRenamed('count', 'all_diag')

# Total number of hospitalisations with discharge diagnoses in the primary position
total_pri = sum_pri.groupBy(grp_cols).count().withColumnRenamed('count', 'pri_diag')

# Total number of mapped (to NHFA) hospitalisations with discharge diagnoses in the primary position
mapped_pri = sum_pri.filter(f.col('nicor_id').isNotNull()).groupBy(grp_cols).count().withColumnRenamed('count', 'pri_mapped')

# Combine all together
grouped = (
    total_all
    .join(total_pri, grp_cols, how='left')
    .join(mapped_pri, grp_cols, how='left')
    .withColumn('perc_mapped_pri', f.col('pri_mapped') / f.col('pri_diag') * 100)
)
grouped = grouped.sort(grouped.perc_mapped_pri.desc())
# display(grouped)
# get_hospital_statistics(grouped)

# COMMAND ----------

# DBTITLE 1,Restrict hospitals
hosp = (
    grouped
    .filter(f.col('perc_mapped_pri').isNotNull())
    .filter(((f.col('pri_diag') >= 100) & (f.col('perc_mapped_pri') >= 10)) | (f.col('hospital_submits_to_nhfa') == "Y"))
    .select('healthcare_provider_name', 'hospital_site_code', 'hospital_site_name', 'standardised_name', 'hospital_submits_to_nhfa')
)
# get_hospital_statistics(hosp)

# COMMAND ----------

# DBTITLE 1,Join restricted hospitals to incident hospitalisations
rest = inc_1yr_more.join(hosp, ['healthcare_provider_name', 'hospital_site_code', 'hospital_site_name', 'standardised_name', 'hospital_submits_to_nhfa'], how='inner')
# display(rest)
provide_counts(rest, provide_hospitals=True, provide_subtype=True, addition_details='Incident heart failure cases (restricted hospitals)')
# summarise_dates(rest, provide_mapping_coverage=True)

# COMMAND ----------

# DBTITLE 1,Split records with and without phenotype
# Hospitalisations with phenotypes assigned
sub = rest.filter(f.col('hf_subtype').rlike('hfpef|hfref'))
provide_counts(sub, provide_subtype=True, addition_details='Heart failure subtyped by NHFA')
# summarise_dates(sub, provide_mapping_coverage=True)

# Hospitalisations with no phenotypes assigned
no_sub = rest.filter((f.col('hf_subtype').rlike('unspecified')) | (f.col('hf_subtype').isNull()))
provide_counts(no_sub, provide_subtype=True, addition_details='Heart failure NOT subtyped by NHFA')
# summarise_dates(no_sub, provide_mapping_coverage=True)

# COMMAND ----------

# DBTITLE 1,Using NHFA records outside of patients' hospitalisations
window = w.partitionBy('subject_id').orderBy(*[f.asc_nulls_last(c) for c in ['admission_date', 'nhfa_date']])

nhfa_rd = (
    nhfa_comb
    .filter(f.col('hf_subtype').rlike('hfref|hfpef'))
    .select('subject_id', 'nicor_id', f.col('admission_date').alias('nhfa_date'), 'hf_subtype', 'echo_most_recent_date')
    .distinct()
)
no_sub_nhfa_hist = (
    no_sub
    .select('subject_id', 'spell_id', 'admission_date', 'discharge_date')
    .join(nhfa_rd, on='subject_id', how='inner').withColumn('row', f.row_number().over(window))
    .withColumn('diff1', f.abs(f.datediff(f.col('admission_date'), f.col('nhfa_date'))))
    .withColumn('diff2', f.abs(f.datediff(f.col('admission_date'), f.col('echo_most_recent_date'))))
    .withColumn(
        'hf1',
        f.when((f.col('row') == 1) & ((f.col('diff1') <= 183) | (f.col('diff2') <= 183)), f.col('hf_subtype'))
        .when((f.col('row') == 1) & (f.max(f.col('row')).over(window) == 1), f.col('hf_subtype'))
        # More than one prior HFrEF diagnosis
        .when((f.col('row') == 1) & (f.col('nhfa_date') <= f.col('admission_date')) & (f.col('hf_subtype').rlike('hfref')) & (f.lead(f.col('hf_subtype'), 1).over(window).rlike('hfref')), f.col('hf_subtype'))
        # Same day diagnosis, one of which is HFrEF and one is HFpEF (HFrEF takes precedence)
        .when((f.col('row') == 1) & (f.col('hf_subtype').rlike('hfref')) & (f.lead(f.col('hf_subtype'), 1).over(window).rlike('hfpef')) & (f.col('nhfa_date') == f.lead(f.col('nhfa_date'), 1).over(window)), 'hfref')
        .when((f.col('row') == 1) & (f.col('hf_subtype').rlike('hfpef')) & (f.lead(f.col('hf_subtype'), 1).over(window).rlike('hfref')) & (f.col('nhfa_date') == f.lead(f.col('nhfa_date'), 1).over(window)), 'hfref')
        # Patients with two subtypes on the same day
        .when((f.col('row') == 1) & (f.col('hf_subtype').rlike('hfref')) & (f.lead(f.col('hf_subtype'), 1).over(window).rlike('hfref')) & (f.col('nhfa_date') == f.lead(f.col('nhfa_date'), 1).over(window)), 'hfref')
        .when((f.col('row') == 1) & (f.col('hf_subtype').rlike('hfpef')) & (f.lead(f.col('hf_subtype'), 1).over(window).rlike('hfpef')) & (f.col('nhfa_date') == f.lead(f.col('nhfa_date'), 1).over(window)), 'hfpef')
        .otherwise(None)
   )
    .withColumn("hf1", f.last("hf1", ignorenulls=True).over(window))
    .select('subject_id', 'spell_id', f.col('hf1').alias('hf_subtype'))
    .distinct()
)

# display(no_sub_nhfa_hist)
provide_counts(no_sub_nhfa_hist, provide_hospitals=False, provide_subtype=True, addition_details='')

# COMMAND ----------

# DBTITLE 1,Join hospitalisations with no phenotypes to other NICOR audits
# Select rows with identified heart failure phenotype and rename date column so it doesn't clash with primary care later on 
lvef2 = lvef.filter(f.col('subtype').isNotNull()).withColumn('date_nicor', f.to_date('date')).drop('date').withColumnRenamed('subtype', 'subtype_nicor')

# Inner join data to other NICOR audits
no_sub_nicor = (
    no_sub
    .select('subject_id', 'spell_id', 'hf_subtype', 'admission_date', 'discharge_date')
    .distinct()
    .join(no_sub_nhfa_hist.select('subject_id'), on='subject_id', how='left_anti')
    .join(lvef2, on='subject_id', how='inner')
)

# Filter patients whose LVEF in other audits is between admission and discharge dates OR 120 days either side
nicor_samedate = (
    no_sub_nicor
    .withColumn('diff_adm', f.datediff(f.col('admission_date'), f.col('date_nicor')))
    .withColumn('diff_dis', f.datediff(f.col('discharge_date'), f.col('date_nicor')))
    .filter(
        ((f.col('admission_date') <= f.col('date_nicor')) & (f.col('discharge_date') >= f.col('date_nicor'))) |
        (((f.col('diff_dis') >= -120) & (f.col('diff_dis') <= 120)) & ((f.col('diff_adm') >= -120) & (f.col('diff_adm') <= 120)))
    )
    .drop('hf_subtype')
    .withColumnRenamed('subtype_nicor', 'hf_subtype')
)

# Count number of patient records and number of phenotypes identified
nicor_pats = nicor_samedate \
    .select('subject_id', 'hf_subtype') \
    .distinct().groupBy(['subject_id']) \
    .count() \
    .withColumnRenamed('count', 'count_pats')
nicor_subs = nicor_samedate \
    .groupBy(['subject_id', 'spell_id', 'hf_subtype']) \
    .count() \
    .withColumnRenamed('count', 'count_subs')

# Separate individuals with one phenotype identified and those with multiple identified
nicor_samedate_single = nicor_pats \
    .join(nicor_subs, 'subject_id', how='left') \
    .filter(f.col('count_pats') == 1)
nicor_samedate_mult = nicor_pats \
    .join(nicor_subs, 'subject_id', how='left') \
    .filter(f.col('count_pats') > 1) \
    .join(nicor_samedate, ['subject_id', 'spell_id', 'hf_subtype'], 'left').sort('subject_id', 'date_nicor')

provide_counts(nicor_samedate_single, provide_hospitals=False, provide_subtype=True, addition_details='')
# summarise_dates(nicor_samedate_single, provide_mapping_coverage=True)
# display(nicor_samedate_single)

# COMMAND ----------

# DBTITLE 1,Join hospitalisations with no phenotypes to GDPPR
# Get unique identifiers for patients initially phenotyped by NICOR audits
nicor_samedate_pats = nicor_samedate_single.select('subject_id').distinct()

# Inner join data to primary care heart failure
no_sub_pc = (
    no_sub
    .select('subject_id', 'spell_id', 'admission_date', 'discharge_date')
    .distinct()
    .join(no_sub_nhfa_hist.select('subject_id'), on='subject_id', how='left_anti')
    .join(nicor_samedate_pats, on='subject_id', how='left_anti')
    .join(hf_pc, on='subject_id', how='inner')
)
# Show data frame information
provide_counts(no_sub_pc, provide_hospitals=False, provide_subtype=False, addition_details='Individuals with no HF phenotype with primary care HF records')

# COMMAND ----------

# DBTITLE 1,Identify patients with only HFrEF diagnoses (and no HFpEF)
# Partition data by `subject_id` ordering by subtype
window_spec = w.partitionBy('subject_id').orderBy(f.col('subtype_pc')) 
hfref_mult_pc = (
    no_sub_pc
     # Group over columns
    .groupBy(['subject_id', 'spell_id', 'subtype_pc'])
     # Count number of rows for each group
    .count()
    .withColumn('row', f.row_number().over(window_spec)) # Create row column (in cases where patients have HFrEF and HFpEF, HFrEF will be 2)
    .filter((f.col('row') == 1) & (f.col('subtype_pc') == "HFrEF")) # Select patients with only HFrEF with one or more historic diagnoses
    .drop('count', 'row') # Drop columns
)
provide_counts(hfref_mult_pc, provide_hospitals=False, provide_subtype=False, addition_details='Individuals with only HFrEF records in primary care')

# COMMAND ----------

# DBTITLE 1,Identify patients with only HFpEF diagnoses (and no HFrEF)
# Partition data by `subject_id` ordering by subtype (ascending)
window_spec = w.partitionBy('subject_id').orderBy(f.col('subtype_pc').desc()) 
hfpef_mult_pc = (
    no_sub_pc
     # Group over columns
    .groupBy(['subject_id', 'spell_id', 'subtype_pc'])
     # Count number of rows for each group
    .count()
    .withColumn('row', f.row_number().over(window_spec)) # Create row column (in cases where patients have HFrEF and HFpEF, HFpEF will be 1)
    .filter((f.col('row') == 1) & (f.col('subtype_pc') == "HFpEF")) # Select HFpEF with one or more historic diagnoses
    .drop('count', 'row') # Drop columns
)
# provide_counts(hfpef_mult_pc, provide_hospitals=False, provide_subtype=False, addition_details='Individuals with only HFpEF records in primary care')

# COMMAND ----------

# DBTITLE 1,Assign phenotype to patients with multiple diagnoses
#  Subject identifiers for people with HFrEF and HFpEF in primary care
hfref_pat_pc = hfref_mult_pc.select('subject_id').distinct()
hfpef_pat_pc = hfpef_mult_pc.select('subject_id').distinct()

# Define row number for each patient (ordering by GP visit and NICOR date)
window = w.partitionBy('subject_id').orderBy(*[f.asc_nulls_last(c) for c in ['date']])

# Identify people with both HFrEF and HFpEF records in primary care
mult_diag_pc = (
    no_sub_pc
    # Antijoin patients with phenotypes identified
    .join(hfref_pat_pc, 'subject_id', how='left_anti')
    .join(hfpef_pat_pc, 'subject_id', how='left_anti')
    .distinct()
    .withColumn('row', f.row_number().over(window))
    .withColumn('diff', f.datediff(f.col('admission_date'), f.col('date')))
    .withColumn('diff', f.abs(f.col('diff')))
)

# Filter for HFrEF diagnoses in primary care within 14 days of the hospitalisation
mult_diag_ref1 = mult_diag_pc \
    .filter((f.col('subtype_pc') == 'HFrEF') & (f.col('diff') <= 14)) \
    .select('subject_id', 'spell_id', 'admission_date', 'discharge_date') \
    .distinct() \
    .withColumn('subtype_pc', f.lit('hfref'))

# Anti-join to remove these patients with HFrEF within 14 days
mult_diag_pc2 = mult_diag_pc \
    .join(mult_diag_ref1, ['subject_id', 'spell_id', 'admission_date', 'discharge_date'], how='left_anti')

# Get primary care records which are closest to admission date (if both HFrEF and HFpEF are the same distance apart, prefer HFrEF)
w_pc = w.partitionBy('subject_id')
w_pc2 = w.partitionBy('subject_id').orderBy(f.col('subtype_pc').desc()) 
mult_diag_pc3 = mult_diag_pc2 \
    .withColumn('min', f.min('diff').over(w_pc)) \
    .where(f.col('diff') == f.col('min')) \
    .drop('min') \
    .select('subject_id', 'spell_id', 'admission_date', 'discharge_date', 'subtype_pc') \
    .distinct() \
    .withColumn('order', f.row_number().over(w_pc2)) \
    .filter(f.col('order') == 1) \
    .drop('order')

# Separate HFrEF and HFpEF
mult_diag_ref2 = mult_diag_pc3.filter(f.col('subtype_pc') == 'HFrEF')
mult_diag_pef = mult_diag_pc3.filter(f.col('subtype_pc') == 'HFpEF')

# Combine data
dfs = [mult_diag_ref1, mult_diag_ref2, mult_diag_pef]
mult_diag_pc_comb = reduce(f.DataFrame.unionByName, dfs) 

# REMOVING THE NEED FOR THIS
# Multiple diags with LVEF in other NICOR data
mult_diag_lvef = mult_diag_pc.join(lvef2, 'subject_id', how='inner').withColumn('row', f.row_number().over(window))
# Multiple diags with no LVEF in other NICOR data
# mult_diag_nolvef = mult_diag_pc.join(lvef2, 'subject_id', how='left').withColumn('row', f.row_number().over(window))

provide_counts(mult_diag_lvef, provide_hospitals=False, provide_subtype=False, addition_details='')

# COMMAND ----------

# Function for defining distinct patients
def get_patients(data):
   pats = data.select('subject_id').distinct()
   return pats

no_sub2 = (
    no_sub
    .join(get_patients(no_sub_nhfa_hist), 'subject_id', how='left_anti')
    .join(get_patients(nicor_samedate_single), 'subject_id', how='left_anti')
    .join(get_patients(hfref_mult_pc), 'subject_id', how='left_anti')
    .join(get_patients(hfpef_mult_pc), 'subject_id', how='left_anti')
    .join(get_patients(mult_diag_pc_comb), 'subject_id', how='left_anti')  
    .select('subject_id', 'spell_id', 'admission_date', 'discharge_date')
    .distinct()
    .join(lvef2, 'subject_id', how='inner')
    # .cache()
)

# COMMAND ----------

# Define row number for each patient (ordering by admission date and NICOR date)
window = w.partitionBy('subject_id').orderBy(*[f.asc_nulls_last(c) for c in ['admission_date', 'date_nicor']])

no_sub2_prox = (
    no_sub2
    .withColumn('row', f.row_number().over(window))
    .withColumn('diff_adm', f.datediff(f.col('date_nicor'), f.col('admission_date')))
    .withColumn('diff_dis', f.datediff(f.col('date_nicor'), f.col('discharge_date')))
    .withColumn('exclude', f.when((((f.col('diff_dis') >= -120) & (f.col('diff_dis') <= 120)) & ((f.col('diff_adm') >= -120) & (f.col('diff_adm') <= 120))), 0).otherwise(1))
    # .filter((f.col('diff') >= -120) & (f.col('diff') <= 120 ))
)

# Remove patients that only have records which are 120 days outside of the admission
excl_pats = no_sub2_prox.filter(f.col('exclude') == 1).select('subject_id').distinct()
no_sub2_prox2 = no_sub2_prox.join(excl_pats, on='subject_id', how='left_anti')

# COMMAND ----------

# DBTITLE 1,Join all together
# Select columns to join
sub_join = sub.select('subject_id', 'spell_id', 'nicor_id', 'hf_subtype').withColumn('subclass_source', f.lit('nhfa'))
no_sub_nhfa_join = no_sub_nhfa_hist.withColumn('nicor_id', f.lit('')).withColumn('subclass_source', f.lit('nhfa (hist)'))
nicor_samedate_join = nicor_samedate_single.select('subject_id', 'spell_id', 'hf_subtype').withColumn('nicor_id', f.lit('')).withColumn('subclass_source', f.lit('nicor'))
hfref_mult_pc_join = hfref_mult_pc.withColumnRenamed('subtype_pc', 'hf_subtype').select('subject_id', 'spell_id', 'hf_subtype').withColumn('nicor_id', f.lit('')).withColumn('subclass_source', f.lit('gdppr'))
hfpef_mult_pc_join = hfpef_mult_pc.withColumnRenamed('subtype_pc', 'hf_subtype').select('subject_id', 'spell_id', 'hf_subtype').withColumn('nicor_id', f.lit('')).withColumn('subclass_source', f.lit('gdppr'))
mult_diag_pc_join = mult_diag_pc_comb.withColumnRenamed('subtype_pc', 'hf_subtype').select('subject_id', 'spell_id', 'hf_subtype').withColumn('nicor_id', f.lit('')).withColumn('subclass_source', f.lit('gdppr'))

dfs = [sub_join, nicor_samedate_join, no_sub_nhfa_join, hfref_mult_pc_join, hfpef_mult_pc_join, mult_diag_pc_join]
subclass = reduce(f.DataFrame.unionByName, dfs)

final = rest \
    .select('subject_id', 'spell_id', 'admission_date', 'discharge_date', 'admission_method') \
    .distinct() \
    .join(subclass, on=['subject_id', 'spell_id'], how='left') \
    .withColumn('hf_subtype', f.lower(f.col('hf_subtype')))
provide_counts(final, provide_hospitals=False, provide_subtype=True, addition_details='')    

# COMMAND ----------

# DBTITLE 1,Write data
final.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{new_db}.{write_tbl}_{ver}')