#*******************************************************************************
#
# Project: CCU005_04
# Title:   Trends and impact of multiple long-term conditions in heart failure
# Date:    11-Mar-2023
# Author:  Robert Fletcher
# Purpose: Import data from database and write to project folder
#
#*******************************************************************************


# Notes -------------------------------------------------------------------

# Open the `nicor` RStudio project to run this code


# Load libraries ----------------------------------------------------------

libs <- c("DBI", "here", "tidyverse")

lapply(libs, library, character.only = TRUE)


# Source functions --------------------------------------------------------

source(here::here("src", "import_data.R"))


# Define variables --------------------------------------------------------

# Database
db <- "dsa_391419_j3w9t_collab"

# Project
prj <- "ccu005_04"

# Tables
tbl <- c(
  "baseline", "cohort_deaths", "cohort_hospitalisations", "confirmed_covid",
  "pillar2_clean", "nicor_characteristics", "gdppr_characteristics",
  "medications", "nhfa_clean", "nicor_combined_lvef",
  "hospitals_nhfa_mapping_percentage"
)

# Pipeline version
ver <- "v2"


# Set-up Databricks connection --------------------------------------------

con <- dbConnect(
  odbc::odbc(), dsn = 'databricks',
  HTTPPath = '',
  PWD = rstudioapi::askForPassword('Please enter Databricks PAT')
)


# Create new sub-directory in data folder ---------------------------------

dir.create(here::here("data", ver), showWarnings = FALSE)


# Import data and write to project folder ---------------------------------

for (t in tbl) {
  import_data(
    .connection = con, .db_name = db, .proj = prj, .table_name = t,
    .version = ver
  )
}
