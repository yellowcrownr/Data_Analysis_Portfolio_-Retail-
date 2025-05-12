# Import libraries
library(DBI)
library(odbc)
library(stringr)
library(caret)
library(arrow)
library(dplyr)
library(naniar)

# Connection
con <- dbConnect(odbc::odbc(),
                 Driver = "ODBC Driver 17 for SQL Server",
                 Server = "******************",
                 Database = "**********",
                 UID = "*********",
                 PWD = "*************",
                 Port = 1433)

# SQL Query
query <- "
SELECT
  a.f125_rowid_item AS Rowid_item
  ,a.f125_id_plan AS Id_plan
  ,c.f105_descripcion AS Desc_plan
  ,a.f125_id_criterio_mayor AS Id_crit_myr
  ,b.f106_descripcion AS Desc_crit_myr
FROM
  t125_mc_items_criterios a
JOIN
  t106_mc_criterios_item_mayores b ON a.f125_id_plan = b.f106_id_plan AND a.f125_id_criterio_mayor = b.f106_id
JOIN
  t105_mc_criterios_item_planes c ON a.f125_id_plan = c.f105_id
WHERE
  a.f125_id_plan IN ('001', '002', '003', '004', '005', '006')
"

# Send query
res <- dbSendQuery(con, query)

# Download all data
datos <- dbFetch(res, n = -1)  # n = -1 fetches all

# Show the first 6 and last 6 rows of the dataset
head(datos)
tail(datos)

# Dataset information
str(datos)

# Basic statistics of the dataset
summary(datos)

# Number of rows and columns of the dataset
dim(datos)

# Verify if there are missing values in the dataset
any(is.na(datos))

# Count the number of missing values per column
colSums(is.na(datos))

# Remove white spaces from the strings
datos <- datos %>%
  mutate(
    Rowid_item = str_trim(Rowid_item), # Remove spaces at the beginning and end
    Id_plan = str_trim(Id_plan),
    Desc_plan = str_trim(Desc_plan),
    Id_crit_myr = str_trim(Id_crit_myr),
    Desc_crit_myr = str_trim(Desc_crit_myr)
  )

# Convert data types
datos <- datos %>%
  mutate(
    Desc_plan = as.factor(str_trim(Desc_plan)),      # Convert to factor (categorical variable) and remove spaces
    Desc_crit_myr = as.factor(str_trim(Desc_crit_myr)),      # Convert to factor (categorical variable) and remove spaces
  )

# Inspect the levels of the factors
levels(datos$Desc_plan)
levels(datos$Desc_crit_myr)

# Export
# Specify the complete path where you want to save the file
ruta <- "************************"

# Export the cleaned dataset to a .parquet file
write_parquet(datos, ruta)

# Verify that the file was created
file.exists(ruta)
