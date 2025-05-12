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
                 Server = "***************",
                 Database = "**********",
                 UID = "**********",
                 PWD = "***************",
                 Port = 1433)

# SQL Query
query <- "
SELECT
  b.f120_id AS Item
  ,b.f120_rowid AS Rowid_item
  ,a.f121_rowid AS Rowid_item_ext
  ,b.f120_descripcion AS Desc_item
  ,b.f120_id_tipo_inv_serv AS Tipo_inv
  ,a.f121_id_ext1_detalle AS Color
  ,a.f121_id_ext2_detalle AS Talla
  ,a.f121_id_barras_principal AS Barras
FROM
  t121_mc_items_extensiones a
LEFT JOIN
  t120_mc_items b ON a.f121_rowid_item = b.f120_rowid
"

# Send query
res <- dbSendQuery(con, query)

# Download all data
datos <- dbFetch(res, n = -1)  # n = -1 fetches all

# Show the first 6 and last 6 rows of the dataset
head(datos)
tail(datos)

# Basic statistics of the dataset
summary(datos)

# Number of rows and columns of the dataset
dim(datos)

# Verify if there are missing values in the dataset
any(is.na(datos))

# Count the number of missing values per column
sapply(datos, function(x) sum(is.na(x)))

# Handle missing values ("Add n/a to the color and size columns")
datos <- datos %>%
  mutate(
    Color = ifelse(is.na(Color), "n/a", Color),
    Talla = ifelse(is.na(Talla), "n/a", Talla)
  )

# Correct errors and inconsistencies in the data
datos <- datos %>%
  mutate(
    Color = toupper(Color), # Convert to uppercase
    Desc_item = str_to_title(tolower(Desc_item)) # Convert to initial capital letter
  )

# Remove white spaces from the strings
datos <- datos %>%
  mutate(
    Color = str_trim(Color), # Remove spaces at the beginning and end
    Desc_item = str_trim(Desc_item),
    Talla = str_trim(Talla),
    Barras = str_trim(Barras)
  )

# Convert data types
datos <- datos %>%
  mutate(
    Item = as.character(Item),         # Convert to character (text)
    Rowid_item = as.character(Rowid_item), # Convert to character (text)
    Rowid_item_ext = as.character(Rowid_item_ext), # Convert to character (text)
    Desc_item = as.character(Desc_item),   # Ensure that it is character (it already is)
    Tipo_inv = as.factor(str_trim(Tipo_inv)),   # Convert to factor (categorical variable) and remove spaces
    Color = as.factor(str_trim(Color)),      # Convert to factor (categorical variable) and remove spaces
    Talla = as.factor(str_trim(Talla)),      # Convert to factor (categorical variable) and remove spaces
    Barras = as.character(Barras)         # Convert to character (text)
  )

# Inspect the levels of the factors
levels(datos$Tipo_inv)
levels(datos$Color)
levels(datos$Talla)

# Export
# Specify the complete path where you want to save the file
ruta <- "***********************************************************"

# Export the cleaned dataset to a .parquet file
write_parquet(datos, ruta)

# Verify that the file was created
file.exists(ruta)