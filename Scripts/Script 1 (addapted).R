# Import libraries
library(DBI)
library(odbc)
library(stringr)
library(caret)
library(arrow)
library(dplyr)

# Connection
con <- dbConnect(odbc::odbc(),
                 Driver = "***************",
                 Server = "***************",
                 Database = "************",
                 UID = "***********",
                 PWD = "***********",
                 Port = 1433)

# SQL Query
query <- "
SELECT
  f_id_tipo_docto AS Tipo_doc,
  f_nrodocto AS Nro_doc,
  f_fecha AS Fecha,
  f_bodega AS Bodega,
  f_item AS Item,
  f_rowid_item AS Rowid_item,
  f_rowid_item_ext AS Rowid_item_ext,
  f_lista_precios AS Lista_precios,
  f_cant_inv AS Cant_inv,
  f_cant_base AS Cant_base,
  f_tipo_inv AS Tipo_inv,
  f_valor_bruto_local AS Bruto,
  f_valor_neto_local AS Neto,
  f_valor_subtotal_local AS Subtotal
FROM
  BI_T461_1
WHERE
  f_parametro_biable = 1
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

# Convert date: Convert number to text, and then from text to date
datos$Fecha <- as.Date(as.character(datos$Fecha), format = "%Y%m%d")

# Check for duplicate rows (identical in all columns)
sum(duplicated(datos))


# Convert columns to their correct data types
datos <- datos %>%
  mutate(
    Fecha = as.Date(Fecha),
    Nro_doc = as.integer(Nro_doc),
    Item = as.integer(Item),
    Rowid_item = as.integer(Rowid_item),
    Rowid_item_ext = as.integer(Rowid_item_ext),
    Cant_inv = as.numeric(Cant_inv),
    Cant_base = as.numeric(Cant_base),
    Bruto = as.numeric(Bruto),
    Neto = as.numeric(Neto),
    Subtotal = as.numeric(Subtotal)
  )

# Standardize date format
datos <- datos %>%
  mutate(Fecha = format(Fecha, "%Y-%m-%d"))


# Verify consistency between Cant_inv and Cant_base
datos_inconsistentes <- datos %>%
  filter(Cant_inv != Cant_base)

# Show inconsistent records
head(datos_inconsistentes)

# Outlier detection

# Function to detect outliers using IQR
outliers <- function(x) {
  Q1 <- quantile(x, 0.25, na.rm = TRUE)
  Q3 <- quantile(x, 0.75, na.rm = TRUE)
  IQR <- Q3 - Q1
  x < (Q1 - 1.5 * IQR) | x > (Q3 + 1.5 * IQR)
}

# Apply the function to the numeric columns
datos_atipicos <- datos %>%
  mutate(
    Bruto_outlier = outliers(Bruto),
    Neto_outlier = outliers(Neto),
    Subtotal_outlier = outliers(Subtotal)
  )

# Show records with outliers
head(datos_atipicos %>% filter(Bruto_outlier | Neto_outlier | Subtotal_outlier))

# Specify the complete path where you want to save the file
ruta <- "**********************************************"

# Export the cleaned dataset to a .parquet file
write_parquet(datos, ruta)

# Verify that the file was created
file.exists(ruta)