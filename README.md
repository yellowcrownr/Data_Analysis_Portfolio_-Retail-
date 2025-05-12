# Sales Performance Dashboard for Retail

This project showcases the development of a comprehensive sales reporting system designed for real-time and historical analysis in a retail environment. The system includes interactive dashboards built with Power BI to provide insights into daily and monthly sales performance, enabling data-driven decision-making for store management and strategic planning.

Project Scope

"Provide real-time visibility into daily sales performance across all stores."
"Enable analysis of monthly sales trends and historical performance."
"Facilitate identification of top-performing products and stores."
"Support informed decision-making regarding inventory management and sales strategies."

Daily-Sales-Performance-Dashboard
This dashboard provides a real-time overview of daily sales performance
![Daily-Sales-Performance-Dashboard](Documents/Daily-Sales-Performance-Dashboard.pdf)

Key features:

* Displays current date and key metrics like budget, total sales, and units sold.
* Shows percentage of sales and units achieved against the budget.
* Presents a map visualization of store locations and sales distribution.
* Includes a table with detailed sales data per store.

Monthly Sales Analysis

 - Monthly-Sales-Performance-Dashboard -
This dashboard enables in-depth analysis of monthly sales trends.
![Monthly-Sales-Performance-Dashboard](Documents/Monthly-Sales-Performance-Dashboard.pd)

Key features:

* Provides a summary of monthly sales against budget.
* Visualizes daily sales and unit performance over the month.
* Allows interactive exploration of sales by category for individual stores.
* Presents a table with detailed monthly sales data per store.
* 
 - Script 1 -
This script is used to extract, tranform and load the principal table for sales (extracted from Microsoft SQL Server)
![Script 1](Scripts/Script-1-(addapted).R)

Key Features

1. Database Connection:

* Establishes a connection to a SQL Server database using the odbc library.
* Database credentials (Driver, Server, Database, UID, PWD, Port) are used for authentication.
* Important Note: In a production environment, credentials should never be hardcoded directly into the script. They should be stored securely (environment variables, configuration files, etc.).

2. SQL Query:

* Defines a SQL query to select specific data from the BI_T461_1 table.
* The query selects various columns, including information on sales transactions, products, quantities, and values.
* Filters the data using the condition f_parametro_biable = 1.

3. Data Extraction:

* Executes the SQL query on the database using dbSendQuery() and dbFetch().
* dbFetch(res, n = -1) retrieves all records from the query.

4. Data Inspection:

* Displays the first and last 6 rows of the dataset using head() and tail().
* Provides a statistical summary of the dataset using summary().
* Determines the dimensions of the dataset (number of rows and columns) using dim().
* Checks for missing values in the dataset using any(is.na(datos)) and counts the number of missing values per column using sapply(datos, function(x) sum(is.na(x))).

5. Data Transformation:

* Converts the Fecha column to the Date data type from a numeric format.
* Converts several columns (Nro_doc, Item, etc.) to their appropriate data types (integer or numeric) using mutate().
* Standardizes the format of the Fecha column to "YYYY-MM-DD".

6. Data Consistency Verification:

* Filters the dataset to identify records where the Cant_inv and Cant_base values do not match.
* Displays the first rows of the inconsistent records.

7. Outlier Detection:

* Defines an outliers() function to detect outliers in numeric columns using the interquartile range (IQR) method.
* Applies the function to the Bruto, Neto, and Subtotal columns to identify outlier values.
* Displays the first rows of records containing outliers.

8. Data Export:

* Saves the cleaned and transformed dataset to a Parquet file using write_parquet().
* Parquet is an efficient, columnar file format suitable for large datasets.
* Verifies that the file was created successfully using file.exists().
