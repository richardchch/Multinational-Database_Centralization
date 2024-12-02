# Multinational Retail Data Centralization Project

## Table of Contents

- [Project Overview](#project-overview)
- [Installation Instructions](#installation-instructions)
- [Usage Instructions](#usage-instructions)
- [Database Schema Changes](#database-schema-changes)
- [SQL Files and Queries](#sql-files-and-queries)
- [File Structure](#file-structure)
- [License](#license)

---

## Project Overview

The Data Centralization Project is designed to extract, clean, and load data from multiple sources into a local PostgreSQL database. The project primarily focuses on handling API data, PDF data, S3 data, and data from a relational database.

The goal of this project is to automate the extraction, cleaning, and storage of data from various sources such as an external API (for store details), PDF files (for card details), and CSV files from an S3 bucket. After data cleaning, it is uploaded to a PostgreSQL database for further analysis and reporting.

### Project Objectives:

This project integrates data from diverse sources, standardizes it, and loads it into a structured PostgreSQL database following a star schema model. Key functionalities include:

- **Data Extraction:**
  - Retrieves data from an AWS RDS database, external APIs, PDF documents, and CSV files stored in an S3 bucket.

- **Data Cleaning and Transformation:**
  - Cleans inconsistent data, such as removing extraneous characters (e.g., ? and £) and handling missing or invalid values.
  - Standardizes data formats by converting columns to appropriate data types (e.g., TEXT to UUID, SMALLINT, NUMERIC, or VARCHAR).
  - Merges duplicate or redundant columns (e.g., consolidating latitude fields in the store_details table).
  - Introduces derived fields for operational insights (e.g., `weight_class` in the products table to categorize items by delivery weight).

- **Database Normalization:**
  - Implements a star schema by creating dimension tables (`dim_*`) with clearly defined primary keys.
  - Establishes foreign key constraints in the `orders_table` to reference related records in dimension tables, ensuring data integrity and referential consistency.

- **Database Optimization:**
  - Aligns database design with best practices for analytics and reporting by ensuring efficient data retrieval and minimizing redundancy.

This pipeline provides a robust foundation for centralized data storage, ensuring the data is clean, consistent, and optimized for downstream analysis and business intelligence.

### Project Aim:

The primary aim of this project is to automate the end-to-end data pipeline for centralizing data from various sources, processing it, and storing it in a relational database for further use.

### Insights Gained:

This project provided valuable hands-on experience in designing and implementing a data pipeline for a centralized database. Key takeaways include:

- **Data Integration Techniques:**
  - Extracting data from diverse sources, such as AWS RDS, APIs, S3 buckets, and PDF files, and consolidating it into a unified PostgreSQL database.
  - Leveraging Python libraries and SQL for seamless data retrieval, transformation, and storage.

- **Data Cleaning and Transformation:**
  - Handling inconsistent data formats by identifying and resolving issues such as invalid characters, redundant columns, and missing values.
  - Applying robust data type conversions to standardize datasets (e.g., converting TEXT to UUID, NUMERIC, or VARCHAR).
  - Creating derived fields for actionable insights (e.g., categorizing product weights into delivery-friendly classes).

- **Database Design and Normalization:**
  - Implementing a star schema with well-defined dimension tables (`dim_*`) to normalize the database structure.
  - Establishing primary and foreign key relationships to ensure referential integrity and efficient data querying.

- **Advanced SQL Skills:**
  - Writing SQL scripts to automate complex data transformations and enforce schema constraints.
  - Optimizing database operations by adhering to industry best practices for column data types, indexing, and normalization.

- **End-to-End Pipeline Development:**
  - Building an automated, scalable data pipeline capable of handling diverse data sources, transforming raw data into a clean, analytics-ready state.
  - Ensuring data consistency, reliability, and usability across all stages of the pipeline.

This project enhances technical proficiency in data engineering, database management, and pipeline automation, equipping one with practical skills for real-world data processing challenges.

---



## Installation Instructions

To get started with this project, follow the instructions below:

### Prerequisites:
- **Python** (3.x)
- **PostgreSQL** (for local database setup)
- **pip** (Python package installer)

### Steps:
1. **Clone the Repository:**
   ```bash
   git clone https://github.com/richardchch/Multinational-Retail-Data-Centralization.git
   cd data-centralization-project

2. **Install Dependencies: Create a virtual environment and activate it (optional, but recommended):**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   
   pip install -r requirements.txt

3. **Setup PostgreSQL Database:**
- Install PostgreSQL locally or use a remote PostgreSQL service.
- Create a database called sales_data in PostgreSQL.

4. **Set up AWS Credentials (for S3 access):**
- Install AWS CLI and configure your credentials if you're accessing an S3 bucket.
- Ensure that the S3 bucket and the file you need are accessible.

### Usage Instructions
Run the Project: Execute the main.py script to run the entire pipeline:
   
    `python main.py`

**How the Data Is Processed:**
- Store Data: The project fetches store data from the API, cleans it, and stores it in a local PostgreSQL table.
- Card Details: Extracts data from a provided PDF link and cleans it before uploading it to the database.
- Product Details: Extracts a CSV file from an S3 bucket, processes it, and uploads it to the database.


### Database Schema Changes

#### Orders Table Changes
To align the `orders_table` with the normalized star schema, the following changes were made:

- **Column Data Type Changes:**
  - `date_uuid`: `TEXT` → `UUID`
  - `user_uuid`: `TEXT` → `UUID`
  - `card_number`: `TEXT` → `VARCHAR(19)`
  - `store_code`: `TEXT` → `VARCHAR(12)`
  - `product_code`: `TEXT` → `VARCHAR(11)`
  - `product_quantity`: `BIGINT` → `SMALLINT`

---

#### Primary Keys Added in the `dim_*` Tables
Primary keys were added in the `dim_*` tables to match the respective columns in the `orders_table`:

- `date_uuid` → `dim_date`
- `user_uuid` → `dim_users`
- `card_number` → `dim_card_details`
- `store_code` → `dim_store_details`
- `product_code` → `dim_products`

---

#### Foreign Key Constraints
Foreign key constraints were established in the `orders_table` to reference the primary keys from the `dim_*` tables.

---

#### Users Table Changes
The following changes were made in the `dim_users` table:

- **Column Data Type Changes:**
  - `first_name`: `TEXT` → `VARCHAR(255)`
  - `last_name`: `TEXT` → `VARCHAR(255)`
  - `date_of_birth`: `TEXT` → `DATE`
  - `country_code`: `TEXT` → `VARCHAR(3)`
  - `user_uuid`: `TEXT` → `UUID`
  - `join_date`: `TEXT` → `DATE`

---

#### Store Details Table Changes
- **Latitude Columns Merge:**  
  Merged the two latitude columns into one and set the correct data type for `latitude` and `longitude`.

- **Column Data Type Changes:**
  - `longitude`: `TEXT` → `NUMERIC`
  - `locality`: `TEXT` → `VARCHAR(255)`
  - `store_code`: `TEXT` → `VARCHAR(12)`
  - `staff_numbers`: `TEXT` → `SMALLINT`
  - `opening_date`: `TEXT` → `DATE`
  - `store_type`: `TEXT` → `VARCHAR(255) NULLABLE`
  - `latitude`: `TEXT` → `NUMERIC`
  - `country_code`: `TEXT` → `VARCHAR(3)`
  - `continent`: `TEXT` → `VARCHAR(255)`

---

#### Product Table Changes
- **Product Price:** Removed the pound character (£) from the `product_price` column and converted it to `NUMERIC`.

- **Weight Column:**  
  Added a new column `weight_class` based on the weight range:
  - `Light`: `< 2 kg`
  - `Mid_Sized`: `>= 2 kg and < 40 kg`
  - `Heavy`: `>= 40 kg and < 140 kg`
  - `Truck_Required`: `>= 140 kg`

- **Column Data Type Changes:**
  - `product_price`: `TEXT` → `NUMERIC`
  - `weight`: `TEXT` → `NUMERIC`
  - `EAN`: `TEXT` → `VARCHAR(13)`
  - `product_code`: `TEXT` → `VARCHAR(11)`
  - `date_added`: `TEXT` → `DATE`
  - `uuid`: `TEXT` → `UUID`
  - `still_available`: `TEXT` → `BOOL`
  - `weight_class`: `TEXT` → `VARCHAR(20)`

---

#### Date Table Changes
The following changes were made in the `dim_date_times` table:

- **Column Data Type Changes:**
  - `month`: `TEXT` → `VARCHAR(2)`
  - `year`: `TEXT` → `VARCHAR(4)`
  - `day`: `TEXT` → `VARCHAR(2)`
  - `time_period`: `TEXT` → `VARCHAR(10)`
  - `date_uuid`: `TEXT` → `UUID`

---

#### Card Details Table Changes
The following changes were made in the `dim_card_details` table:

- **Column Data Type Changes:**
  - `card_number`: `TEXT` → `VARCHAR(19)`
  - `expiry_date`: `TEXT` → `VARCHAR(5)`
  - `date_payment_confirmed`: `TEXT` → `DATE`


## SQL Files and Queries

This section details the SQL operations performed as part of the **Data Querying** process, divided into two categories: **Creating the Database Schema** and **Querying the Data**. The actual SQL queries can be found in the corresponding `.sql` files.

### 1. Creating the Database Schema

This section includes SQL queries for setting up the database schema, including table creation, primary and foreign key constraints, and data type definitions. These operations are organized into the following files:

- **`create_schema.sql`**: Contains queries for creating tables and defining relationships between them, including:
  - Creating dimension (`dim_*`) and fact tables.
  - Defining primary keys and foreign keys.
  - Setting data types and constraints.

### 2. Querying the Data

This section includes SQL queries for extracting, aggregating, and analyzing data. These queries support various business intelligence and operational needs, such as identifying top-performing countries, sales trends, and staff distribution.

The queries are organized into the following files:
- **`data_queries.sql`**: Contains queries related to sales performance, including:
  - **Query 1**: Countries with the most physical stores.
  - **Query 2**: Months with the highest sales.
  - **Query 3**: Sales breakdown by location (online vs offline).
  - **Query 4**: Revenue and sales breakdown by store type.
  - **Query 5**: Historical sales data by year and month.
  - **Query 6**: Staff numbers by country.
  - **Query 7**: Store type performance in Germany.
  - **Query 8**: Average time between sales, grouped by year.

Each `.sql` file contains all the queries.

---



### File Structure

**data_centralization_project/**
- **`data_extraction.py`**: Handles extracting data from sources like databases, PDFs, API endpoints, and S3 buckets.
- **`data_cleaning.py`**: Contains methods for cleaning the extracted data (e.g., handling missing values, data type conversions).
- **`database_utils.py`**: Manages database connections and uploading data to a database.
- **`main.py`**: The main script that coordinates the workflow, extracting, cleaning, and uploading data.
- **`requirements.txt`**: Specifies the Python packages needed for the project.
- **`README.md`**: Contains this file structure documentation and project instructions.
- **`LICENSE`**: Legal information and terms for using the project.
- **`db_creds.yaml`**: This file stores your database credentials (should be kept private and not included in the GitHub repository).
- **`sql/`**  Contains SQL scripts for schema creation and querying.
  -**`create_schema.sql`**
  -**`data_queries`.sql**


---

### License

This project is licensed under the MIT License - see the LICENSE file for details.

