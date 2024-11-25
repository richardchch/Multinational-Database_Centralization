# Multinational Database Centralization Project

## Table of Contents
1. [Project Overview](#project-overview)
2. [Installation Instructions](#installation-instructions)
3. [Usage Instructions](#usage-instructions)
4. [File Structure](#file-structure)
5. [License](#license)

## Project Overview

The **Data Centralization Project** is designed to extract, clean, and load data from multiple sources into a local PostgreSQL database. The project primarily focuses on handling API data, PDF data, S3 data, and data from a relational database. 

The goal of this project is to automate the extraction, cleaning, and storage of data from various sources such as an external API (for store details), PDF files (for card details), and CSV files from an S3 bucket. After data cleaning, it is uploaded to a PostgreSQL database for further analysis and reporting.

### What the Project Does:
- Extracts data from an AWS RDS in the cloud.
- Extracts data stored in a PDF document in an AWS S3 bucket
- Extracts data from an external API (store details and number of stores).
- Retrieves data from a PDF document containing card information.
- Extracts CSV data from an S3 bucket.
- Cleans data to ensure it's in a usable format (e.g., handling missing values, data type conversion).
- Loads cleaned data into a local PostgreSQL database.

### Aim of the Project:
The primary aim of this project is to automate the end-to-end data pipeline for centralizing data from various sources, processing it, and storing it in a relational database for further use.

### What I Learned:
- How to interact with APIs using Python and handle API authentication with API keys.
- Extracting and cleaning data from various sources (API, PDF, S3).
- How to interact with PostgreSQL databases using SQLAlchemy and pandas.
- Writing robust error handling and logging for data extraction tasks.

## Installation Instructions

To get started with this project, follow the instructions below:

### Prerequisites:
- **Python** (3.x)
- **PostgreSQL** (for local database setup)
- **pip** (Python package installer)

### Steps:
1. **Clone the Repository:**
   ```bash
   git clone https://github.com/your-username/data-centralization-project.git
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





### License

This project is licensed under the MIT License - see the LICENSE file for details.






   

