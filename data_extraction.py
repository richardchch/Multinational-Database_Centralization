#%%
import pandas as pd
from sqlalchemy import inspect
import requests
from io import StringIO
import boto3
from tabula import read_pdf

class DataExtractor:
    def __init__(self, engine):
        """
        Initialize the DataExtractor with a database engine.

        Args:
            engine (sqlalchemy.engine.Engine): The SQLAlchemy engine for the database.
        """
        self.engine = engine

    def list_db_tables(self):
        """
        Lists all the tables in the database.

        Returns:
            list: A list of table names.
        """
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            print(f"Tables in the database: {tables}")
            return tables
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []

    def read_table(self, table_name):
        """
        Reads data from a specified table in the database and returns it as a Pandas DataFrame.

        Args:
            table_name (str): The name of the table to read.

        Returns:
            pd.DataFrame: The table data as a DataFrame.
        """
        try:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, self.engine)
            print(f"Data from table '{table_name}' read successfully.")
            return df
        except Exception as e:
            print(f"Error reading table '{table_name}': {e}")
            return pd.DataFrame()

    def retrieve_stores_data(self, total_stores, base_url, headers):
        """
        Retrieves details for all stores from the API and returns them as a DataFrame.

        Args:
            total_stores (int): Total number of stores.
            base_url (str): The API base endpoint for retrieving store details.
            headers (dict): The API headers containing the API key.

        Returns:
            pd.DataFrame: A DataFrame containing details of all stores.
        """
        try:
            print(f"Retrieving store details for {total_stores} stores.")
            store_details = []
            for store_number in range(1, total_stores + 1):
                store_url = f"{base_url}/{store_number}"
                store_data = self.get_store_details(store_url, headers)
                if store_data:
                    store_details.append(store_data)

            if not store_details:
                print("No store details retrieved.")
                return pd.DataFrame()

            store_df = pd.DataFrame(store_details)
            print("Store data retrieved successfully.")
            return store_df
        except Exception as e:
            print(f"Error retrieving store data: {e}")
            return pd.DataFrame()

    def get_store_details(self, store_url, headers):
        """
        Retrieves details for a specific store from the API.

        Args:
            store_url (str): The API endpoint for retrieving a store's details.
            headers (dict): The API headers containing the API key.

        Returns:
            dict: Store details as a dictionary.
        """
        try:
            print(f"Fetching store details from URL: {store_url}")
            response = requests.get(store_url, headers=headers)
            response.raise_for_status()  # Raises HTTPError for bad responses
            store_details = response.json()
            print(f"Store details retrieved successfully for URL: {store_url}")
            return store_details
        except requests.exceptions.RequestException as e:
            print(f"Error fetching store details from {store_url}: {e}")
            return {}

    def retrieve_pdf_data(self, pdf_link):
        """
        Retrieves data from a PDF link and returns it as a Pandas DataFrame.

        Args:
            pdf_link (str): URL of the PDF document.

        Returns:
            pd.DataFrame: Extracted data from the PDF.
        """
        try:
            data = read_pdf(pdf_link, pages="all", lattice=True, multiple_tables=True)
            df = pd.concat(data, ignore_index=True) if isinstance(data, list) else data
            print("PDF data retrieved successfully.")
            return df
        except Exception as e:
            print(f"Error retrieving PDF data: {e}")
            return pd.DataFrame()

    def extract_from_s3(self, s3_address):
        """
        Extracts data from an S3 bucket and returns it as a Pandas DataFrame.

        Args:
            s3_address (str): The S3 address of the file.

        Returns:
            pd.DataFrame: The extracted data.
        """
        try:
            bucket_name, key_name = s3_address[5:].split("/", 1)
            s3_client = boto3.client("s3")
            response = s3_client.get_object(Bucket=bucket_name, Key=key_name)
            file_content = response["Body"].read().decode("utf-8")
            df = pd.read_csv(StringIO(file_content))
            print("Data successfully extracted from S3.")
            return df
        except Exception as e:
            print(f"Error extracting data from S3: {e}")
            return pd.DataFrame()

    def extract_json_data(self, json_url):
        """
        Extracts data from a JSON file at the given URL and returns a Pandas DataFrame.

        Args:
            json_url (str): URL of the JSON file.

        Returns:
            pd.DataFrame: Extracted data.
        """
        try:
            response = requests.get(json_url)
            response.raise_for_status()
            json_data = response.json()
            df = pd.DataFrame(json_data)
            print("JSON data extracted successfully.")
            return df
        except Exception as e:
            print(f"Error extracting JSON data from {json_url}: {e}")
            return pd.DataFrame()

class StoreDetails:
    def __init__(self):
        self.base_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod"
        self.api_key = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
        self.headers = {"x-api-key": self.api_key}

    def get_total_stores(self):
        url = f"{self.base_url}/number_stores"
        try:
            print(f"Fetching total number of stores from URL: {url}")
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            total_stores = response.json()
            print(f"Total stores: {total_stores}")
            return total_stores.get("number_stores", 0)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching total number of stores: {e}")
            return 0

    def get_store_details(self, store_number):
        url = f"{self.base_url}/store_details/{store_number}"
        try:
            print(f"Fetching store details from URL: {url}")
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            store_details = response.json()
            print(f"Store details retrieved successfully for store number: {store_number}")
            return store_details
        except requests.exceptions.RequestException as e:
            print(f"Error fetching store details from {url}: {e}")
            return {}

    def retrieve_store_details_for_all(self, total_stores):
        """
        Retrieves details for all stores from the API and returns them as a DataFrame.
        """
        try:
            print(f"Retrieving store details for {total_stores} stores.")
            store_details = []
            for store_number in range(1, total_stores + 1):
                store_data = self.get_store_details(store_number)
                if store_data:
                    store_details.append(store_data)

            if not store_details:
                print("No store details retrieved.")
                return pd.DataFrame()

            store_df = pd.DataFrame(store_details)
            print("Store data retrieved successfully.")
            return store_df
        except Exception as e:
            print(f"Error retrieving store data: {e}")
            return pd.DataFrame()
#%%
