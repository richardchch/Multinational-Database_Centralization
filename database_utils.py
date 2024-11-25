# database_utils.py
#%%
import yaml
import pandas as pd
from sqlalchemy import create_engine


class DatabaseConnector:
    def __init__(self, db_name, user, password, host="localhost", port=5432):
        """
        Initializes the DatabaseConnector with connection details for the PostgreSQL database.
        """
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.engine = None

    def connect(self):
        """
        Establishes a connection to the PostgreSQL database using SQLAlchemy.
        """
        try:
            db_url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"
            self.engine = create_engine(db_url)
            print(f"Connected to PostgreSQL database '{self.db_name}' successfully.")
        except Exception as e:
            print(f"Error connecting to PostgreSQL database: {e}")

    def upload_to_db(self, df, table_name):
        """
        Uploads a Pandas DataFrame to the specified table in the PostgreSQL database.

        Args:
            df (pd.DataFrame): The DataFrame to upload.
            table_name (str): The name of the table to upload the data to.
        """
        try:
            if self.engine is None:
                self.connect()
            if df.empty:
                print(f"No data to upload for table: {table_name}.")
                return
            df.to_sql(table_name, con=self.engine, if_exists="replace", index=False)
            print(f"Data uploaded successfully to the table '{table_name}'.")
        except Exception as e:
            print(f"Error uploading data to the table '{table_name}': {e}")


def read_db_creds(file_path):
    """
    Reads the database credentials from a YAML file and returns them as a dictionary.

    Args:
        file_path (str): Path to the YAML file containing database credentials.

    Returns:
        dict: A dictionary containing database credentials.
    """
    try:
        with open(file_path, "r") as file:
            creds = yaml.safe_load(file)
        return creds
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return None
    except yaml.YAMLError as exc:
        print(f"Error parsing YAML file: {exc}")
        return None


def init_db_engine(file_path):
    """
    Initializes and returns an SQLAlchemy engine using credentials from a YAML file.

    Args:
        file_path (str): Path to the YAML file containing database credentials.

    Returns:
        sqlalchemy.engine.Engine: A SQLAlchemy engine instance.
    """
    try:
        creds = read_db_creds(file_path)
        if not creds:
            print("Error: Missing credentials in YAML file.")
            return None

        host = creds.get("RDS_HOST")
        port = creds.get("RDS_PORT", 5432)
        user = creds.get("RDS_USER")
        password = creds.get("RDS_PASSWORD")
        db_name = creds.get("RDS_DATABASE")

        db_url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
        engine = create_engine(db_url)
        print("Successfully connected to the AiCore RDS database.")
        return engine
    except Exception as e:
        print(f"Error initializing AiCore RDS engine: {e}")
        return None

# %%
