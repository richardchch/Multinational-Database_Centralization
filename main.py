
# %%
from data_extraction import DataExtractor, StoreDetails
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector, init_db_engine
import pandas as pd


def main():
    # Step 1: Initialize engines
    aicore_engine = init_db_engine("db_creds.yaml")  # For AiCore RDS instance
    local_db_connector = DatabaseConnector(
        db_name="sales_data",
        user="postgres",  
        password="Rishabh%4021",
        host="localhost",
        port=5432
    )

    if not aicore_engine:
        print("Failed to initialize AiCore database engine. Exiting.")
        return

    print("Successfully initialized AiCore database engine.")

    # Initialize components
    extractor = DataExtractor(aicore_engine)
    cleaner = DataCleaning()

    # --- USER DATA TASK ---
    try:
        print("\n--- Starting User Data Task ---")
        user_data = extractor.read_table("legacy_users")
        cleaned_user_data = cleaner.clean_user_data(user_data)
        local_db_connector.upload_to_db(cleaned_user_data, "dim_users")
        print("User data uploaded successfully.")
    except Exception as e:
        print(f"Error during User Data Task: {e}")

    # --- CARD DETAILS TASK ---
    try:
        print("\n--- Starting Card Details Task ---")
        pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        card_data = extractor.retrieve_pdf_data(pdf_link)
        cleaned_card_data = cleaner.clean_card_data(card_data)
        local_db_connector.upload_to_db(cleaned_card_data, "dim_card_details")
        print("Card data uploaded successfully.")
    except Exception as e:
        print(f"Error during Card Details Task: {e}")

    # --- PRODUCT DETAILS TASK ---
    try:
        print("\n--- Starting Product Details Task ---")
        s3_address = "s3://data-handling-public/products.csv"
        product_data = extractor.extract_from_s3(s3_address)
        cleaned_product_data = cleaner.clean_products_data(product_data)
        local_db_connector.upload_to_db(cleaned_product_data, "dim_products")
        print("Product data uploaded successfully.")
    except Exception as e:
        print(f"Error during Product Details Task: {e}")

    # --- ORDER DETAILS TASK ---
    try:
        print("\n--- Starting Order Details Task ---")
        order_data = extractor.read_table("orders_table")
        cleaned_order_data = cleaner.clean_orders_data(order_data)
        local_db_connector.upload_to_db(cleaned_order_data, "dim_orders")
        print("Order data uploaded successfully.")
    except Exception as e:
        print(f"Error during Order Details Task: {e}")

    # --- DATE EVENTS TASK ---
    try:
        print("\n--- Starting Date Events Task ---")
        json_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
        date_events_data = extractor.extract_json_data(json_url)
        cleaned_date_events_data = cleaner.clean_date_events_data(date_events_data)
        local_db_connector.upload_to_db(cleaned_date_events_data, "dim_date_times")
        print("Date events data uploaded successfully.")
    except Exception as e:
        print(f"Error during Date Events Task: {e}")

    # --- STORE DETAILS TASK ---
    try:
        print("\n--- Starting Store Details Task ---")
        store_processor = StoreDetails()
        total_stores = store_processor.get_total_stores()

        if total_stores > 0:
            print(f"Retrieving details for {total_stores} stores.")
            store_df = store_processor.retrieve_store_details_for_all(total_stores)

            if not store_df.empty:
                print("Cleaning store data...")
                cleaned_store_data = cleaner.clean_store_data(store_df)
                local_db_connector.upload_to_db(cleaned_store_data, "dim_store_details")
                print("Store data uploaded successfully.")
            else:
                print("No store details retrieved.")
        else:
            print("No stores found.")
    except Exception as e:
        print(f"Error during Store Details Task: {e}")

    print("\n--- All tasks completed successfully! ---")


if __name__ == "__main__":
    main()
# %%
