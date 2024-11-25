#%%
import pandas as pd
import re

class DataCleaning:
    def clean_user_data(self, user_data):
        """
        Cleans the user data by handling NULL values, converting date columns, and resetting the index.

        Args:
            user_data (pd.DataFrame): The user data DataFrame.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """
        if user_data.empty:
            print("The DataFrame is empty. No data to clean.")
            return user_data
        try:
            user_data.replace("NULL", pd.NA, inplace=True)
            user_data.dropna(inplace=True)
            if "join_date" in user_data.columns:
                user_data["join_date"] = pd.to_datetime(user_data["join_date"], errors="coerce")
                user_data.dropna(subset=["join_date"], inplace=True)
            user_data.reset_index(drop=True, inplace=True)
            print("User data cleaned successfully.")
            return user_data
        except Exception as e:
            print(f"Error cleaning user data: {e}")
            return user_data

    def clean_card_data(self, card_data):
        """
        Cleans the card data by handling NULL values, removing duplicates, and converting date columns.

        Args:
            card_data (pd.DataFrame): The card data DataFrame.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """
        if card_data.empty:
            print("The DataFrame is empty. No data to clean.")
            return card_data
        try:
            card_data.replace("NULL", pd.NA, inplace=True)
            card_data.dropna(inplace=True)
            if "CardNumber" in card_data.columns:
                card_data.drop_duplicates(subset=["CardNumber"], inplace=True)
                card_data = card_data[card_data["CardNumber"].str.isnumeric()]
            if "date_payment_confirmed" in card_data.columns:
                card_data["date_payment_confirmed"] = pd.to_datetime(
                    card_data["date_payment_confirmed"], errors="coerce"
                )
                card_data.dropna(subset=["date_payment_confirmed"], inplace=True)
            card_data.reset_index(drop=True, inplace=True)
            print("Card data cleaned successfully.")
            return card_data
        except Exception as e:
            print(f"Error cleaning card data: {e}")
            return card_data

    def clean_store_data(self, store_data):
        """
        Cleans the store data by handling NULL values, converting dates, and stripping non-numeric characters.

        Args:
            store_data (pd.DataFrame): The store data DataFrame.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """
        if store_data.empty:
            print("The DataFrame is empty. No data to clean.")
            return store_data
        
        print(f"Initial data shape: {store_data.shape}")

        try:
            # Step 1: Replace "NULL" strings with actual NULL values (pd.NA)
            store_data.replace("NULL", pd.NA, inplace=True)
            print(f"After replacing 'NULL' with pd.NA: {store_data.shape}")

            # Step 2: Check for missing values before dropping
            print("Missing values before drop:")
            print(store_data.isna().sum())

            if "opening_date" in store_data.columns:
                store_data["opening_date"] = pd.to_datetime(
                    store_data["opening_date"], format="%Y-%m-%d", errors="coerce"
                )
                

            # Step 3: Clean 'staff_number' column by stripping non-numeric characters and whitespace
            if "staff_number" in store_data.columns:
                store_data["staff_number"] = store_data["staff_number"].astype(str).str.replace(
                    r"[^\d]", "", regex=True
                )
                store_data["staff_number"] = pd.to_numeric(store_data["staff_number"], errors="coerce")
                print(f"After cleaning 'staff_number': {store_data.shape}")
                store_data.dropna(subset=["staff_number"], inplace=True)
                print(f"After dropping invalid 'staff_number': {store_data.shape}")

            # Step 4: Reset index after cleaning
            store_data.reset_index(drop=True, inplace=True)

            # Step 5: Validate the row count
            if len(store_data) != 441:
                print(f"Warning: Unexpected row count after cleaning. Found {len(store_data)} rows, expected 441.")
            else:
                print("Store data cleaned successfully with 441 rows.")

            return store_data

        except Exception as e:
            print(f"Error cleaning store data: {e}")
            return store_data
    
    def convert_product_weights(self, products_df):
        """
        Converts the weight column in products DataFrame to kilograms.

        Args:
            products_df (pd.DataFrame): The products DataFrame.

        Returns:
            pd.DataFrame: The updated DataFrame with weights in kg.
        """
        def convert_to_kg(weight):
            try:
                weight = str(weight).lower().strip()
                if "g" in weight:
                    return float(weight.replace("g", "")) / 1000
                if "kg" in weight:
                    return float(weight.replace("kg", ""))
                if "ml" in weight:
                    return float(weight.replace("ml", "")) / 1000
                if "l" in weight:
                    return float(weight.replace("l", ""))
                return pd.NA
            except ValueError:
                return pd.NA

        try:
            products_df["weight"] = products_df["weight"].apply(convert_to_kg)
            products_df.dropna(subset=["weight"], inplace=True)
            products_df["weight"] = products_df["weight"].astype(float)
            print("Product weights converted to kilograms successfully.")
            return products_df
        except Exception as e:
            print(f"Error converting product weights: {e}")
            return products_df

    def clean_products_data(self, products_df):
        """
        Cleans the products data by handling NULL values and converting weights.

        Args:
            products_df (pd.DataFrame): The products data DataFrame.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """
        if products_df.empty:
            print("The DataFrame is empty. No data to clean.")
            return products_df
        try:
            products_df.replace("NULL", pd.NA, inplace=True)
            products_df.dropna(inplace=True)
            products_df = self.convert_product_weights(products_df)
            products_df.reset_index(drop=True, inplace=True)
            print("Products data cleaned successfully.")
            return products_df
        except Exception as e:
            print(f"Error cleaning products data: {e}")
            return products_df

    def clean_orders_data(self, orders_df):
        """
        Cleans the orders data by removing unwanted columns and handling NULL values.

        Args:
            orders_df (pd.DataFrame): The orders data DataFrame.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """
        if orders_df.empty:
            print("The DataFrame is empty. No data to clean.")
            return orders_df
        try:
            columns_to_remove = ["first_name", "last_name", "1"]
            orders_df.drop(columns=columns_to_remove, errors="ignore", inplace=True)
            orders_df.replace("NULL", pd.NA, inplace=True)
            orders_df.dropna(inplace=True)
            if "order_date" in orders_df.columns:
                orders_df["order_date"] = pd.to_datetime(orders_df["order_date"], errors="coerce")
                orders_df.dropna(subset=["order_date"], inplace=True)
            orders_df.reset_index(drop=True, inplace=True)
            print("Orders data cleaned successfully.")
            return orders_df
        except Exception as e:
            print(f"Error cleaning orders data: {e}")
            return orders_df

    def clean_date_events_data(self, date_events_df):
        """
        Cleans the date events data by handling NULL values and converting numeric columns.

        Args:
            date_events_df (pd.DataFrame): The date events data DataFrame.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """
        if date_events_df.empty:
            print("The DataFrame is empty. No data to clean.")
            return date_events_df
        try:
            date_events_df.replace("NULL", pd.NA, inplace=True)
            date_events_df.dropna(inplace=True)
            for col in ["day", "month", "year"]:
                if col in date_events_df.columns:
                    date_events_df[col] = pd.to_numeric(date_events_df[col], errors="coerce")
            date_events_df.dropna(inplace=True)
            date_events_df.reset_index(drop=True, inplace=True)
            print("Date events data cleaned successfully.")
            return date_events_df
        except Exception as e:
            print(f"Error cleaning date events data: {e}")
            return date_events_df

# %%
