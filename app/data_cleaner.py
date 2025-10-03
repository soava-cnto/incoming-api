# data_cleaner.py
import pandas as pd
import re

class DataCleaner:
    @staticmethod
    def sanitize_columns(df):
        df.columns = (
            df.columns.str.strip()
                      .str.lower()
                      .str.replace(" ", "_", regex=False)
                      .str.replace("-", "_", regex=False)                      
                      .str.replace("[^0-9a-z_]", "", regex=True)
        )
        return df

    @staticmethod
    def normalize_phone(phone):
        if pd.isna(phone):
            return None
        digits = re.sub(r"\D+", "", str(phone))
        return digits or None

    @staticmethod
    def clean(df):
        df = df.copy()
        df = DataCleaner.sanitize_columns(df)

        # Trim des strings
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].str.strip().replace("", pd.NA)
            
        # Ajout semaine ISO
        if "date_appel" in df.columns:
            df["date_appel"] = pd.to_datetime(df["date_appel"], errors="coerce")
            df["semaine"] = df["date_appel"].dt.isocalendar().week
            # df["iso_year"] = df["date_appel"].dt.isocalendar().year

        # DateTime
        if "date_appel" in df.columns and "heure_appel" in df.columns:
            df["datetime_appel"] = pd.to_datetime(
                df["date_appel"].dt.strftime("%Y-%m-%d") + " " + df["heure_appel"].fillna(""),
                errors="coerce"
            )


        # Colonnes numériques
        numeric_cols = [
            "duree_prise_en_charge", "duree_post_travail_agent", 
            "duree_appel", "indice", "raccrochage", "numero_court"
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        # Normaliser téléphone
        if "numero_telephone" in df.columns:
            df["numero_telephone_clean"] = df["numero_telephone"].apply(DataCleaner.normalize_phone)

        return df
