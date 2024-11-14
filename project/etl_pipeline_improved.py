import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
import os
import logging
from typing import Tuple, List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Config:
    """Configuration settings for the ETL pipeline."""
    datasets = [
        "arindam235/startup-investments-crunchbase",
        "claygendron/us-household-income-by-zip-code-2021-2011",
        "ramjasmaurya/unicorn-startups",
        "sergejnuss/united-states-cities-database"
    ]
    source_file_paths = [
        "../data/startup_VCs/",
        "../data/household_income/",
        "../data/unicorn-startups/",
        "../data/us_cities/"
    ]
    kaggle_encoding = "ISO-8859-1"
    csv_names = [
        "investments_VC.csv",
        "us_income_zipcode.csv",
        "unicorns_till_sep_2022.csv",
        "uscities.csv"
    ]
    final_data_file_name = "startups_household_income_metropolitan_area.csv"


class DataExtractor:
    """Handles data extraction from sources."""
    def __init__(self, config: Config):
        self.config = config
        self.api = KaggleApi()
        self.api.authenticate()
    
    def download_datasets(self) -> None:
        """Download datasets from Kaggle."""
        for i, dataset in enumerate(self.config.datasets):
            try:
                self.api.dataset_download_files(dataset, path=self.config.source_file_paths[i], unzip=True, force=True)
                logging.info(f"Downloaded dataset: {dataset}")
            except Exception as e:
                logging.error(f"Failed to download dataset {dataset}: {e}")
                raise

    def load_dataframes(self) -> List[pd.DataFrame]:
        """Load data into DataFrames."""
        dataframes = []
        for i, file_path in enumerate(self.config.source_file_paths):
            try:
                df = pd.read_csv(os.path.join(file_path, self.config.csv_names[i]), encoding=self.config.kaggle_encoding)
                dataframes.append(df)
            except Exception as e:
                logging.error(f"Failed to load DataFrame from {self.config.csv_names[i]}: {e}")
                raise
        return dataframes

    def cleanup_files(self) -> None:
        """Clean up temporary files."""
        for i, file_path in enumerate(self.config.source_file_paths):
            try:
                os.remove(os.path.join(file_path, self.config.csv_names[i]))
                os.rmdir(file_path)
                logging.info(f"Removed temporary file: {self.config.csv_names[i]}")
            except Exception as e:
                logging.error(f"Failed to remove temporary file {self.config.csv_names[i]}: {e}")


class DataTransformer:
    """Handles data transformation operations."""
    
    @staticmethod
    def transform_vc_data(df: pd.DataFrame) -> pd.DataFrame:
        """Transform VC dataset."""
        columns_to_drop = [
            'permalink', 'homepage_url', 'first_funding_at', 'last_funding_at', 'seed', 'venture', 
            'equity_crowdfunding', 'undisclosed', 'convertible_note', 'debt_financing', 'angel', 
            'grant', 'private_equity', 'post_ipo_equity', 'post_ipo_debt', 'secondary_market', 
            'product_crowdfunding', 'round_A', 'round_B', 'round_C', 'round_D', 'round_E', 
            'round_F', 'round_G', 'round_H'
        ]
        df.rename(columns={' market ': "market", ' funding_total_usd ': "funding_total_usd"}, inplace=True)
        return df.drop(columns=columns_to_drop)

    @staticmethod
    def transform_income_data(df: pd.DataFrame) -> pd.DataFrame:
        """Transform household income dataset."""
        columns_to_drop = [col for col in df.columns if "Error" in col]
        df.drop(columns=columns_to_drop, inplace=True)
        df['zip_code'] = df.ZIP.apply(lambda x: str(x).zfill(5))
        return df.drop(columns=["ZIP"])

    @staticmethod
    def transform_unicorn_data(df: pd.DataFrame) -> pd.DataFrame:
        """Transform unicorn startups dataset."""
        df = df[df.Country == "United States"]
        df['joined_date'] = pd.to_datetime(df['Date Joined'])
        df.rename(columns={df.columns[4]: "city"}, inplace=True)
        return df.drop(columns=["Investors", "Date Joined"])

    @staticmethod
    def transform_cities_data(df: pd.DataFrame) -> pd.DataFrame:
        """Transform US cities dataset."""
        df['zips'] = df.zips.str.split()
        df = df.drop(columns=["source", "military", "incorporated", "timezone", "ranking", "id"])
        return df.loc[df.groupby('city')['population'].idxmax()]

    @staticmethod
    def merge_data(df_hhi, df_ucs, df_us_cities) -> pd.DataFrame:
        """Merge transformed datasets into a single DataFrame."""
        df_ucs_us_cities = pd.merge(df_ucs, df_us_cities, on='city', how='inner').explode('zips').rename(columns={'zips': 'zip_code'})
        transformed_df = df_hhi.merge(df_ucs_us_cities, on='zip_code', how='inner')
        return transformed_df.groupby("Company").first().reset_index()


class DataLoader:
    """Handles loading of transformed data to a destination."""
    
    def __init__(self, config: Config):
        self.config = config

    def save_to_csv(self, df: pd.DataFrame, destination_path: str) -> None:
        """Save DataFrame to CSV."""
        final_file_path = os.path.join(destination_path, self.config.final_data_file_name)
        try:
            df.to_csv(final_file_path, index=False)
            logging.info("Data saved successfully.")
        except Exception as e:
            logging.error(f"Failed to save data to CSV: {e}")
            raise


class ETLPipeline:
    """ETL pipeline controller for managing extraction, transformation, and loading."""
    
    def __init__(self):
        self.config = Config()
        self.extractor = DataExtractor(self.config)
        self.transformer = DataTransformer()
        self.loader = DataLoader(self.config)
        
    def run(self, del_tmp_files: bool = False) -> pd.DataFrame:
        try:
            logging.info("Starting ETL process")
            self.extractor.download_datasets()
            dfs = self.extractor.load_dataframes()
            df_vc, df_hhi, df_ucs, df_us_cities = dfs
            logging.info("Data extraction complete.")

            # Transformation
            df_vc = self.transformer.transform_vc_data(df_vc)
            df_hhi = self.transformer.transform_income_data(df_hhi)
            df_ucs = self.transformer.transform_unicorn_data(df_ucs)
            df_us_cities = self.transformer.transform_cities_data(df_us_cities)
            transformed_df = self.transformer.merge_data(df_hhi, df_ucs, df_us_cities)
            logging.info("Data transformation complete.")
            
            # Loading
            transformed_data_destination = os.path.join(os.path.split(os.getcwd())[0], "data")
            self.loader.save_to_csv(transformed_df, transformed_data_destination)
            logging.info("ETL process completed successfully.")
            
            if del_tmp_files:
                self.extractor.cleanup_files()

            return transformed_df
        except Exception as e:
            logging.error(f"ETL process failed: {e}")
            raise


# Running the ETL pipeline
if __name__ == "__main__":
    etl_pipeline = ETLPipeline()
    etl_pipeline.run(del_tmp_files=True)
