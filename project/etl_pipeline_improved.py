# import pandas as pd
# from kaggle.api.kaggle_api_extended import KaggleApi
# import os
# import logging
# from typing import List, Tuple
# from retry import retry
# import warnings
# import math
# from pathlib import Path

# warnings.filterwarnings("ignore")

# # Configure logging
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# class Config:
#     """Configuration settings for the ETL pipeline."""
#     DATASETS = [
#         "arindam235/startup-investments-crunchbase",
#         "claygendron/us-household-income-by-zip-code-2021-2011",
#         "ramjasmaurya/unicorn-startups",
#         "sergejnuss/united-states-cities-database",
#         "axeltorbenson/us-cities-by-population-top-330"
#     ]
#     SOURCE_FILE_PATHS = [
#         "../data/startup_VCs/",
#         "../data/household_income/",
#         "../data/unicorn-startups/",
#         "../data/us_cities/",
#         "../data/us_land_area/"
#     ]
#     CSV_NAMES = [
#         "investments_VC.csv",
#         "us_income_zipcode.csv",
#         "unicorns till sep 2022.csv",
#         "uscities.csv",
#         "us_cities_by_pop.csv"
#     ]
#     ENCODING = "ISO-8859-1"
#     FINAL_DATA_FILE_NAME = "startups_household_income_metropolitan_area.csv"


# class DataExtractor:
#     """Handles data extraction from Kaggle datasets."""

#     def __init__(self, config: Config):
#         self.config = config
#         self.api = KaggleApi()
#         self.api.authenticate()

#     @retry(exceptions=Exception, tries=3, delay=3, backoff=1, jitter=(0, 1))
#     def download_datasets(self, force: bool = True) -> None:
#         """Download datasets from Kaggle."""
#         for i, dataset in enumerate(self.config.DATASETS):
#             path = self.config.SOURCE_FILE_PATHS[i]
#             file_path = Path(os.path.join(path, self.config.CSV_NAMES[i]))
#             if file_path.exists() and not force:
#                 logging.info(f"Dataset already exists: {dataset}. Skipping download.")
#             else:
#                 self.api.dataset_download_files(dataset, path=path, unzip=True, force=force)
#                 logging.info(f"Downloaded dataset: {dataset}")

#     def load_dataframes(self) -> List[pd.DataFrame]:
#         """Load datasets into DataFrames."""
#         dataframes = []
#         for i, path in enumerate(self.config.SOURCE_FILE_PATHS):
#             try:
#                 df = pd.read_csv(os.path.join(path, self.config.CSV_NAMES[i]), encoding=self.config.ENCODING)
#                 dataframes.append(df)
#                 logging.info(f"Loaded dataset: {self.config.CSV_NAMES[i]}")
#             except Exception as e:
#                 logging.error(f"Failed to load {self.config.CSV_NAMES[i]}: {e}")
#                 raise
#         return dataframes

#     def cleanup_files(self) -> None:
#         """Clean up temporary files."""
#         for path in self.config.SOURCE_FILE_PATHS:
#             try:
#                 for file in Path(path).iterdir():
#                     file.unlink()
#                 Path(path).rmdir()
#                 logging.info(f"Cleaned up directory: {path}")
#             except Exception as e:
#                 logging.error(f"Error during cleanup in {path}: {e}")


# class DataTransformer:
#     """Handles data transformation tasks."""

#     @staticmethod
#     def income_data_percent_to_value(row: pd.Series, *percentage_columns: List[str]) -> pd.Series:
#         """Convert percentage values to actual numbers based on population."""
#         for col in percentage_columns:
#             row[col] = math.floor(row["Households"] * row[col] / 100)
#         return row

#     @staticmethod
#     def transform_vc_data(df: pd.DataFrame) -> pd.DataFrame:
#         """Clean and reduce VC dataset."""
#         df.rename(columns={"name":"Company",' market ': "Industry", ' funding_total_usd ': "funding_total_usd"}, inplace=True)
#         df = df[df['status']!="closed"]
#         df = df[df['country_code']=="USA"]
#         df['funding_total_usd']=df.funding_total_usd.str.strip().str.strip().str.replace(',', '').str.replace('-', '0').astype(float)   
#         df = df[df['funding_total_usd']>0]
#         df['Valuation ($B)'] = (df['grant']+ df['seed']+df['venture']+df['funding_total_usd'])/1000000000 ## for Billion conversion
#         df['joined_date'] = pd.to_datetime(df.founded_at,errors="coerce")
#         columns_to_keep = ['Company', 'Valuation ($B)','Country','city','Industry','joined_date']
#         df['Country'] = "United States"
#         df['city'] = df['city'].str.lower().str.replace(" ", "_")
#         df = df[columns_to_keep]
#         return df #df.drop(columns=columns_to_drop)


#     @staticmethod
#     def transform_income_data(df: pd.DataFrame) -> pd.DataFrame:
#         """Transform household income dataset."""
#         columns_to_drop = [col for col in df.columns if "Error" in col]
#         percentage_columns = ['Households Less Than $10,000', 'Households $10,000 to $14,999',
#        'Households $15,000 to $24,999', 'Households $25,000 to $34,999',
#        'Households $35,000 to $49,999', 'Households $50,000 to $74,999',
#        'Households $75,000 to $99,999', 'Households $100,000 to $149,999',
#        'Households $150,000 to $199,999', 'Households $200,000 or More']
#         df.rename(columns={"Households Median Income (Dollars)":"Households Median Income",
#                           "Households Mean Income (Dollars)":"Households Mean Income"},inplace=True)
#         income_columns = ["Households Median Income", "Households Mean Income",]
#         columns_to_keep = ["Geography",
#                            "Geographic Area Name","Households"] + percentage_columns + income_columns +["Year","zip_code"]
#         drop_column_subset = [x for x in columns_to_keep if x not in percentage_columns]
#         df.drop(columns=columns_to_drop, inplace=True)
#         df = df[df['Year']==2021] # keeping only last years data
#         df['zip_code'] = df.ZIP.apply(lambda x: str(x).zfill(5))
#         df.Year = df.Year.astype(int)
#         df = df.dropna(subset=drop_column_subset)
#         df = df.apply(DataTransformer.income_data_percent_to_value,args=percentage_columns,axis=1,result_type="expand")
#         return df[columns_to_keep] 

    
    
#     @staticmethod
#     def transform_unicorn_data(df: pd.DataFrame) -> pd.DataFrame:
#         """Transform unicorn startups dataset."""
#         df = df[df.Country == "United States"]
#         df['joined_date'] = pd.to_datetime(df['Date Joined'])
#         df.rename(columns={df.columns[4]: "city"}, inplace=True)
#         df.city = df.city.str.lower().str.replace(" ", "_")
#         df['Valuation ($B)'] = df['Valuation ($B)'].str.replace('$',"").astype(float)
#         return df.drop(columns=["Investors", "Date Joined"])

#     @staticmethod
#     def transform_cities_data(df: pd.DataFrame) -> pd.DataFrame:
#         """Transform US cities dataset."""
#         df['zips'] = df['zips'].str.split()
#         df.state_name = df.state_name.str.lower().str.replace(" ", "_")
#         df.city = df.city.str.lower().str.replace(" ", "_")
#         df = df.drop(columns=["source", "military", "incorporated", "timezone", "ranking", "id"])
#         return df.loc[df.groupby('city')['population'].idxmax()]

#     @staticmethod
#     def transform_area_data(df: pd.DataFrame) -> pd.DataFrame:
#         """Transform US cities land area stats dataset."""
#         df.state = df.state.str.lower().str.replace(" ", "_")
#         df.city = df.city.str.lower().str.replace(" ", "_")
#         return df[['city', 'state', 'land_area_km']]

    
#     @staticmethod
#     def merge_data(df_vc,df_hhi, df_ucs, df_us_cities,df_us_area) -> pd.DataFrame:
#         """Merge transformed datasets into a single DataFrame."""
#         df_vc_ucs_concat = pd.concat([df_vc, df_ucs], ignore_index=True, sort=False)
#         df_ucs_us_cities = pd.merge(df_vc_ucs_concat, df_us_cities, on='city', how='inner').explode('zips').rename(columns={'zips': 'zip_code'})
#         transformed_df = df_hhi.merge(df_ucs_us_cities, on='zip_code', how='inner')
#         merged_df = transformed_df.merge(df_us_area, on='city', how='inner')

#         return merged_df#.groupby("Company").first().reset_index()


# class DataLoader:
#     """Saves transformed data to a destination."""

#     def __init__(self, config: Config):
#         self.config = config

#     def save_to_csv(self, df: pd.DataFrame, destination_path: str) -> None:
#         """Save the DataFrame to CSV."""
#         final_path = os.path.join(destination_path, self.config.FINAL_DATA_FILE_NAME)
#         logging.info(f"Saving transformed data to {final_path}")
#         df.to_csv(final_path, index=False)
#         logging.info(f"Data saved to {final_path}")


# class ETLPipeline:
#     """Main class controlling the ETL pipeline."""

#     def __init__(self):
#         self.config = Config()
#         self.extractor = DataExtractor(self.config)
#         self.transformer = DataTransformer()
#         self.loader = DataLoader(self.config)

        
#     def create_directory_if_not_exist(self,directory_path:str):
#         """Create directory if not exists"""
#         output_dir = Path(directory_path)
#         output_dir.mkdir(parents=True, exist_ok=True)
#         return True
        
    
#     def run(self, del_tmp_files: bool = False, force_download: bool = False) -> pd.DataFrame:
#         try:
#             logging.info("ETL process started.")
#             self.extractor.download_datasets(force=force_download)
#             dfs = self.extractor.load_dataframes()
#             df_vc = self.transformer.transform_vc_data(dfs[0])
#             logging.info("Transformation process on VC crunchbase data completed.")
#             df_hhi = self.transformer.transform_income_data(dfs[1])
#             logging.info("Transformation process on household income data completed.")
#             df_ucs = self.transformer.transform_unicorn_data(dfs[2])
#             logging.info("Transformation process on unicorn startups data completed.")
#             df_us_cities = self.transformer.transform_cities_data(dfs[3])
#             logging.info("Transformation process on US cities data completed.")
#             df_us_area = self.transformer.transform_area_data(dfs[4])
#             logging.info("Transformation process on US land area and population  data completed.")

#             merged_df = self.transformer.merge_data(df_vc,df_hhi, df_ucs, df_us_cities, df_us_area)
#             logging.info("Data merged successfully.")
#             logging.info("All transformations completed.")
#             transformed_data_destination = os.path.join(os.path.split(os.getcwd())[0], "data")
#             self.create_directory_if_not_exist(transformed_data_destination)
#             self.loader.save_to_csv(merged_df, transformed_data_destination)
#             if del_tmp_files:
#                 self.extractor.cleanup_files()
#             logging.info("ETL process completed.")
#             return merged_df
#         except Exception as e:
#             logging.error(f"ETL process failed: {e}")
#             raise
import pandas as pd
import numpy as np
from kaggle.api.kaggle_api_extended import KaggleApi
import os
import logging
from typing import List, Tuple
from retry import retry
import warnings
import math
from pathlib import Path

warnings.filterwarnings("ignore")

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class Config:
    """Configuration settings for the ETL pipeline."""
    DATASETS = [
        "arindam235/startup-investments-crunchbase",
        "claygendron/us-household-income-by-zip-code-2021-2011",
        "ramjasmaurya/unicorn-startups",
        "sergejnuss/united-states-cities-database",
        "axeltorbenson/us-cities-by-population-top-330"
    ]
    SOURCE_FILE_PATHS = [
        "../data/startup_VCs/",
        "../data/household_income/",
        "../data/unicorn-startups/",
        "../data/us_cities/",
        "../data/us_land_area/"
    ]
    CSV_NAMES = [
        "investments_VC.csv",
        "us_income_zipcode.csv",
        "unicorns till sep 2022.csv",
        "uscities.csv",
        "us_cities_by_pop.csv"
    ]
    ENCODING = "ISO-8859-1"
    FINAL_DATA_FILE_NAME = "startups_household_income_metropolitan_area.csv"


class DataExtractor:
    """Handles data extraction from Kaggle datasets."""

    def __init__(self, config: Config):
        self.config = config
        self.api = KaggleApi()
        self.api.authenticate()

    @retry(exceptions=Exception, tries=3, delay=3, backoff=1, jitter=(0, 1))
    def download_datasets(self, force: bool = True) -> None:
        """Download datasets from Kaggle."""
        for i, dataset in enumerate(self.config.DATASETS):
            path = self.config.SOURCE_FILE_PATHS[i]
            file_path = Path(os.path.join(path, self.config.CSV_NAMES[i]))
            if file_path.exists() and not force:
                logging.info(f"Dataset already exists: {dataset}. Skipping download.")
            else:
                self.api.dataset_download_files(dataset, path=path, unzip=True, force=force)
                logging.info(f"Downloaded dataset: {dataset}")

    def load_dataframes(self) -> List[pd.DataFrame]:
        """Load datasets into DataFrames."""
        dataframes = []
        for i, path in enumerate(self.config.SOURCE_FILE_PATHS):
            try:
                df = pd.read_csv(os.path.join(path, self.config.CSV_NAMES[i]), encoding=self.config.ENCODING)
                dataframes.append(df)
                logging.info(f"Loaded dataset: {self.config.CSV_NAMES[i]}")
            except Exception as e:
                logging.error(f"Failed to load {self.config.CSV_NAMES[i]}: {e}")
                raise
        return dataframes

    def cleanup_files(self) -> None:
        """Clean up temporary files."""
        for path in self.config.SOURCE_FILE_PATHS:
            try:
                for file in Path(path).iterdir():
                    file.unlink()
                Path(path).rmdir()
                logging.info(f"Cleaned up directory: {path}")
            except Exception as e:
                logging.error(f"Error during cleanup in {path}: {e}")


class DataTransformer:
    """Handles data transformation tasks."""

    @staticmethod
    def income_data_percent_to_value(row: pd.Series, *percentage_columns: List[str]) -> pd.Series:
        """Convert percentage values to actual numbers based on population."""
        for col in percentage_columns:
            row[col] = math.floor(row["Households"] * row[col] / 100)
        return row

    @staticmethod
    def transform_vc_data(df: pd.DataFrame) -> pd.DataFrame:
        """Clean and reduce VC dataset."""
        df.rename(columns={"name":"Company",' market ': "Industry", ' funding_total_usd ': "funding_total_usd"}, inplace=True)
        df = df[df['status']!="closed"]
        df = df[df['country_code']=="USA"]
        df['funding_total_usd']=df.funding_total_usd.str.strip().str.strip().str.replace(',', '').str.replace('-', '0').astype(float)   
        df = df[df['funding_total_usd']>0]
        df['Valuation ($B)'] = (df['grant']+ df['seed']+df['venture']+df['funding_total_usd'])/1000000000 ## for Billion conversion
        df['joined_date'] = pd.to_datetime(df.founded_at,errors="coerce")
        columns_to_keep = ['Company', 'Valuation ($B)','Country','city','Industry','joined_date']
        df['Country'] = "United States"
        df['city'] = df['city'].str.lower().str.replace(" ", "_")
        df = df[columns_to_keep]
        return df #df.drop(columns=columns_to_drop)


    @staticmethod
    def transform_income_data(df: pd.DataFrame) -> pd.DataFrame:
            """Transform household income dataset."""
            columns_to_drop = [col for col in df.columns if "Error" in col]
            percentage_columns = ['Households Less Than $10,000', 'Households $10,000 to $14,999',
                                'Households $15,000 to $24,999', 'Households $25,000 to $34,999',
                                'Households $35,000 to $49,999', 'Households $50,000 to $74,999',
                                'Households $75,000 to $99,999', 'Households $100,000 to $149,999',
                                'Households $150,000 to $199,999', 'Households $200,000 or More']
            years = [2018, 2019, 2020, 2021]
            household_columns = ["Households_"+str(i) for i in years]
            percentage_columns_new=[]
            for i in percentage_columns:
                for j in years:
                    percentage_columns_new.append(i+'_'+str(j))
            df.rename(columns={"Households Median Income (Dollars)":"Households Median Income",
                              "Households Mean Income (Dollars)":"Households Mean Income"},inplace=True)
            income_columns = ["Households Median Income", "Households Mean Income"]
            income_columns_new = [i+'_'+str(j) for i in income_columns for j in years]
            columns_to_keep = ["zip_code"] + ["Households"] + percentage_columns + income_columns
            columns_to_keep_pivot = ["zip_code"] + household_columns + percentage_columns_new + income_columns_new 
            drop_column_subset = [x for x in columns_to_keep if x not in percentage_columns]
            df.drop(columns=columns_to_drop, inplace=True)
            # df = df[df['Year']==2021] # keeping only last years data
            df = df[df['Year']>=np.min(years)] # keeping only last 4 years data
            df['zip_code'] = df.ZIP.apply(lambda x: str(x).zfill(5))
            df.Year = df.Year.astype(int)
            df = df.dropna(subset=drop_column_subset)
            df = df.apply(DataTransformer.income_data_percent_to_value,args=percentage_columns,axis=1,result_type="expand")
            # convert multiple rows for years for company to multiple columns with _{year} appended to columns
            pivot_df = df.pivot(index=['zip_code'],columns='Year')
            # Flatten the multi-level columns
            pivot_df.columns = [f"{col[0]}_{col[1]}" for col in pivot_df.columns]

            # Reset index to make city a column
            pivot_df.reset_index(inplace=True)
            return pivot_df[columns_to_keep_pivot] 
    
    
    @staticmethod
    def transform_unicorn_data(df: pd.DataFrame) -> pd.DataFrame:
        """Transform unicorn startups dataset."""
        df = df[df.Country == "United States"]
        df['joined_date'] = pd.to_datetime(df['Date Joined'])
        df.rename(columns={df.columns[4]: "city"}, inplace=True)
        df.city = df.city.str.lower().str.replace(" ", "_")
        df['Valuation ($B)'] = df['Valuation ($B)'].str.replace('$',"").astype(float)
        return df.drop(columns=["Investors", "Date Joined"])

    @staticmethod
    def transform_cities_data(df: pd.DataFrame) -> pd.DataFrame:
        """Transform US cities dataset."""
        df['zips'] = df['zips'].str.split()
        df.state_name = df.state_name.str.lower().str.replace(" ", "_")
        df.city = df.city.str.lower().str.replace(" ", "_")
        df = df.drop(columns=["source", "military", "incorporated", "timezone", "ranking", "id"])
        return df.loc[df.groupby('city')['population'].idxmax()]

    @staticmethod
    def transform_area_data(df: pd.DataFrame) -> pd.DataFrame:
        """Transform US cities land area stats dataset."""
        df.state = df.state.str.lower().str.replace(" ", "_")
        df.city = df.city.str.lower().str.replace(" ", "_")
        return df[['city', 'state', 'land_area_km']]

    
    @staticmethod
    def merge_data(df_vc,df_hhi, df_ucs, df_us_cities,df_us_area) -> pd.DataFrame:
        """Merge transformed datasets into a single DataFrame."""
        df_vc_ucs_concat = pd.concat([df_vc, df_ucs], ignore_index=True, sort=False)
        df_ucs_us_cities = pd.merge(df_vc_ucs_concat, df_us_cities, on='city', how='inner').explode('zips').rename(columns={'zips': 'zip_code'})
        transformed_df = df_hhi.merge(df_ucs_us_cities, on='zip_code', how='inner')
        merged_df = transformed_df.merge(df_us_area, on='city', how='inner')

        return merged_df.groupby(["Company","state_name","county_name","city"]).first().reset_index()
    


class DataLoader:
    """Saves transformed data to a destination."""

    def __init__(self, config: Config):
        self.config = config

    def save_to_csv(self, df: pd.DataFrame, destination_path: str) -> None:
        """Save the DataFrame to CSV."""
        final_path = os.path.join(destination_path, self.config.FINAL_DATA_FILE_NAME)
        logging.info(f"Saving transformed data to {final_path}")
        df.to_csv(final_path, index=False)
        logging.info(f"Data saved to {final_path}")


class ETLPipeline:
    """Main class controlling the ETL pipeline."""

    def __init__(self):
        self.config = Config()
        self.extractor = DataExtractor(self.config)
        self.transformer = DataTransformer()
        self.loader = DataLoader(self.config)

        
    def create_directory_if_not_exist(self,directory_path:str):
        """Create directory if not exists"""
        output_dir = Path(directory_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        return True
        
    
    def run(self, del_tmp_files: bool = False, force_download: bool = False) -> pd.DataFrame:
        try:
            logging.info("ETL process started.")
            self.extractor.download_datasets(force=force_download)
            dfs = self.extractor.load_dataframes()
            df_vc = self.transformer.transform_vc_data(dfs[0])
            logging.info("Transformation process on VC crunchbase data completed.")
            df_hhi = self.transformer.transform_income_data(dfs[1])
            logging.info("Transformation process on household income data completed.")
            df_ucs = self.transformer.transform_unicorn_data(dfs[2])
            logging.info("Transformation process on unicorn startups data completed.")
            df_us_cities = self.transformer.transform_cities_data(dfs[3])
            logging.info("Transformation process on US cities data completed.")
            df_us_area = self.transformer.transform_area_data(dfs[4])
            logging.info("Transformation process on US land area and population  data completed.")

            merged_df = self.transformer.merge_data(df_vc,df_hhi, df_ucs, df_us_cities, df_us_area)
            logging.info("Data merged successfully.")
            logging.info("All transformations completed.")
            transformed_data_destination = os.path.join(os.path.split(os.getcwd())[0], "data")
            self.create_directory_if_not_exist(transformed_data_destination)
            self.loader.save_to_csv(merged_df, transformed_data_destination)
            if del_tmp_files:
                self.extractor.cleanup_files()
            logging.info("ETL process completed.")
            return merged_df
        except Exception as e:
            logging.error(f"ETL process failed: {e}")
            raise


if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run(del_tmp_files=False, force_download=False)
