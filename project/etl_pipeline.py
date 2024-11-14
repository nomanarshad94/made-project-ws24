import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ETLPipeline:
    
    def __init__(self):
        self.datasets = [
            "arindam235/startup-investments-crunchbase",
            "claygendron/us-household-income-by-zip-code-2021-2011",
            "ramjasmaurya/unicorn-startups",
            "sergejnuss/united-states-cities-database"
        ]

        self.source_file_paths = [
         "../data/startup_VCs/",
         "../data/household_income/",
         "../data/unicorn-startups/",
         "../data/us_cities/"]
        
        self.kaggle_encoding = "ISO-8859-1"
        self.csv_names = [  "investments_VC.csv",
                            "us_income_zipcode.csv",
                            "unicorns till sep 2022.csv",
                            "uscities.csv",
                         ]

        self.final_data_file_name = "startups_household_income_metropolitan_area.csv"
        

    def extract(self,del_tmp_files):
        """Extract data from online sources/apis in dataframe and return pandas dataframes for each file"""
        try:
            
            # Download the dataset from Kaggle
            for i,dataset in enumerate(self.datasets):
                try:
                    api.dataset_download_files(self.datasets[i], path=self.source_file_paths[i], unzip=True,force=True)
                except Exception as e:
                    logging.error(f"Failed to download data from {dataset}: {e}")
            logging.info(f"Successfully downloaded data files")
            
            dfs = []
            for i,dataset in enumerate(self.datasets):
                try:
                    dfs.append(pd.read_csv(os.path.join(self.source_file_paths[i], self.csv_names[i]),encoding=self.kaggle_encoding))
                except Exception as e:
                    logging.error(f"Failed to create dataframe for {self.csv_names[i]}: {e}")
            logging.info(f"Successfully created dataframes from files")
            
            
            # Cleanup temporary files and folders after loading the data in dataframes
            if del_tmp_files:
                logging.info(f"Removing the temporary files and folders")
                for i,dataset in enumerate(self.source_file_paths):
                    # Remove the raw csv files
                    try:
                        os.remove(os.path.join(self.source_file_paths[i], self.csv_names[i]))
                        os.rmdir(self.source_file_paths[i])
                        logging.info(f"Successfully removed temporary source file: {os.path.join(self.source_file_paths[i], self.csv_names[i])}")
                    except Exception as e:
                        logging.error(f"Failed to remove temporary file {os.path.join(self.source_file_paths[i], self.csv_names[i])}: {e}")
            
            df_vc, df_hhi, df_ucs, df_us_cities = dfs[0],dfs[1],dfs[2],dfs[3]
            return df_vc, df_hhi, df_ucs, df_us_cities
        
        except Exception as e:
            logging.error(f"An error occurred during extraction: {e}")
            raise

    def transform(self, df_vc, df_hhi, df_ucs, df_us_cities):
        """Apply df specific transformations on dataframe and join them"""
        try:
            
            # ----------------------------------------------------------------------------------------
            # DataSource-1: VC data
            # ----------------------------------------------------------------------------------------
            try:
                vc_columns_to_drop = ['permalink','homepage_url','first_funding_at',
                                   'last_funding_at', 'seed', 'venture', 'equity_crowdfunding',
                                   'undisclosed', 'convertible_note', 'debt_financing', 'angel', 'grant',
                                   'private_equity', 'post_ipo_equity', 'post_ipo_debt',
                                   'secondary_market', 'product_crowdfunding', 'round_A', 'round_B',
                                   'round_C', 'round_D', 'round_E', 'round_F', 'round_G', 'round_H']
                df_vc.rename(columns={' market ':"market",' funding_total_usd ':"funding_total_usd"},inplace=True)
                df_vc.drop(vc_columns_to_drop,inplace=True,axis=1)
            except Exception as e:
                logging.error(f"Failed to transform 'VC' data: {e}")
            
            
            # ----------------------------------------------------------------------------------------
            # DataSource-2: US household_income data
            # ----------------------------------------------------------------------------------------
            try:
                hhi_columns_to_drop = []
                for i in df_hhi.columns:
                    if "Error" in i:
                        hhi_columns_to_drop.append(i)

                df_hhi.drop(hhi_columns_to_drop,axis=1,inplace=True)

                df_hhi = df_hhi[list(df_hhi.columns[:16]) + list(df_hhi.columns[-1:])]
                # df_hhi['zip_code'] = df_hhi.ZIP.astype(str).str.zfill(5)
                df_hhi['zip_code'] = df_hhi.ZIP.apply(lambda x: str(x).zfill(5))
                df_hhi.drop(columns=["ZIP"],inplace=True)

            except Exception as e:
                logging.error(f"Failed to transform 'Household Income' data: {e}")
                              
            
            # ----------------------------------------------------------------------------------------
            # DataSource-3: US unicorn startups data
            # ----------------------------------------------------------------------------------------
                              
            try:
                ucs_columns_to_drop = ["Investors","Date Joined"]
                df_ucs = df_ucs[df_ucs.Country=="United States"]
                df_ucs['joined_date'] = pd.to_datetime(df_ucs['Date Joined'])
                df_ucs.rename(columns={ df_ucs.columns[4]: "city" }, inplace = True)
                df_ucs.drop(columns=ucs_columns_to_drop,inplace=True)
                df_ucs.reset_index(drop=True, inplace=True)
            except Exception as e:
                logging.error(f"Failed to transform 'US unicorn startups' data: {e}")
            # ----------------------------------------------------------------------------------------
            # DataSource-4: US cities database
            # ----------------------------------------------------------------------------------------

            try:
                us_cities_columns_to_drop = ["source","military","incorporated","timezone","ranking","id"]
                df_us_cities['zips'] = df_us_cities.zips.str.split()
                df_us_cities = df_us_cities.drop(us_cities_columns_to_drop,axis=1)
                ## city name is not unique and can have multiple instances in US so for joining it with other 
                ## have to select most populated one.
                df_us_cities=df_us_cities.iloc[df_us_cities.groupby('city')['population'].idxmax()]
            except Exception as e:
                logging.error(f"Failed to transform 'US cities' data: {e}")                              
                              
            # ----------------------------------------------------------------------------------------
            # Transformation --> Merging DataSource-3 & DataSource-4 to combine regional and population related details
            transformed_df_ucs_us_cities = pd.merge(df_ucs, df_us_cities, on='city', how='inner')
            transformed_df_ucs_us_cities = transformed_df_ucs_us_cities.explode('zips').rename(columns={'zips': 'zip_code'})
            
            # Transformation --> Merging DataSource-2 & cities transformed data to add US houesehold income details with metropolitan area mapping
            transformed_df = df_hhi.merge(transformed_df_ucs_us_cities, on='zip_code', how='inner')
            # Due to exploding of cities dataframe with zip codes there is duplication in data. To remove duplication:
            transformed_df = transformed_df.groupby("Company").first()

            transformed_df = transformed_df.reset_index()

            # transformed_city_hhi_vc.describe()

            return transformed_df
        
        except Exception as e:
            logging.error(f"An error occurred during transformation: {e}")
            raise

    def load(self, transformed_df, destination_path):
        try:

            final_file_path = os.path.join(destination_path,self.final_data_file_name)
            final_df.to_csv(final_file_path,index=False)
        except Exception as e:
            logging.error(f"An error occurred while writing df to file: {e}")
            raise

        return None           
            
        
    def run(self,del_tmp_files=False):
        try:
            df_vc, df_hhi, df_ucs, df_us_cities = self.extract(del_tmp_files)
            logging.info("Extraction completed.")
            
            transformed_df = self.transform(df_vc, df_hhi, df_ucs, df_us_cities)
            logging.info("Transformation completed.")
                              
            transformed_data_destination = os.path.join(os.path.split(os.getcwd())[0],"data")
            self.load(transformed_df, transformed_data_destination)
            logging.info("ETL process completed successfully.")
            
        except Exception as e:
            logging.error(f"An error occurred during the ETL process: {e}")
                         
        return transformed_df


# Running the ETL pipeline
if __name__ == "__main__":
    etl = ETLPipeline()
    transformed_df = etl.run(del_tmp_files=True)