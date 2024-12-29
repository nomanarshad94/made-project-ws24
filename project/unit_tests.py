import unittest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import Mock, patch
from etl_pipeline_improved import ETLPipeline, Config, DataTransformer

class TestETLPipelineUnit(unittest.TestCase):
    """Unit tests for individual components of the ETL pipeline."""

    def test_directory_creation(self):
        """Test directory creation functionality."""
        pipeline = ETLPipeline()
        test_dir = Path("test_directory")
        pipeline.create_directory_if_not_exist(test_dir)
        self.assertTrue(test_dir.exists())
        test_dir.rmdir()

    @patch('etl_pipeline_improved.DataExtractor')
    def test_transform_vc_data(self, mock_extractor):
        """Test VC data transformation."""
        test_data = pd.DataFrame({
            'name': ['Company1', 'Company2'],
            ' market ': ["IT", "software"],
            'status': ['active', 'closed'],
            'country_code': ['USA', 'USA'],
            'funding_total_usd': ['1,000,000', '2,000,000'],
            'grant': [100000, 200000],
            'seed': [100000, 200000],
            'venture': [100000, 200000],
            'founded_at': ['2021-01-01', '2020-02-02'],
            'city': ['New York', 'San Francisco']
        })
        
        transformed_df = DataTransformer.transform_vc_data(test_data)
        self.assertIn('Valuation ($B)', transformed_df.columns)
        self.assertIn('Country', transformed_df.columns)
        self.assertEqual(transformed_df['joined_date'].dtype,np.dtype('datetime64[ns]'))
        self.assertEqual(transformed_df['Country'].iloc[0], 'United States')
        
        # Test filtering of US companies
        self.assertEqual(len(transformed_df), 1)  # Only active companies
        self.assertTrue(all(transformed_df['Country'] == 'United States'))
        
        # Test datetime conversion
        self.assertEqual(transformed_df['joined_date'].dtype, np.dtype('datetime64[ns]'))
        self.assertEqual(
            transformed_df['joined_date'].iloc[0],
            pd.Timestamp('2021-01-01')
        )
        # Test string transformations
        self.assertTrue(all(transformed_df['city'].str.contains('_')))
        self.assertTrue(all(transformed_df['city'].str.islower()))
        self.assertEqual(transformed_df['city'].iloc[0], 'new_york')
        
        # Test valuation conversion
        self.assertTrue(pd.api.types.is_float_dtype(transformed_df['Valuation ($B)']))
        self.assertAlmostEqual(transformed_df['Valuation ($B)'].iloc[0], (100000 + 100000 + 100000 + 1000000) / 1000000000)
        
        # Test dropped columns
        self.assertNotIn('Investors', transformed_df.columns)
        self.assertNotIn('Date Joined', transformed_df.columns)
        
        
    @patch('etl_pipeline_improved.DataExtractor')
    def test_transform_income_data(self, mock_extractor):
        """Test household income data transformation"""
        test_data = pd.DataFrame({
            'ZIP': [12345],
            'Year': [2021],
            'Households': [1000],
            'Households Less Than $10,000': [10],
            'Households $10,000 to $14,999': [10],
            'Households $15,000 to $24,999': [10],
            'Households $25,000 to $34,999': [10],
            'Households $35,000 to $49,999': [10],
            'Households $50,000 to $74,999': [10],
            'Households $75,000 to $99,999': [10],
            'Households $100,000 to $149,999': [10],
            'Households $150,000 to $199,999': [10],
            'Households $200,000 or More': [10],
            'Households Median Income (Dollars)': [50000],
            'Households Mean Income (Dollars)': [60000],
            'Geography': ['Test'],
            'Geographic Area Name': ['Test Area']
        })
        
        
        transformed_df = DataTransformer.transform_income_data(test_data)
        self.assertEqual(transformed_df['Year'].iloc[0], 2021)
        self.assertEqual(transformed_df['zip_code'].iloc[0], '12345')
        
        
    def test_transform_unicorn_data(self):
        """Test unicorn data transformation with various scenarios."""
        # Test data
        test_data = pd.DataFrame({
            'Company': ['Company1', 'Company2', 'Company3'],
            'Valuation ($B)': ['$1.5', '$2.0', '$3.0'],
            'Date Joined': ['2021-01-01', '2021-02-01', '2021-03-01'],
            'Country': ['United States', 'Canada', 'United States'],
            'City': ['New York', 'Toronto', 'San Francisco'],  # Column at index 4
            'Industry': ['Tech', 'Finance', 'Health'],
            'Investors': ['Investor1', 'Investor2', 'Investor3']
        })
        
        transformed_df = DataTransformer.transform_unicorn_data(test_data)
        
        # Test filtering of US companies
        self.assertEqual(len(transformed_df), 2)
        self.assertTrue(all(transformed_df['Country'] == 'United States'))
        
        # Test datetime conversion
        self.assertEqual(transformed_df['joined_date'].dtype, np.dtype('datetime64[ns]'))
        self.assertEqual(
            transformed_df['joined_date'].iloc[0],
            pd.Timestamp('2021-01-01')
        )
        # Test city name transformation
        self.assertTrue(all(transformed_df['city'].str.contains('_')))
        self.assertTrue(all(transformed_df['city'].str.islower()))
        self.assertEqual(transformed_df['city'].iloc[0], 'new_york')
        
        # Test valuation conversion
        self.assertTrue(pd.api.types.is_float_dtype(transformed_df['Valuation ($B)']))
        self.assertEqual(transformed_df['Valuation ($B)'].iloc[0], 1.5)
        
        # Test dropped columns
        self.assertNotIn('Investors', transformed_df.columns)
        self.assertNotIn('Date Joined', transformed_df.columns)

    def test_transform_cities_data(self):
        """Test cities data transformation."""
        test_data = pd.DataFrame({
            'city': ['New York', 'New York', 'Los Angeles'],
            'state_name': ['New York', 'New York', 'California'],
            'population': [8000000, 7000000, 4000000],
            'density': [10000, 9000, 8000],
            'zips': ['10001 10002', '10003 10004', '90001 90002'],
            'source': ['source1', 'source2', 'source3'],
            'military': [True, False, True],
            'incorporated': [True, True, False],
            'timezone': ['EST', 'EST', 'PST'],
            'ranking': [1, 2, 3],
            'id': [1, 2, 3],
            'lat': [40.7128, 40.7128, 34.0522],
            'lng': [-74.0060, -74.0060, -118.2437]
        })
        
        transformed_df = DataTransformer.transform_cities_data(test_data)
        
        # Test city selection (should keep highest population per city)
        self.assertEqual(len(transformed_df), 2)  # Unique cities
        ny_pop = transformed_df[transformed_df['city'] == 'new_york']['population'].iloc[0]
        self.assertEqual(ny_pop, 8000000)  # Should keep highest population
        
        # Test string transformations
        self.assertTrue(all(transformed_df['city'].str.islower()))
        self.assertTrue(all(transformed_df['state_name'].str.islower()))
        
        # Test zip code splitting
        self.assertTrue(isinstance(transformed_df['zips'].iloc[0], list))
        
        # Test dropped columns
        dropped_columns = ['source', 'military', 'incorporated', 'timezone', 'ranking', 'id']
        for col in dropped_columns:
            self.assertNotIn(col, transformed_df.columns)

    def test_transform_area_data(self):
        """Test area data transformation."""
        test_data = pd.DataFrame({
            'city': ['New York', 'Los Angeles', 'Chicago'],
            'state': ['New York', 'California', 'Illinois'],
            'land_area_km': [1000, 1200, 800],
            'extra_column': [1, 2, 3]
        })
        
        transformed_df = DataTransformer.transform_area_data(test_data)
        
        # Test column selection
        expected_columns = {'city', 'state', 'land_area_km'}
        self.assertEqual(set(transformed_df.columns), expected_columns)
        
        # Test string transformations
        self.assertTrue(all(transformed_df['city'].str.islower()))
        self.assertTrue(all(transformed_df['state'].str.islower()))
        self.assertEqual(transformed_df['city'].iloc[0], 'new_york')
        self.assertEqual(transformed_df['state'].iloc[0], 'new_york')
        
        # Test data preservation
        self.assertEqual(len(transformed_df), len(test_data))
        self.assertEqual(transformed_df['land_area_km'].iloc[0], 1000)
        
        
    def test_merge_data(self):
        """Test data merging functionality."""
        # Create sample dataframes for merging
        df_vc = pd.DataFrame({
            'Company': ['Company1'],
            'Valuation ($B)': [1.5],
            'Country': ['United States'],
            'city': ['new_york']
        })
        
        df_hhi = pd.DataFrame({
            'zip_code': ['10001'],
            'Households Median Income': [75000],
            'Households Mean Income': [75000]
        })
        
        df_ucs = pd.DataFrame({
            'Company': ['Company2'],
            'Valuation ($B)': [2.0],
            'Country': ['United States'],
            'city': ['new_york']
        })
        
        df_us_cities = pd.DataFrame({
            'city': ['new_york'],
            'state_name': ['new_york'],
            'county_name': ['new_york'],
            'population': [8000000],
            'zips': [['10001', '10002']]
        })
        
        df_us_area = pd.DataFrame({
            'city': ['new_york'],
            'state': ['new_york'],
            'land_area_km': [1000],
            'lat':[42],
            'lng': [-71],
            'density':[50]
        })
        
        merged_df = DataTransformer.merge_data(df_vc, df_hhi, df_ucs, df_us_cities, df_us_area)
        
        # Test merged dataframe properties
        self.assertGreater(len(merged_df), 0)
        
        # Test if all required columns are present
        required_columns = ['Company', 'Valuation ($B)', 'Country', 'city', 
                          'Households Median Income', 'Households Mean Income','land_area_km',"lat",
                            'lng','population','density','state']
        for column in required_columns:
            self.assertIn(column, merged_df.columns)
        
        # Test if the merge preserved the correct relationships
        self.assertEqual(merged_df['city'].iloc[0], 'new_york')
        self.assertEqual(merged_df['zip_code'].iloc[0], '10001')
        
        # Test if concatenation worked
        companies = merged_df['Company'].unique()
        self.assertTrue({'Company1', 'Company2'}.issubset(set(companies)))
        df_vc_missing = pd.DataFrame({
                                    'Company': ['Company5'],
                                    'Valuation ($B)': [5.0],
                                    'Country': ['United States'],
                                    'city': ['missing_city']  # City not in cities dataset
                                    })
        merged_missing = DataTransformer.merge_data(df_vc_missing, df_hhi, df_ucs, df_us_cities, df_us_area)
        self.assertFalse('missing_city' in merged_missing['city'].values)  # Should not include missing cities
    
    
    def test_edge_cases(self):
        """Test edge cases and error handling."""
        # Empty DataFrame test
        empty_df = pd.DataFrame(columns=['city', 'state', 'land_area_km'])
        transformed_empty = DataTransformer.transform_area_data(empty_df)
        self.assertEqual(len(transformed_empty), 0)
        
        # Missing required columns test
        with self.assertRaises(KeyError):
            invalid_df = pd.DataFrame({'wrong_column': [1, 2, 3]})
            DataTransformer.transform_cities_data(invalid_df)
        
        # Test with null values
        df_with_nulls = pd.DataFrame({
            'Company': ['Company1'],
            'Valuation ($B)': ['$1.5'],
            'Date Joined': [None],
            'Country': ['United States'],
            'City': ['New York'],
            'Industry': ['Tech'],
            'Investors': ['Investor1']
        })
        transformed_nulls = DataTransformer.transform_unicorn_data(df_with_nulls)
        self.assertTrue(pd.isna(transformed_nulls['joined_date'].iloc[0]))

if __name__ == '__main__':
    unittest.main(verbosity=2,exit=True,warnings=False)