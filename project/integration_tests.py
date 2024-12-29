import unittest
import pandas as pd
from pathlib import Path
from etl_pipeline_improved import ETLPipeline, Config, DataTransformer

class TestETLPipelineIntegration(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Run the ETL pipeline once before all tests."""
        cls.pipeline = ETLPipeline()
        cls.data_dir = Path("../data")
        cls.output_file = cls.data_dir / Config.FINAL_DATA_FILE_NAME
        cls.pipeline.run(del_tmp_files=True, force_download=False)
        
        
    def setUp(self):
        """Instance-level setup for tests."""
        self.pipeline = self.__class__.pipeline
        self.final_output_path = self.__class__.output_file
    

    def test_output_file_exists(self):
        """Test if the output CSV file is created."""
        self.assertTrue(self.final_output_path.exists(), "Output file was not created.")

    def test_output_file_not_empty(self):
        """Test if the output CSV file is not empty."""
        self.assertGreater(self.final_output_path.stat().st_size, 0, "Output file is empty.")

    def test_output_data_valid(self):
        """Test if the output DataFrame has valid columns and no duplicates."""
        df = pd.read_csv(self.final_output_path)
        required_columns = [
            'city', 'Households', 'Households Mean Income', 'Households Median Income',
            'Valuation ($B)', 'state_name','county_name', 'zip_code', 'Company', 'Industry', 'state_id',
            'county_name', 'population', 'density', 'land_area_km'
        ]
        for col in required_columns:
            self.assertIn(col, df.columns, f"Column '{col}' is missing in the output.")
        self.assertFalse(df.duplicated().any(), "Output data contains duplicate rows.")

    
    def test_pipeline_system(self):
        """System-level test for the entire ETL pipeline."""
        df = self.pipeline.run(del_tmp_files=True, force_download=False)
        self.assertTrue(self.final_output_path.exists(), "Final output file not found.")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0, "Merged DataFrame is empty.")
        required_columns = ['Company', 'Valuation ($B)', 'Country', 'city', 
                          'Households Median Income', 'Households Mean Income','land_area_km',"lat",
                            'lng','population','density','state']
        for col in required_columns:
            self.assertIn(col, df.columns)
    
    
    @classmethod
    def tearDownClass(cls):
        """Cleanup resources after all tests."""
        if cls.output_file.exists():
            cls.output_file.unlink()
            print("Final output file cleaned up.")

if __name__ == '__main__':
    unittest.main(verbosity=2,exit=True,warnings=False)