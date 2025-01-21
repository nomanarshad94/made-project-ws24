# Investigating the Relationship Between Startup Density and Household Income in U.S. Metropolitan Areas

## Overview

This project aims to analyze how the density of startups in U.S. metropolitan areas correlates with household income levels. By leveraging multiple datasets, we will examine if fostering startup ecosystems contributes to economic growth, thus providing valuable insights for policymakers, investors, and urban developers.  

## Research Question  
**Can an increase in the number of startups cause higher average household income in a metropolitan area?**

## Motivation  
Startups play a pivotal role in driving economic innovation and creating job opportunities. Understanding their impact on household income can guide strategic decisions in supporting entrepreneurial ecosystems and improving urban economic policies.

## Datasets

The analysis will utilize five key datasets:

### Datasource1: Startup Data with Fundings and Locations  
- **Metadata URL:** [Kaggle Link](https://www.kaggle.com/datasets/arindam235/startup-investments-crunchbase)  
- **Data URL:** [Dataset Link](https://www.kaggle.com/datasets/arindam235/startup-investments-crunchbase)  
- **Type:** CSV  
- **Description:** Contains investment series at a company level with detailed locations.

### Datasource2: United States Household Income  
- **Metadata URL:** [Kaggle Link](https://www.kaggle.com/datasets/claygendron/us-household-income-by-zip-code-2021-2011)  
- **Data URL:** [Dataset Link](https://www.kaggle.com/datasets/claygendron/us-household-income-by-zip-code-2021-2011?select=us_income_zipcode.csv)  
- **Type:** CSV  
- **Description:** Provides average household income data by metropolitan area, including income distribution bins.

### Datasource3: Unicorn Startups  
- **Metadata URL:** [Kaggle Link](https://www.kaggle.com/datasets/ramjasmaurya/unicorn-startups)  
- **Data URL:** [Dataset Link](https://www.kaggle.com/datasets/ramjasmaurya/unicorn-startups?select=unicorns+till+sep+2022.csv)  
- **Type:** CSV  
- **Description:** Includes information about billion-dollar startups (unicorns), their funding rounds, and founding dates.

### Datasource4: United States Cities Data  
- **Metadata URL:** [SimpleMaps Link](https://simplemaps.com/data/us-cities)  
- **Data URL:** [Dataset Link](https://www.kaggle.com/datasets/sergejnuss/united-states-cities-database)  
- **Type:** CSV  
- **Description:** Contains zip codes, population density, and geographic details for U.S. cities.

### Datasource5: U.S. Cities by Population  
- **Metadata URL:** [Kaggle Link](https://www.kaggle.com/datasets/axeltorbenson/us-cities-by-population-top-330)  
- **Data URL:** [Dataset Link](https://www.kaggle.com/datasets/axeltorbenson/us-cities-by-population-top-330?select=us_cities_by_pop.csv)  
- **Type:** CSV  
- **Description:** Includes statistics like land area and population density, enabling higher-level regional analysis.

## Project Objectives  


1. Aggregate and preprocess data from multiple sources for analysis.  
2. Apply statistical and machine learning methods to identify patterns and relationships.  
3. Explore the relationship between startup density and average household income.  
4. Generate actionable insights to inform policy recommendations.  

## Methodology  

1. **Data Collection:** Download datasets from the provided sources.  
2. **Data Preprocessing:** Clean, merge, and transform data into a unified format.  
3. **Exploratory Data Analysis (EDA):** Use visualizations and statistical summaries to identify trends.  
4. **Modeling:** Apply regression models and statistical tests to analyze the relationship between startup density and household income.  
5. **Insights & Recommendations:** Present findings and propose actionable strategies based on the analysis.  

## Repository Structure  

```
├── data/                     		# Raw and processed datasets  
├── project/						# Parent folder for this project
	├── data_exploration.ipynb/     # Jupyter notebook for analysis  
	├── etl_pipeline_improved.py    # Python scripts for data processing and modeling  
	├── requirements.txt          	# List of Python dependencies  
	├── visuals/  			  		# Contain all graphs and plots generated for project
	├── data-report.pdf				# Data report contains required information for etl pipeline and process   
	├── analysis-report.pdf         # Final analysis report contains results, research  and final analysis
	├── unit_tests.py		        # Python script for unit tests
	├── integration_tests.py        # Python script for integration tests
	├── pipeline.sh         		# Bash script to run etl pipeline
	├── tests.sh         			# Bash script to run unit tests and integration tests
├── README.md                 		# Project overview and documentation  
├── NOTICE.md						# Project details required for Apache 2.0 License
└── LICENSE          				# License file. This project is licensed under Apache 2.0 
```

## Getting Started  

### Prerequisites  

Install the required Python packages:  

```bash
pip install -r requirements.txt
```

### Usage  

1. Clone the repository:  
   ```bash
   git clone <repository_url>
   cd <repository_name>
   ```
2. Running the pipeline.sh will download source datasets and generate transformed data in data folder.  
3. Verify data transformation pipeline by running tests.sh script.
3. Run the notebooks or scripts for preprocessing, analysis, and visualization.

## Future Work  

- Incorporate additional datasets (e.g., demographic and education statistics) for deeper insights.  
- Explore causal relationships using advanced econometric models.  
- Extend analysis to include international startup ecosystems for comparison.  

## Contributing  

Contributions are welcome! Please submit a pull request or open an issue for suggestions.  

## License  

This project is licensed under the Apache 2.0.
