# Data-Engineer - Spotify - Study Case

## Spotify
The main objective of this project is to analyze music trends on the Spotify platform, and recommend popular songs and artists to users based on their listening history and preferences. The project will involve building a batch data pipeline using various Azure services and tools.

### Data Pipeline Steps
1. **Infrastructure**: Provisioning necessary cloud resource using Terraform.
2. **CI/CD**: GitHub Actions served as a CI/CD platform for the Terraform infrastructure.
3. **Data Modeling**: Use Python to create a script that generates dummy data for the Spotify project context, simulating user listening history and preferences.
4. **Data Storage**: Store the generated data in Azure SQL Server.
5. **Data Ingestion**: Use Python with Pandas to move the data from Azure SQL Server to Azure Blob Storage Landing Zone.
6. **Data Preprocessing**: Perform data cleaning and preprocessing using Azure Databricks.
7. **Data Lake Processing Zone**: Save the preprocessed data in Azure Blob Storage Processing Zone.
8. **Data Warehouse**: Load the preprocessed data into Azure Synapse Analytics, which will act as the data warehouse for the project.
9. **Data Transformation**: Use DBT (Data Build Tool) to transform the data and create a denormalized view for analysis.
10. **Data Orchestration**: Orchestrate the data pipeline using with Airflow in Docker.

### Pipeline Diagram
![alt text](https://github.com/makima0499/3.Data-Engineer/blob/main/3.DataPipeline.png)

### Tools
* Python
* Jupyter
* Airflow
* Terraform
* Github Actions
* Docker
* Azure SQL Server
* Azure Blob Storage
* Azure Databricks Storage
* Azure Synapses Analytics
* DBT

### Note
This repository is provided for study purposes only, focusing on data engineering pipelines.

## License

[MIT](https://choosealicense.com/licenses/mit/)
