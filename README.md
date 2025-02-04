
# era5-data-engineering 🌍🔧

## Overview

This repository contains the data processing pipeline that ingests, transforms, and enriches **ERA5-daily weather data**. The pipeline leverages **Databricks Asset Bundles (DAB)** to automate deployments and CI/CD workflows.

This documentation is intended for **Data Scientists, Analysts, and Data Engineers**, detailing the available datasets, where to find relevant code, and insights into the engineering workflow.

### 1. Data Pipeline: Table Outputs

The pipeline processes ERA5-daily weather data (NetCDF climate datasets), structuring outputs into Bronze (raw data) and Silver (analysis-ready) tables. The tables are listed in reverse order of production, meaning that Silver tables depend on the underlying Bronze tables. 

#### Spark Delta Tables (in reverse order)

**Silver Tables:** Analysis-ready products, enriched with country names, and created from the bronze tables.

**Bronze Tables:** Raw data and metadata extracted from NetCDF files, retaining original structure.

### Available Tables

| Tier   | Table Name (`catalog.schema.table`) | Code Location |
|--------|------------------------------------|---------------|
| Silver | `era5-daily-data.silver_staging.aer_era5_silver_approximate_1950_to_present_staging_interpolation_res5`| [Code](era5-daily/Combined_Run/src/03_Bronze_to_Silver_Approximate_Join/03_Bronze_to_Silver_era5_country_approximate_join_autoloader.py) |
| Silver | `era5-daily-data.silver_staging.aer_era5_silver_strict_1950_to_present_staging_interpolation_res5` | [Code](era5-daily/Combined_Run/src/03_Bronze_to_Silver_Strict_Join/03_Bronze_to_Silver_era5_country_strict_join_autoloader.py) |
| Bronze | `era5-daily-data.bronze_staging.aer_era5_bronze_1950_to_present_staging_interpolation`|[Code](era5-daily/Combined_Run/src/02_Staging_to_Bronze/02_Staging_to_Bronze.py) |
| Bronze | `era5-daily-data.bronze_staging.era5_inventory_table` |[Code](era5-daily/Combined_Run/src/01_Source_to_Staging) | 


### 2. CI/CD Workflow for Data Engineers

For **Data Engineers**, this repository implements a CI/CD workflow using **Databricks Asset Bundles (DAB)**, a framework for packaging, deploying, and managing Databricks jobs and workflows, streamlining deployment and pipeline execution.


**How It Works**

- Git: Version Control
- Git Actions: CI/CD build, test and deploy
- Databricks Asset Bundles (DAB): Infrastructure as code resource that describe Databricks resources such as jobs, pipelines, 
  and notebooks as source files. These source files provide an end-to-end definition of a project, including how it should be structured, tested, and deployed, which makes it easier to collaborate on projects during active development
  
  General information on DAB: [See Databricks Asset Bundles Documentation](https://docs.databricks.com/en/dev-tools/bundles/index.html)

Please see the next section on CI/CD files to find more information about the GitHub Actions workflow files 

**Key CI/CD Files** 

The table below provides the location of the GitHub Actions workflow files used to deploy the CI/CD workflow in the dev, staging and production workspaces.

| File | Description |
|------|------------|
| `.github/workflows/era5_combined_workflow_dev.yaml` | GitHub Actions workflow to run CI/CD in the dev workspace |
| `.github/workflows/era5_combined_workflow_staging.yaml` | GitHub Actions workflow to run CI/CD in the staging workspace |
| `.github/workflows/era5_combined_workflow_production.yaml`| GitHub Actions workflow to run CI/CD in the production workspace |

Once deployed the via Github Actions, the DAB is run on Databricks which in turn configures and deploys the pipeline.

For a detailed look at Databricks jobs pertaining to the DAB, the related notebooks, and how they are configured and orchestrated using YAML files, see the [`**Combined_Run**`]((era5-daily/Combined_Run) folder, which contains all necessary configurations and deployment scripts for the DAB



## Contact
- Sambadi Majumder (smajumder1@ua.edu)
- Hobson Bryan (cbryan26@ua.edu)

