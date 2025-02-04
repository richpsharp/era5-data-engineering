# ERA5 End-to-End Run Databricks Asset Bundle (DAB)

This GitHub folder follows the **Databricks Asset Bundle (DAB)** structure, organizing the necessary **notebooks, configurations, job definitions, resources, and development tools** for streamlined **CI/CD, deployment, and execution** of the ERA5 climate data pipeline.

## README Table of Contents
1. **Data Workflow Structure** - This section provides a detailed overview of all the jobs executed in the **DAB**. For each 
   job, the tasks are outlined with their purpose and the corresponding notebook locations. Here are the complete list of all the jobs:
   a) `**era5_dev_source_to_staging_job**`
   b) `**era5_dev_source_to_bronze_country_boundaries_job**`
   c) `**era5_staging_to_bronze_interpolation_job**`
   d) `**era5_bronze_to_silver_country_indices_job**` 
   e) `**era5_bronze_to_silver_approximate_join_job**`
   f) `**era5_bronze_to_silver_strict_join_job**` 
  
   For more information on each of these subsections please see **Data Workflow Structure** subsection below.

2. **DAB Folder Structure** - This section covers resources other than the notebooks i.e. the configuration files which 
  support job orchestration and environment management within the DAB. This section is divided into subsections which describe specific files and their function in the context of the DAB:   

  a) `**Job & Task Definitions**`
  b) `**Databricks Asset Bundle Configuration**`
  c) `**3. Databricks Development & IDE Support**`
  d) `**4. Testing & Sample Data**`
  e) `**5. Geospatial Library Setup**`
  f) `**6. Scratch Workspace**`
  g) `**7. Testing Framework**`

  For more information on each of these subsections please see **DAB Folder Structure** subsection below. 

### Data Workflow Structure ("src folder contents")
The pipeline consists of several **Databricks Workflows (Jobs)** that **run in sequence** to process ERA5 climate data. Each workflow executes multiple **tasks**, where each task corresponds to the execution of a **Databricks notebook**.  Tasks within a workflow are **executed sequentially**, with each task **dependent on the successful completion of the preceding task**. The scripts are designed with **conditional execution** to differentiate behavior between **development and production (staging) environments**. Below is the order of operations, along with the location of each corresponding notebook and a brief description of its role.

---

#### `era5_dev_source_to_staging_job`

**Description**: This job contains 3 tasks, the **unit_tests_task** , the **run_on_sample_task** and the **data_quality_spatial_dimension_check_task**. The task **unit_tests_task** executes the unit test, the **run_on_sample_task** moves NetCDF files from raw storage to the staging folder (`era5_gwsc_staging_folder`) and **data_quality_spatial_dimension_check_task** runs a data quality check on the files that were moved to the staging folder.

The task **run_on_sample_task** is configured at the notebook level to transfer only a small number of files when running in the **development** workspace and the transfer the entire dataset (upto December 31st, 2023) when in **staging** workspace.

The **data_quality_spatial_dimension_check_task** verifies the spatial dimensions (latitude/longitude) of the NetCDF files in the `era5_gwsc_staging_folder`. Ensures that NetCDF files adhere to expected **latitude (721 points) and longitude (1440 points) resolutions**.

Below are the notebook links for the abovementioned tasks:
- **Unit Tests**:   
  - 📄 Notebook: [`era5-daily/Combined_Run/tests/01_Source_to_Staging/source-to-staging-run-unit-test.py`](era5-daily/Combined_Run/tests/01_Source_to_Staging/source-to-staging-run-unit-test.py)

- **Run on Sample**:  
  - 📄 Notebook: [`era5-daily/Combined_Run/src/01_Source_to_Staging/01_Source_to_Staging.py`](era5-daily/Combined_Run/src/01_Source_to_Staging/01_Source_to_Staging.py)  

- **Data Quality Spatial Dimension Check**: 
  - 📄 Notebook: [`era5-daily/Combined_Run/src/01_Source_to_Staging/01_Data_Quality_Spatial_Dimension_Check.py`](era5-daily/Combined_Run/src/01_Source_to_Staging/01_Data_Quality_Spatial_Dimension_Check.py)   
  
---

#### `era5_dev_source_to_bronze_country_boundaries_job`

**Description**: This job contains 2 tasks, the **unit_tests_task** and the **run_on_sample_task**. The task **unit_tests_task** executes the unit test, the **run_on_sample_task** creates the bronze table `era5-daily-data.bronze_dev.esri_worldcountryboundaries_global_bronze_test` in the **development** workspace and the `era5-daily-data.bronze_staging.esri_worldcountryboundaries_global_bronze` table in the **staging** workspace from a shapefile which contains world country boundaries

Below are the notebook links for the abovementioned tasks:
- **Unit Tests**:  
  - 📄 Notebook: [`era5-daily/Combined_Run/tests/02_Source_to_Bronze_Country_Boundaries/source_to_bronze_countryboundaries_run-unit-test.py`](era5-daily/Combined_Run/tests/02_Source_to_Bronze_Country_Boundaries/source_to_bronze_countryboundaries_run-unit-test.py)

- **Run on Sample**:  
  - 📄 Notebook: [`era5-daily/Combined_Run/src/02_Source_to_Bronze_Country_Boundaries/02_Source_to_Bronze_Country_Boundaries.py`](era5-daily/Combined_Run/src/02_Source_to_Bronze_Country_Boundaries/02_Source_to_Bronze_Country_Boundaries.py)  
  
---

#### `era5_staging_to_bronze_interpolation_job`

**Description**: This job contains 5 tasks, the **unit_tests_task** , the **run_on_sample_task**, the **data_value_check_task** , the **date_range_check_task** and **data_duplication_check_task**. The **unit_tests_task** executes the unit test, the **run_on_sample_task** processes data from staging the staging folder (`era5_gwsc_staging_folder`) and creates the bronze table `era5-daily-data.bronze_dev.aer_era5_bronze_1950_to_present_dev_interpolation` in the development workspace and the `era5-daily-data.bronze_staging.aer_era5_bronze_1950_to_present_staging_interpolation` in the staging workspace. 

The **data_value_check_task** identifies outlier values for **temperature and precipitation** within the bronze-tier dataset, flagging anomalies based on a predefined criteria and outputs the anomalies as delta tables. The predefined criteria for each  variable are as follows: 

  - **`mean_t2m_c`**: Values outside of `-123.15` and `100` are loaded in the delta table `era5-daily-data.bronze_dev.mean_t2m_c_check` in the **development** workspace and `era5-daily-data.bronze_staging.mean_t2m_c_check` in the **staging** workspace. 
  
  - **`max_t2m_c`**: Values `-123.15` and `100` are loaded in the delta table `era5-daily-data.bronze_dev.max_t2m_c_check` in the **development** workspace and `era5-daily-data.bronze_staging.max_t2m_c_check` in the the **staging** workspace.
  
  - **`min_t2m_c`**: Values `-123.15` and `100` are loaded in the delta table `era5-daily-data.bronze_dev.min_t2m_c_check` in the **development** workspace and `era5-daily-data.bronze_staging.min_t2m_c_check` in the the **staging** workspace.
  
  - **`sum_tp_mm`**: Values `-1` and `100,000` are loaded in the delta table `era5-daily-data.bronze_dev.sum_tp_mm_check` in the **development** workspace and `era5-daily-data.bronze_staging.sum_tp_mm_check` in the the **staging** workspace.
  
The task **date_range_check_task** ensures there are **no missing dates** within the dataset's time range by validating against a complete sequence of expected dates. This task computes **earliest and latest dates** from the dataset. Generates a **full date sequence** and compares against stored records and identifies **any missing dates** and logs them in the delta table `era5-daily-data.bronze_dev.missing_dates` in the development workspace and `era5-daily-data.bronze_staging.missing_dates` in the staging workspace.
  
The task **data_duplication_check_task** is responsible for identifying and handling duplicate entries. It ensures spatial and temporal consistency by checking for duplicate `latitude`, `longitude`, and `time` combinations.

Below are the notebook links for the abovementioned tasks:
- **unit_tests_task**: 
  - 📄 Notebook: [`era5-daily/Combined_Run/tests/02_Staging_to_Bronze/staging-to-bronze-run-unit-test.py`](era5-daily/Combined_Run/tests/02_Staging_to_Bronze/staging-to-bronze-run-unit-test.py)

- **run_on_sample_task**:   
  - 📄 Notebook: [`era5-daily/Combined_Run/src/02_Staging_to_Bronze/02_Staging_to_Bronze.py`](era5-daily/Combined_Run/src/02_Staging_to_Bronze/02_Staging_to_Bronze.py)  
  
- **data_value_check_task**: 
  - 📄 Notebook: [`era5-daily/Combined_Run/src/02_Staging_to_Bronze/02_Date_Range_Check.py`](era5-daily/Combined_Run/src/02_Staging_to_Bronze/02_Date_Range_Check.py)  
  
- **date_range_check_task**:
  - 📄 Notebook: [`era5-daily/Combined_Run/src/02_Staging_to_Bronze/02_Date_Range_Check.py`](era5-daily/Combined_Run/src/02_Staging_to_Bronze/02_Date_Range_Check.py)  

- **data_duplication_check_task**:   
  - 📄 Notebook: [`era5-daily/Combined_Run/src/02_Staging_to_Bronze/02_Check_Data_Duplication.py`](era5-daily/Combined_Run/src/02_Staging_to_Bronze/02_Check_Data_Duplication.py)  

---

#### `era5_bronze_to_silver_country_indices_job`

**Description**: This job consists of seven tasks that process country boundary data from bronze to the silver table. The tasks **unit_tests_task** and **unit_tests_task_2** runs the unit tests. The **`ingest_countries`** task loads raw shapefiles into Delta tables (`countries_raw`), generating unique geometry IDs and ensuring valid geometries. The **`validate_countries`** task standardizes geometries to **SRID=4326 (WGS84)**, repairs invalid shapes, and flattens multi-polygons for efficient processing. The **`tessellate_countries`** task applies **H3 tessellation**, segmenting country boundaries into hexagonal grid cells while handling anti-meridian crossings. This task outputs `era5-daily-data.bronze_dev.countries_h3` in the **development** workspace and `era5-daily-data.bronze_staging.countries_h3` in the **staging** workspace. The **`viz_countries`** task visualizes tessellated boundaries using **Kepler.gl**, enabling geospatial exploration of processed geometries. This task outputs the `era5-daily-data.bronze_dev.countries_h3` delta table in the **development** workspace and `era5-daily-data.bronze_dev.countries_h3` in the **staging** workspace. Finally, the **`union_chips`** task aggregates tessellated grid cells into final country boundaries, validating data integrity before writing results to `era5-daily-data.silver_dev.esri_worldcountryboundaries_global_silver` in the development environment and `era5-daily-data.silver_staging.esri_worldcountryboundaries_global_silver` in the staging workspace for downstream use. Each task is executed sequentially, ensuring spatial consistency, optimized performance, and structured data enrichment in the silver tier.

Below are the notebook links for the abovementioned tasks:
- **unit_tests_task**: 
  - 📄 Notebook: [`era5-daily/Combined_Run/tests/03_Bronze_to_Silver_Country_Indices/bronze-to-silver-countryindices-run-unit-test.py`](era5-daily/Combined_Run/tests/03_Bronze_to_Silver_Country_Indices/bronze-to-silver-countryindices-run-unit-test.py)

- **unit_tests_task_2**: 
  - 📄 Notebook: [`era5-daily/Combined_Run/tests/03_Bronze_to_Silver_Country_Indices/df-tessellate-bronze-to-silver-countryindices-run-unit-test.py`](era5-daily/Combined_Run/tests/03_Bronze_to_Silver_Country_Indices/df-tessellate-bronze-to-silver-countryindices-run-unit-test.py)

- **`ingest_countries`**: 
  - 📄 Notebook: [`era5-daily/Combined_Run/src/03_Bronze_to_Silver_Country_Indices/02_Validate_Countries.py`](era5-daily/Combined_Run/src/03_Bronze_to_Silver_Country_Indices/02_Validate_Countries.py)  
  
- **`validate_countries`**:  
  - 📄 Notebook: [`era5-daily/Combined_Run/src/03_Bronze_to_Silver_Country_Indices/02_Validate_Countries.py`](era5-daily/Combined_Run/src/03_Bronze_to_Silver_Country_Indices/02_Validate_Countries.py)  
    
- **`tessellate_countries`**:  
  - 📄 Notebook: [`era5-daily/Combined_Run/src/03_Bronze_to_Silver_Country_Indices/03_Tessellate_Countries.py`](era5-daily/Combined_Run/src/03_Bronze_to_Silver_Country_Indices/03_Tessellate_Countries.py)  

- **`viz_countries`**: 
  - 📄 Notebook: [`era5-daily/Combined_Run/src/03_Bronze_to_Silver_Country_Indices/04_Viz_Countries.py`](era5-daily/Combined_Run/src/03_Bronze_to_Silver_Country_Indices/04_Viz_Countries.py) 

- **`union_chips`**:  
  - 📄 Notebook: [`era5-daily/Combined_Run/src/03_Bronze_to_Silver_Country_Indices/05_Union_Chips.py`](era5-daily/Combined_Run/src/03_Bronze_to_Silver_Country_Indices/05_Union_Chips.py) 

---

#### `era5_bronze_to_silver_approximate_join_job` 

**Description**: This job consists of 3 tasks. The **unit_tests_task** executes the unit test. The **`run_on_sample_task`** task streams data from the `era5-daily-data.bronze_dev.aer_era5_bronze_1950_to_present_dev_interpolation`, and joins with the `era5-daily-data.silver_dev.esri_worldcountryboundaries_global_silver` in the development workspace. In the staging workspace this joining takes place between the `era5-daily-data.bronze_staging.aer_era5_bronze_1950_to_present_staging_interpolation` and `era5-daily-data.silver_staging.esri_worldcountryboundaries_global_silver`. The merging of the tables take place by joinin records to country polygons based on shared **H3 grid cells**. The type of join performed is an approximate join and what this means is that **H3-based spatial join** includes **points located within a given country** as well as **points slightly outside its borders** if they fall within **H3 cells that intersect the country’s polygon**. This approach simplifies computation but may introduce **minor inaccuracies along country boundaries**. To maintain data integrity, updates are handled by **deleting any matching records first** before inserting new or updated records. This prevents **merge conflicts** caused by multiple records sharing the same key (`latitude`, `longitude`, `time`).

The task **data_duplication_check_task** is responsible for identifying and handling duplicate entries. It ensures spatial and temporal consistency by checking for duplicate `latitude`, `longitude`, and `time` combinations.

Below are the notebook links for the abovementioned tasks:
- **`unit_tests_task`**:   
  - 📄 Notebook: [`tests/03_Bronze_to_Silver_Approximate_Join/test_unit_approximate_join.py`](./tests/03_Bronze_to_Silver_Approximate_Join/test_unit_approximate_join.py)

- **`run_on_sample_task`**:  
  - 📄 Notebook: [`era5-daily/Combined_Run/src/03_Bronze_to_Silver_Approximate_Join/03_Bronze_to_Silver_era5_country_approximate_join_autoloader.py`](era5-daily/Combined_Run/src/03_Bronze_to_Silver_Approximate_Join/03_Bronze_to_Silver_era5_country_approximate_join_autoloader.py)  

- **`data_duplication_check_task`**:   
  - 📄 Notebook: [`era5-daily/Combined_Run/src/03_Bronze_to_Silver_Approximate_Join/03_Check_Data_Duplication_Approximate_Join.py`](era5-daily/Combined_Run/src/03_Bronze_to_Silver_Approximate_Join/03_Check_Data_Duplication_Approximate_Join.py)    



#### `era5_bronze_to_silver_strict_join_job` 

**Description**: This job consists of 3 tasks. The **`unit_tests_task`** executes the unit test. The **`run_on_sample_task`** task streams data from the `era5-daily-data.bronze_dev.aer_era5_bronze_1950_to_present_dev_interpolation`, and joins with the `era5-daily-data.silver_dev.esri_worldcountryboundaries_global_silver` in the development workspace. In the staging workspace this joining takes place between the `era5-daily-data.bronze_staging.aer_era5_bronze_1950_to_present_staging_interpolation` and `era5-daily-data.silver_staging.esri_worldcountryboundaries_global_silver`. The merging of the tables take place by joinin records to country polygons based on shared **H3 grid cells**. The type of join performed is a strict join, which ensures that only ERA5 points definitively within country boundaries are included. The join condition verifies whether a point is either within an H3 cell classified as "core" (entirely inside a country) or within a chip of an H3 cell that intersects a country boundary. This approach provides higher accuracy compared to approximate joins while optimizing performance by avoiding redundant containment checks for each point and polygon. Update processing is streamlined, as each incoming record contains a unique latitude, longitude, and time combination, allowing updates to be handled efficiently through a straightforward MERGE statement, ensuring precise and unambiguous data integration.

The task **`data_duplication_check_task`** is responsible for identifying and handling duplicate entries. It ensures spatial and temporal consistency by checking for duplicate `latitude`, `longitude`, and `time` combinations.

Below are the notebook links for the abovementioned tasks:
- **`unit_tests_task`**:   
  - 📄 Notebook: [`era5-daily/Combined_Run/tests/03_Bronze_to_Silver_Strict_Join/bronze-to-silver-strict-join-run-unit-test.py`](era5-daily/Combined_Run/tests/03_Bronze_to_Silver_Strict_Join/bronze-to-silver-strict-join-run-unit-test.py)

- **`run_on_sample_task`**:  
  - 📄 Notebook: [`era5-daily/Combined_Run/src/03_Bronze_to_Silver_Strict_Join/03_Bronze_to_Silver_era5_country_strict_join_autoloader.py`](era5-daily/Combined_Run/src/03_Bronze_to_Silver_Strict_Join/03_Bronze_to_Silver_era5_country_strict_join_autoloader.py)  

- **`data_duplication_check_task`**:   
  - 📄 Notebook: [`era5-daily/Combined_Run/src/03_Bronze_to_Silver_Strict_Join/03_Check_Data_Duplication_Strict_Join.py`](era5-daily/Combined_Run/src/03_Bronze_to_Silver_Strict_Join/03_Check_Data_Duplication_Strict_Join.py)  
 


### DAB Folder Structure

---

#### **1. Job & Task Definitions**
##### 📁 `resources/`
Contains configuration files (YAML files) that **define job sequences, task dependencies, and execution parameters** for Databricks workflows.

| File | Description |
|------|------------|
| `combined_run_job.yaml` | Defines the **main workflow** that coordinates all jobs, including **source-to-staging**, **bronze table** and **silver table** creation. |
| `staging_to_bronze_job.yaml` | Handles the **staging-to-bronze** data movement and transformation steps. |
| `bronze_to_silver_job.yaml` | Governs the **bronze-to-silver transformation**, including **joins, validations, and aggregations**. |

---

#### **2. Databricks Asset Bundle Configuration**
##### 📄 `databricks.yml`
This is the **main DAB configuration file**, defining the structure of the **Databricks Asset Bundle (DAB)** for managing deployments across environments.

| Section | Description |
|---------|------------|
| **`bundle`** | Defines the **Databricks asset bundle** name (`combined_run`). |
| **`include`** | Specifies which **YAML configuration files** are included in the bundle (e.g., job definitions from `resources/`). |
| **`targets`** | Defines **deployment environments**: `dev`, `staging`, and `prod`, each with its own **Databricks workspace, permissions, and job execution logic**. |

**Deployment Environment Breakdown**
| Environment | Mode | Description |
|------------|------|-------------|
| **`dev`** | `development` | Used for **testing and iteration**. Runs **small-scale samples**, disables automatic triggers, and names resources with `[dev username]` to avoid conflicts. |
| **`staging`** | `production` | Serves as a **pre-production validation step**. Processes full datasets before moving to production. |
| **`prod`** | `production` | Runs **entire workflows at scale** with full data ingestion, quality checks, and transformations. |

🚀 **CI/CD Integration**:  
This configuration ensures **seamless deployments across environments**, leveraging Databricks' **service principals** for authentication and **environment-specific resource allocation**.

---

#### **3. Databricks Development & IDE Support**
##### 📁 `.vscode/`
Contains **VS Code configurations** to optimize development and debugging.

| File | Description |
|------|------------|
| `extensions.json` | Recommends VS Code extensions: **Databricks Extension**, **Pylance**, and **YAML Support**. |
| `settings.json` | Configures **Python analysis, Jupyter support, and Databricks notebook compatibility**. |
| `__builtins__.pyi` | Provides **type hints** for Databricks SDK functions, improving auto-completion and type checking. |

🚀 **Developer Experience Boost**:  
These settings ensure **smooth integration with VS Code**, allowing users to edit, test, and deploy Databricks notebooks efficiently.

---

#### **4. Testing & Sample Data**
##### 📁 `fixtures/`
Contains **test fixtures and sample datasets** for validating the pipeline.

| File | Description |
|------|------------|
| `.gitkeep` | Ensures the `fixtures/` folder remains **tracked by Git**, even when empty. |
| Sample CSV files | Provide **test data** for verifying data quality checks and pipeline transformations. |

---

#### **5. Geospatial Library Setup**
##### 📁 `geospatial/mosaic/gdal/jammy/`
Contains **setup scripts** for enabling **GDAL (Geospatial Data Abstraction Library) and Mosaic** on Databricks.

| File | Description |
|------|------------|
| `mosaic-gdal-init.sh` | Installs **GDAL and Mosaic** for geospatial processing on Databricks clusters. Supports **custom GDAL versions and Mosaic integration**. |

🚀 **Geospatial Processing Enhancement**:  
This script ensures **proper GDAL/Mosaic setup** in Databricks Runtime **13+**, enabling spatial analytics.

---

#### **6. Scratch Workspace**
##### 📁 `scratch/`
This folder is **excluded from Git tracking** (`.gitignore`), serving as a **personal workspace** for users.

| File | Description |
|------|------------|
| `README.md` | Explains that `scratch/` is intended for **exploratory work and local development**. |

🚀 **Safe Experimentation**:  
Users can **store temporary notebooks** here without affecting production code.

---

#### **7. Testing Framework**
##### 📄 `pytest.ini`
Configures **pytest**, the testing framework used in this project.

| Setting | Description |
|---------|------------|
| `[pytest]` | Specifies global pytest configurations. |
| `testpaths` | Ensures that tests are discovered **inside the `tests/` directory**. |
| `pythonpath` | Adds `src/` to the Python path so test scripts can **import source modules correctly**. |

🚀 **Automated Testing**:  
This setup ensures that **pytest can locate and execute tests efficiently**, improving pipeline reliability.

---

#### **8. Development Dependencies**
##### 📄 `requirements-dev.txt`
Lists dependencies required for **local development**.

| Dependency | Purpose |
|------------|----------|
| `databricks-dlt` | Provides support for **Databricks Delta Live Tables**. |
| `pytest` | Enables **unit testing** for pipeline components. |
| `setuptools` & `wheel` | Required for **building Python packages**. |
| `databricks-connect` (optional) | Allows parts of the pipeline to be **run locally**, simulating Databricks execution. |

🚀 **Optimized Development Workflow**:  
This file ensures **all necessary libraries** are installed for local testing and debugging.

---

#### **9. Project Packaging**
#####  📄 `setup.py`
Configures the project as a **Python package** for deployment.

| Section | Description |
|---------|------------|
| `name="combined_run"` | Defines the **package name**. |
| `version=<timestamp>` | Uses a **timestamp-based versioning system** to ensure uniqueness. |
| `packages=find_packages(where="src")` | Automatically identifies modules inside the `src/` directory. |
| `install_requires=["setuptools"]` | Lists **required dependencies** for the package. |

🚀 **Deployment & Distribution**:  
This script allows **the pipeline to be packaged as a wheel file**, facilitating **deployment on Databricks clusters**.

---









 
