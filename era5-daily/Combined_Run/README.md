# ERA5 End-to-End Run DAB Repository

[HB] 
To Dos
2. Eliminate redundant folder.
3. Remove author's initial's from title of notebook.
4. Explain other files/folders in "combined_run" folder (ex. "created by DAB/Github").
6. Add else if statements for staging (and eventually prod).
7. Remove "dev" from cluster names in resources YAML.
8. Create dev, staging, and prod copies of country tesselation.
Backlog
10. Add data quality check for duplicate files, timestamps within the file, and post-tess. (Backlog - create Jira ticket.)
11. Review flagging data for duplicant and missing data and files. (Backlog - create Jira ticket.)
12. Add screenshot of data lineage once data is in production. (Backlog - create Jira ticket.)
13. In Union Chips workbook, add a comment explaining the context. (Ex. "H3 cells were split as part of the tessalation process and need to be unioned to eliminate this processing artifact.") (Backlog - create Jira ticket.) 
14. Review each notebook. (Backlog - create Jira ticket.)

This repository contains the workflow and configurations for processing ERA5 climate data using Databricks Asset Bundles (DABs). The workflows are designed to run a sequence of tasks, including unit tests, data quality checks, and processing pipelines that transform data from raw formats (source) to usable datasets (bronze and silver tiers).


## Workflow Overview

Each job in the pipeline is responsible for a specific phase of data processing. The main stages include:
1. **Source to Staging:** This phase involves moving raw data from the source into a staging area.
2. **Staging to Bronze:** Data is validated and transformed into bronze-tier datasets, making them available for further processing.
3. **Bronze to Silver:** The bronze datasets are further processed and transformed into more refined silver-tier datasets.

The repository uses a combination of Databricks clusters and Photon acceleration to optimize performance for large-scale data processing. Each task has dependencies on previous tasks to ensure that processes run in a logical sequence.

### Pipeline Order of Operations

[HB]  The last two "Run on Sample" sub tasks, I would like to discuss to be sure I understand. Why are these needed if we have conditional file processing embedded already?

The pipeline consists of several jobs that run in sequence to process ERA5 climate data. Below is the order of operations:

- **`era5_dev_source_to_staging_job`**  
  - **Unit Tests**: Executes unit tests for the source-to-staging process.
  - **Run on Sample**: Moves a subset of raw data from the source to the staging area.
  - **Data Quality Spatial Dimension Check**: Verifies the spatial dimensions (latitude/longitude) of the NetCDF files in the staging area.

- **`era5_dev_source_to_bronze_country_boundaries_job`**  
  - **Unit Tests**: Executes unit tests for processing country boundaries.
  - **Run on Sample**: Moves a subset of data from the staging area to the bronze tier and processes country boundaries.

- **`era5_dev_staging_to_bronze_job`**  
  - **Unit Tests**: Executes unit tests for the staging-to-bronze process.
  - **Run on Sample**: Processes a subset of data from staging to bronze.
  - **Data Value Check**: Validates data values to ensure consistency and quality.
  - **Date Range Check**: Checks for missing or incorrect date ranges in the data.

- **`era5_bronze_to_silver_country_indices_job`**  
  - **Unit Tests**: Runs unit tests for the bronze-to-silver processing of country indices.
  - **Ingest Countries**: Ingests country data for processing.
  - **Validate Countries**: Validates the ingested country data.
  - **Tessellate Countries**: Tessellates the country boundaries.
  - **Visualize Countries**: Generates visualizations for the tessellated countries.
  - **Union Chips**: Combines country chips for further processing.

- **`era5_bronze_to_silver_approximate_join_job`**  
  - **Unit Tests**: Executes unit tests for approximate joins in the bronze-to-silver process.
  - **Run on Sample**: Processes a subset of data for the approximate join between bronze and silver tiers.

- **`era5_bronze_to_silver_strict_join_job`**  
  - **Unit Tests**: Executes unit tests for strict joins in the bronze-to-silver process.
  - **Run on Sample**: Processes a subset of data for the strict join between bronze and silver tiers.

Each job is dependent on the successful completion of the previous job, ensuring the pipeline runs in a logical sequence.


## Repository Structure

### Key Folders

1. **`src/`**  
   Contains the core notebooks and scripts that handle the actual data processing, such as transforming raw climate data into structured tables.
   
   - **Subfolders:**  
     - **`01_Source_to_Staging/`**  
        This folder contains scripts that handle the movement of data from the raw source to the staging area, with logic to handle conditional execution based on the workspace environment.
     
       - **`01_SM_Source_to_Staging.py`**  
         This script is responsible for moving data from the raw source to the staging area and conditionally creating or updating the Delta table based on the workspace URL. It performs the following actions:
         - **Delta Table Check**:  
           - Defines the schema for the Delta table (`era5_inventory_table`) in the development workspace.
           - Checks if the Delta table exists. If it does, it validates the schema to ensure it matches the defined structure. If the table doesn't exist, it creates a new Delta table with the specified schema.
         - **Conditional File Processing**:  
           - If running in the development workspace, it processes a small subset of data for a given date range (e.g., January 1950) and moves files from the source to the staging area using the function `copy_and_move_files_by_date_and_keep_inventory`. 
           - The function is not executed if the script is running in a non-development workspace.
       
       - **`01_SM_Data_Quality_Spatial_Dimension_Check.py`**  
         This script checks the spatial dimensions of NetCDF files in the staging folder to ensure data quality. It performs the following actions:
         - **Spatial Dimension Validation**:  
           - Defines the expected longitude (1440 points) and latitude (721 points) for the NetCDF files.
           - Uses the function `check_netcdf_files()` to verify that files in the directory meet the expected spatial dimensions.
  
     - **`02_Source_to_Bronze_Country_Boundaries/`**  
        Contains scripts for moving data from the staging area to the bronze tier and processing country boundaries.

       - **`02_SM_Source_to_Bronze_Country_Boundaries.py`**  
         This script processes country boundary shapefiles and transforms them into bronze-tier Delta tables, ensuring spatial consistency through CRS (Coordinate Reference System) transformation. The script includes logic to handle execution based on the workspace environment.
         - **Shapefile Processing**:  
           - Checks if the script is running in the appropriate workspace. If it is, the `process_shapefile_to_delta()` function is used to process the shapefile and store it in a Delta table.
         - The function accepts parameters such as:
           - **`shapefile_path`**: Path to the input shapefile containing country boundaries.
           - **`delta_table_name`**: Name of the Delta table where the processed data will be stored.
           - **`batch_size`**: Defines the batch size for processing large datasets.
           - **`target_crs`**: The EPSG code for the target CRS to which the shapefile will be transformed (e.g., 4326 for WGS 84).
         - **Conditional Execution**:  
           - The script checks the workspace URL to determine whether to run the transformation. If the script is not running in the designated development workspace, it will print a message and exit without executing the function.
     
     - **`02_Staging_to_Bronze/`**  
       This folder contains scripts responsible for moving data from the staging area to the bronze tier, including data quality checks and ensuring data consistency, particularly around date ranges and value boundaries.

       - **`02_HK_SM_Staging_to_Bronze.py`**  
         This script processes files from the staging area and moves them to the bronze-tier Delta table. It utilizes Databricks' autoloader to continuously load data and append it to the bronze table, ensuring smooth streaming ingestion of NetCDF files.
         - **Data Ingestion and Processing**:  
           - The script uses the `netcdf_to_bronze_autoloader()` function to load NetCDF files from the staging area and write them to a Delta table in the bronze tier.
           - Key parameters such as source file location, checkpoint location, and table name are used to ensure the correct data is processed.
           - The function is only executed in the development workspace.

       - **`02_SM_Data_Value_Check.py`**  
         This script performs data quality checks on key variables within the bronze-tier dataset, identifying outlier values for temperature and precipitation. It ensures the data is within expected bounds and outputs any anomalies to separate Delta tables.
         - **Data Value Checks**:
           - The script checks the following variables and writes outlier data to new Delta tables:
             - **`mean_t2m_c`**: Ensures values are between `-123.15` and `100`.
             - **`max_t2m_c`**: Ensures values are between `-123.15` and `100`.
             - **`min_t2m_c`**: Ensures values are between `-123.15` and `100`.
             - **`sum_tp_mm`**: Ensures values are between `-1` and `100,000`.
           - Each check creates a new table (e.g., `mean_t2m_c_check`, `max_t2m_c_check`) that stores the records outside the expected ranges.
         - **Conditional Execution**:
           - The script is only executed in the development workspace and exits without running in non-development environments.

       - **`02_SM_Date_Range_Check.py`**  
         This script ensures that there are no missing dates within the time range covered by the dataset. It checks for any gaps in the sequence of dates and reports missing entries.
         - **Date Range Validation**:
           - The script calculates the earliest and latest dates in the dataset, then generates a sequence of all dates in between.
           - It performs a left anti-join between the generated sequence of dates and the dataset's actual dates to identify any missing days.
           - If missing dates are found, they are displayed; otherwise, a message is printed stating there are no missing dates.
         - **Conditional Execution**:
           - The script is only executed in the development workspace and does not run in other environments.

     
     - **`03_Bronze_to_Silver_Country_Indices/`**  
        This folder contains scripts that handle the transformation of country boundary data from the bronze tier to the silver tier. The processing involves tessellation and validation of the country boundaries.

       - **`01_Ingest_Countries.py`**  
         This script ingests country boundary data from raw shapefiles into a Delta table, ensuring that the data is prepared for further processing in the silver tier.
         - **Ingesting Country Data**:  
           - The script first checks if the target Delta table (`esri_worldcountryboundaries_global_silver`) already exists. If the table exists, the notebook execution is skipped to avoid redundant processing.
           - If the table does not exist, the script proceeds to load the shapefile, transforms the geometries, and validates the data.
           - The following key steps are performed:
             - **Loading Shapefile**: Reads the raw shapefile and transforms the geometries using the `st_isvalid` function to ensure geometries are valid.
             - **Generating Geometry ID**: Creates a unique identifier (`geom_id`) for each geometry using the `xxhash64()` function based on various attributes of the country boundaries.
             - **Writing to Delta Table**: Writes the processed data to a Delta table named `countries_raw` for further downstream processing.
         - **Conditional Execution**:  
           - The script first checks if the target table exists to prevent redundant execution. If the table exists, the notebook exits early; otherwise, the ingestion proceeds as described.

       - **`02_Validate_Countries.py`**  
         This script validates and processes country boundary geometries to prepare them for tessellation and further analysis in the silver tier. The script ensures geometries are transformed to a consistent CRS, checks for invalid geometries, and prepares the data for distributed processing by flattening multi-polygons.

         - **Step 1: Transform All Geometries to 4326**  
           - The script first checks if the target Delta table (`esri_worldcountryboundaries_global_silver`) already exists. If the table exists, the notebook execution is skipped to avoid redundant processing.
           - If the table does not exist, the script proceeds by transforming all country boundary geometries to the **SRID=4326** (WGS84) projection, which is required for H3 indexing.
           - Small islands with no area (SRID=0) and Antarctica are excluded from the transformation.
           - The transformed geometries are stored in the `countries_4326` table.

         - **Step 2: Fix Any Invalid Geometries**  
           - The script uses two UDFs:
             - **`explain_wkt_validity()`**: This function checks the validity of the geometries and provides an explanation for any invalid geometries.
             - **`make_wkt_valid()`**: Attempts to fix invalid geometries, including changing geometry types (e.g., POLYGON to MULTIPOLYGON) to ensure validity. The result is stored back as a valid Well-Known Text (WKT) geometry.

         - **Step 3: Flatten All MultiPolygons**  
           - The script flattens multi-polygons, enabling efficient distributed processing. Key actions include:
             - Storing bounds for each geometry (xmin, ymin, xmax, ymax).
             - Checking for anti-meridian crossings.
             - Calculating the number of points in each flattened polygon.
             - Calculating the area of each geometry.
             - Generating a unique `poly_id` for each polygon, combining `geom_id` and the geometry.
           - The flattened geometries are stored in the `countries_flat` table.

         - **Step 4: Check for Anti-Meridian Crossings**  
           - The script ensures that no individual polygons cross the anti-meridian, even though multi-polygons may do so. If any crossings are found, they are flagged, though no crossings were found in this case.

       - **`03_Tessellate_Countries.py`**  
         This script performs the tessellation of country boundaries, using H3 resolution for spatial indexing. It handles the complex geometry processing required for efficient spatial partitioning, particularly in regions that intersect challenging geographic features like the anti-meridian.

         - **Complex Geometry Handling**:
           - The script first checks if the target Delta table (`esri_worldcountryboundaries_global_silver`) already exists. If the table exists, the notebook execution is skipped to avoid redundant processing.
           - If the table does not exist, the script proceeds and employs UDFs for handling large or complex geometries to avoid redundantly repeating geometries for each row. It stores metadata about geometries in paths (using the `complex_write_udf()` function), ensuring efficient reuse of geometry information during tessellation.
         
         - **Tessellation Process**:
           - **Tessellation Strategy**: The script distinguishes between simple and complex tessellations based on geometry size and point count. If a geometry exceeds defined thresholds for area or number of points, it uses a more complex tessellation strategy. 
           - **Core Identification**: The `complex_core_udf()` checks whether a cell boundary (WKT) is core to the geometry (i.e., fully contained within the boundary).
           - **Anti-Meridian Handling**: The `antimeridian_safe_chip()` function handles issues that arise when geometries are near the anti-meridian, ensuring proper boundary chip generation on either side of the meridian.
         
         - **Boundary Chips and H3 Indexing**:
           - The script calculates the intersection of cell boundaries (from H3 indexing) with the country geometries and stores this information as binary geometries (WKB). The geometries are then tessellated into smaller H3 cells for efficient spatial processing.
           - **Normal Tessellation**: For geometries below the area and point thresholds, the script performs normal H3 tessellation and stores the resulting H3 cell IDs and chips in the `countries_h3` table.
           - **Complex Tessellation**: For larger geometries, the script performs more advanced operations, including boundary chip generation and partitioning to handle the complexity.

         - **Clustering and Optimization**:
           - After tessellation, the script optimizes the resulting `countries_h3` table using Databricks' liquid clustering feature to enhance query performance.

       - **`04_Viz_Countries.py`**  
         This script provides functions for visualizing the tessellated country boundaries using Kepler.gl, a web-based geospatial visualization tool.

         - **Kepler.gl Integration**:  
           - The script first checks if the target Delta table (`esri_worldcountryboundaries_global_silver`) already exists. If the table exists, the notebook execution is skipped to avoid redundant processing.
           - If the table does not exist, the script proceeds and integrates with Kepler.gl to render maps, allowing the visual exploration of country boundaries, H3 tessellation, and spatial data. The `display_kepler()` function is used to render the maps in the Databricks notebook environment.
         - **Helper Functions**:  
           - **`display_kepler()`**: Renders a Kepler.gl map with specified height and width.
           - **`map_render_dfMapItems()`**: Renders one or more layers of spatial data on the map using a Spark DataFrame.
           - **`calc_ZoomInfo()`**: Calculates the appropriate zoom level and map center for the visualization based on the spatial extent of the data.
         
         - **Rendering Options**:
           - The script supports rendering different types of geometries (WKT, WKB, H3 cells) and allows for customization of the map style (e.g., dark mode) and zoom level.
           - **Geospatial Operations**: The script ensures that geometries are transformed to the required projection (SRID=4326) before visualization.

         - **Exception Handling**:
           - If the Kepler.gl library or rendering functions are unavailable, the script catches and logs the error, but ensures that the rest of the notebook can continue execution.

       - **`05_Union_Chips.py`**  
         This script aggregates tessellated country boundary chips, combining them into final geometries in the silver-tier Delta table for streamlined analysis and optimized querying.

         - **Union of Chips**:
           - The script first checks if the target Delta table (`esri_worldcountryboundaries_global_silver`) already exists. If the table exists, the notebook execution is skipped to avoid redundant processing.
           - If the table does not exist, the script performs various validation checks on the tessellated data to ensure that:
             - There are no duplicate core chips (`max_duplicates_of_core_chips`).
             - Each cell ID is consistently classified as core or non-core (`cell_id_core_statuses`).
             - Each `geom_id` corresponds to a unique country (`countries_per_geom_id`).
           - Once validated, it groups the tessellated data by `geom_id`, `country`, `cellid`, and `core`, and performs a union of chips using the `st_union_agg()` function. The unioned chips are stored in a new Delta table (`esri_worldcountryboundaries_global_silver_chips`).

         - **Clustering and Optimization**:
           - The resulting chip table is clustered by `grid_index` and `country` to improve query performance, and Databricks' `OPTIMIZE` command is used to further enhance the performance of the table.

         - **Creation of Final Countries Table**:
           - The script then creates the final tessellated countries table (`esri_worldcountryboundaries_global_silver`), dropping intermediate columns used for analysis and joining the original country information with the unioned chips. 
           - The final table is also clustered and optimized for efficient querying.
  
     - **`03_Bronze_to_Silver_Approximate_Join/`**  
        Contains scripts that automate approximate spatial joins between ERA5 climate data (stored in the bronze tier) and country boundary data (stored in the silver tier) using H3 indexing.

       - **`03_HK_SM_Bronze_to_Silver_era5_country_approximate_join_autoloader.py`**  
         This script performs a spatial join between ERA5 climate data and country boundary data using H3 indexing in the silver tier. It processes data in real-time and handles schema evolution for the silver table.

         - **Key Functionality**:
           - **bronze_to_silver_era5_country_approximate_autoloader**: Streams records from the ERA5 bronze table and joins them with country boundary data based on H3 grid cells. 
           - **merge_era5_with_silver**: Merges the ERA5 changeset into the silver table, ensuring only the latest records are updated.

         - **Stream Processing and Schema Evolution**:
           - Processes records in real-time using Delta Lake features and handles schema evolution in the target silver table.
           - The script uses `merge` operations to ensure that records are updated or inserted based on the sequence of events in the ERA5 data.

         - **Conditional Execution**:
           - The script is only executed in the development workspace. If running in non-development environments, it skips execution.

2. **`tests/`**  
   This folder contains unit tests that ensure the quality and correctness of the various transformation steps. The tests run before the actual tasks to catch any issues.
   - **Subfolders:**  
     - `01_Source_to_Staging`: Unit tests for the source-to-staging process.
     - `02_Source_to_Bronze_Country_Boundaries`: Unit tests for processing country boundaries.
     - `02_Staging_to_Bronze`: Unit tests for the staging-to-bronze process.
     - `03_Bronze_to_Silver_Country_Indices`: Unit tests for the bronze-to-silver process, focusing on country indices.
     - `03_Bronze_to_Silver_Approximate_Join`: Unit tests for the approximate join process between bronze and silver tiers.
     - `03_Bronze_to_Silver_Strict_Join`: Unit tests for the strict join process between bronze and silver tiers.
  
3. **`resources/`**  
   Contains configuration files such as YAML files that define the sequence of jobs and tasks. These configurations set up the clusters, task dependencies, and notebook execution for each stage of the data pipeline.
   - **Important YAML Files:**  
     - `combined_run_job.yaml`: Defines the main workflow that coordinates all jobs, from source-to-staging through to bronze and silver tiers.

  ## THESE FILES ARE GENERATED AS A PART OF THE DABs TEMPLATE 

  **NOTE : DO NOT CHANGE THE CONTENTS OF THESE FOLDERS**
   
4. **`.vscode`**

     The `.vscode` folder contains configuration files specific to Visual Studio Code, providing recommended extensions and environment settings that improve the development experience within this project.

    - **`__builtins__.pyi`**  
        A stub file to provide type hints for Databricks SDK functions, ensuring accurate code completions and type-checking support in Visual Studio Code, specifically with the Pylance extension.

    - **`extensions.json`**  
       Recommends essential VS Code extensions for this project:
       - **Databricks Extension for VS Code**: Integrates Databricks functionality.
       - **Pylance**: Provides high-performance IntelliSense and type-checking for Python.
       - **YAML Support by RedHat**: Enhances YAML editing, useful for managing configuration files.

    - **`settings.json`**  
      Defines workspace-specific settings, including:
      - **Python Analysis and Testing**: Configures Pylance for better Python analysis and enables `pytest` for unit testing.
      - **Environment Variable Loading**: Specifies `.env` files for defining environment variables.
      - **Jupyter Notebook Cell Delimiters**: Customizes cell delimiters to match the Databricks notebook format.
      - **Excluded Files**: Omits temporary files such as `__pycache__`, `.pytest_cache`, and `.egg-info` directories from version control and file explorer views.

5. **`fixtures`**

   The `fixtures` folder serves as a repository for test fixtures, such as sample data files (e.g., CSVs), used for testing and validation purposes within the project.

   - **`.gitkeep`**  
     This placeholder file keeps the `fixtures` directory tracked by Git, even if it’s empty. It also includes instructions for loading a CSV fixture as a DataFrame. The provided Python code demonstrates how to dynamically locate the `fixtures` folder, making it accessible in both Databricks and local environments. 

6. **`geospatial/mosaic/gdal/jammy`**

   This folder contains a shell script (`mosaic-gdal-init.sh`) that sets up the GDAL (Geospatial Data Abstraction Library) environment on an Ubuntu 22.04 (Jammy) system, specifically for Databricks Runtime (DBR) 13+ environments. This script provides essential configurations for geospatial data processing, integrating GDAL and Mosaic libraries in Databricks.

    - **`mosaic-gdal-init.sh`**  
     This script is a setup utility for configuring GDAL and Mosaic on Ubuntu-based Databricks clusters. The script is configurable via several conditional variables, allowing the user to control installations based on specific needs:

        - **GDAL Installation**:
          - Installs GDAL version 3.4.1 by default or 3.4.3 with the optional UbuntuGIS PPA.
          - Configures repository sources and installs native dependencies for geospatial processing (e.g., `unixodbc`, `libcurl3-gnutls`, and GDAL bindings).
          - Copies pre-built JNI shared objects from either a local path or GitHub, based on configuration.

        - **Mosaic Installation**:
          - Installs the Mosaic library, a tool for geospatial data processing in Databricks, controlled by the `WITH_MOSAIC` flag.
          - Uses the `MOSAIC_PIP_VERSION` variable to set the specific version of Mosaic for installation.

7. **`scratch`**

   This folder is intended for personal and exploratory notebooks, allowing users to experiment without affecting the core codebase. By default, the `scratch` folder is ignored in Git (as specified in `.gitignore`), meaning files and notebooks saved here won’t be committed to the repository. This setup helps maintain a clean project structure by separating exploratory work from production code.

    - **`README.md`**  
         Provides a brief description of the folder's purpose and clarifies its exclusion from Git tracking.

8. **`databricks.yml`**

   This configuration file defines a Databricks Asset Bundle (`combined_run`) for managing environments across development, staging, and production. It specifies deployment targets with settings to ensure resources are correctly prefixed and scheduled, depending on the environment (development or production).

  - **`bundle`**  
    - **`name`**: The name of the Databricks asset bundle, `combined_run`.

  - **`include`**  
    - Specifies resource files (`*.yml`) to include in the bundle, typically located in the `resources` folder.

  - **`targets`**  
     Defines specific environments (development and production) with distinct configurations:
  
  - **`dev` (default)**  
    - Mode: `development`, used for personal testing and experimentation. Resources are prefixed with `[dev my_user_name]` to avoid collisions, and schedules are paused by default.
    - Workspace: Points to the development workspace host URL.

  - **`prod`**  
    - Mode: `production`, used for official deployment. Enforces strict settings and verification to ensure stability and compliance.
    - Workspace:
      - Host: Specifies the production Databricks workspace.
      - Root Path: Sets the root path for resources in production, using a dynamic path that incorporates the bundle name and target.
    - `run_as`: Specifies the user (`smajumder1@ua.edu`) to execute production deployments, ensuring consistent permissions and accountability.

     This setup facilitates efficient CI/CD for Databricks assets, allowing easy switching between development and production environments.

9. **`pytest.ini`**

   The `pytest.ini` file configures `pytest`, a testing framework for Python, to specify test discovery paths and Python source paths.

   - **`[pytest]`**  
      Declares that the following configurations apply to `pytest`.

    - **`testpaths`**  
     Specifies the directory `tests` as the root path for discovering test files. This allows `pytest` to automatically locate and execute tests within the `tests` folder.

    - **`pythonpath`**  
     Adds the `src` directory to the Python path, enabling `pytest` to find and import modules within `src` during testing. This ensures that tests can access the source code without modifying the system's Python path.

     This setup enables structured testing within the repository, making it easy for `pytest` to locate and execute tests while ensuring access to necessary source files.

10. **`requirements-dev.txt`**

     The `requirements-dev.txt` file lists dependencies required for local development and testing of this project. 

    - **Core Dependencies**
       - `databricks-dlt`  
          Provides code completion and support for working with Databricks Delta Live Tables (DLT).
  
       - `pytest`  
          A testing framework for Python, used as the default testing tool for this project.
  
        - `setuptools` and `wheel`  
          Dependencies for building Python packages (wheel files), facilitating distribution and installation.

   - **Optional Dependencies**
       - `databricks-connect`  
       Allows parts of the project to be run locally, simulating the Databricks environment.  
        - This is especially useful when using the Databricks extension for Visual Studio Code, which can auto-install `databricks-connect`. 
        - For manual installation, the configuration here suggests using a compatible version with Databricks Runtime `13.3`. Uncomment the line with `databricks-connect` to install it if not using Visual Studio Code.

        This file is intended for development dependencies, separate from the dependencies required for Databricks Workflows. 

11. **`setup.py`**

     The `setup.py` file is the configuration script for building and packaging the `combined_run` project. This script is used by `setuptools` to package the project as a wheel file for deployment and distribution.

     **Key Sections of `setup.py`:**

       - **Basic Configuration:**
          - **Project Name**: The project is named `"combined_run"`.
          - **Versioning**: The version includes the `combined_run` package version concatenated with a UTC timestamp, ensuring each build is unique and picked up by all-purpose clusters.
          - **Author and URL**: Contains author contact (`smajumder1@ua.edu`) and a URL (`https://databricks.com`).

     - **Source and Packaging:**
         - **Source Directory**: The code for this project resides in the `src` folder.
         - **Packages**: `find_packages(where='./src')` automatically identifies packages in the `src` directory for inclusion.

     - **Entry Points:**
        - Defines `"main=combined_run.main:main"` as the entry point for the project, allowing `combined_run` to be launched via `main` as the primary script.

    - **Dependencies:**
  - **`install_requires`**: Lists dependencies to be installed if the wheel is used as a library dependency. The `setuptools` package is included for building and installing the package.
  
- **Usage Notes**:
  - **Wheel File**: The setup configuration allows this project to be packaged as a wheel file for convenient deployment. 
  - **Dependencies**: If the wheel is used in a Databricks environment, additional dependencies should be defined in the environment-specific configuration.

  This script should generally not be executed directly; instead, follow the instructions in the `README.md` for deploying, testing, and running the project.






 
