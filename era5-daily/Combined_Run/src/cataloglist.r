# Databricks notebook source
# MAGIC %sql
# MAGIC -- Check available catalogs
# MAGIC SHOW CATALOGS;
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC display(dbutils.fs.mounts())
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC import boto3
# MAGIC from botocore import UNSIGNED
# MAGIC from botocore.client import Config
# MAGIC
# MAGIC s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
# MAGIC
# MAGIC bucket_name = "it-gwsc-dvp-prd-s3"
# MAGIC prefix = "prod/trends_era5"
# MAGIC
# MAGIC # Try listing objects explicitly without auth
# MAGIC try:
# MAGIC     response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
# MAGIC     for obj in response.get('Contents', []):
# MAGIC         print(obj['Key'])
# MAGIC except Exception as e:
# MAGIC     print(f"Failed to access bucket: {e}")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
