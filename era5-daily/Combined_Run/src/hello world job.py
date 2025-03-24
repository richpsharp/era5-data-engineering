# Databricks notebook source
dbutils.widgets.text('input', 'none', 'Input')

# COMMAND ----------

dbutils.notebook.exit(dbutils.widgets.get('input'))
