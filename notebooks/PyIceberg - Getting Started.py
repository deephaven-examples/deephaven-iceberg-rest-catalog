#!/usr/bin/env python
# coding: utf-8

# ![iceberg-logo](https://www.apache.org/logos/res/iceberg/iceberg.png)

# ### [Docker, Spark, and Iceberg: The Fastest Way to Try Iceberg!](https://tabular.io/blog/docker-spark-and-iceberg/)

# In[1]:


from pyiceberg import __version__

__version__


# ## Load NYC Taxi/Limousine Trip Data
# 
# For this notebook, we will use the New York City Taxi and Limousine Commision Trip Record Data that's available on the AWS Open Data Registry. This contains data of trips taken by taxis and for-hire vehicles in New York City. We'll save this into an iceberg table called `taxis`.

# To be able to rerun the notebook several times, let's drop the table if it exists to start fresh.

# In[2]:


get_ipython().run_cell_magic('sql', '', '\nCREATE DATABASE IF NOT EXISTS nyc;\n')


# In[3]:


get_ipython().run_cell_magic('sql', '', 'DROP TABLE IF EXISTS nyc.taxis;\n')


# In[4]:


get_ipython().run_cell_magic('sql', '', 'CREATE TABLE IF NOT EXISTS nyc.taxis (\n    VendorID              bigint,\n    tpep_pickup_datetime  timestamp,\n    tpep_dropoff_datetime timestamp,\n    passenger_count       double,\n    trip_distance         double,\n    RatecodeID            double,\n    store_and_fwd_flag    string,\n    PULocationID          bigint,\n    DOLocationID          bigint,\n    payment_type          bigint,\n    fare_amount           double,\n    extra                 double,\n    mta_tax               double,\n    tip_amount            double,\n    tolls_amount          double,\n    improvement_surcharge double,\n    total_amount          double,\n    congestion_surcharge  double,\n    airport_fee           double\n)\nUSING iceberg\nPARTITIONED BY (days(tpep_pickup_datetime))\n')


# In[5]:


get_ipython().run_cell_magic('sql', '', '\nTRUNCATE TABLE nyc.taxis\n')


# In[6]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Jupyter").getOrCreate()

for filename in [
    "yellow_tripdata_2022-04.parquet",
    "yellow_tripdata_2022-03.parquet",
    "yellow_tripdata_2022-02.parquet",
    "yellow_tripdata_2022-01.parquet",
    "yellow_tripdata_2021-12.parquet",
]:
    df = spark.read.parquet(f"/home/iceberg/data/{filename}")
    df.write.mode("append").saveAsTable("nyc.taxis")


# ## Load data into a PyArrow Dataframe
# 
# We'll fetch the table using the REST catalog that comes with the setup.

# In[7]:


from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThanOrEqual

catalog = load_catalog('default')


# In[ ]:


tbl = catalog.load_table('nyc.taxis')

sc = tbl.scan(row_filter=GreaterThanOrEqual("tpep_pickup_datetime", "2022-01-01T00:00:00.000000+00:00"))


# In[ ]:


df = sc.to_arrow().to_pandas()


# In[ ]:


len(df)


# In[ ]:


df.info()


# In[ ]:


df


# In[ ]:


df.hist(column='fare_amount')


# In[ ]:


import numpy as np
from scipy import stats

stats.zscore(df['fare_amount'])

# Remove everything larger than 3 stddev
df = df[(np.abs(stats.zscore(df['fare_amount'])) < 3)]
# Remove everything below zero
df = df[df['fare_amount'] > 0]


# In[ ]:


df.hist(column='fare_amount')


# # DuckDB
# 
# Use DuckDB to Query the PyArrow Dataframe directly.

# In[ ]:


get_ipython().run_line_magic('load_ext', 'sql')
get_ipython().run_line_magic('config', 'SqlMagic.autopandas = True')
get_ipython().run_line_magic('config', 'SqlMagic.feedback = False')
get_ipython().run_line_magic('config', 'SqlMagic.displaycon = False')
get_ipython().run_line_magic('sql', 'duckdb:///:memory:')


# In[ ]:


get_ipython().run_line_magic('sql', 'SELECT * FROM df LIMIT 20')


# In[ ]:


get_ipython().run_cell_magic('sql', '--save tip-amount --no-execute', '\nSELECT tip_amount\nFROM df\n')


# In[ ]:


get_ipython().run_line_magic('sqlplot', 'histogram --table df --column tip_amount --bins 22 --with tip-amount')


# In[ ]:


get_ipython().run_cell_magic('sql', '--save tip-amount-filtered --no-execute', '\nWITH tip_amount_stddev AS (\n    SELECT STDDEV_POP(tip_amount) AS tip_amount_stddev\n    FROM df\n)\n\nSELECT tip_amount\nFROM df, tip_amount_stddev\nWHERE tip_amount > 0\n  AND tip_amount < tip_amount_stddev * 3\n')


# In[ ]:


get_ipython().run_line_magic('sqlplot', 'histogram --table tip-amount-filtered --column tip_amount --bins 50 --with tip-amount-filtered')


# # Iceberg ❤️ PyArrow and DuckDB
# 
# This notebook shows how you can load data into a PyArrow dataframe and query it using DuckDB easily. Iceberg allows you to take a slice out of the data that you need for your analysis, while reducing the time that you have to wait for the data and without polluting the memory with data that you're not going to use.

# In[ ]:




