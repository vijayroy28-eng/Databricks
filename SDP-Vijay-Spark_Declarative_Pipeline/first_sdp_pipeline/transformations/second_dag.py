from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Table
@dp.table
def src_sales_stream_table():
    df = spark.readStream.table('sdp_catalog.source.sales')
    df = df.withColumn('date',to_date(col('date')))
    return df

#table
@dp.table
def enr_sales_stream():
    df = spark.readStream.table('src_sales_stream_table')
    df = df.withColumn('revenue', col('revenue')*1.05)
    return df

#Materiliazed View
@dp.materialized_view
def cur_sales_stream():
    df = spark.read.table('enr_sales_stream')
    df = df.groupBy('date').agg(sum('revenue').alias('total_revenue'))
    return df
