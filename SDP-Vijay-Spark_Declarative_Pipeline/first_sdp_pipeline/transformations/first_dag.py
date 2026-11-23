from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Materiliazed View
@dp.materialized_view
def src_sales():
    df = spark.read.table('sdp_catalog.source.sales')
    df = df.withColumn('date',to_date(col('date')))
    return df

#Materiliazed View
@dp.materialized_view
def enr_sales():
    df = spark.read.table('src_sales')
    df = df.withColumn('revenue', col('revenue')*1.05)
    return df

#Materiliazed View
@dp.materialized_view
def cur_sales():
    df = spark.read.table('enr_sales')
    df = df.groupBy('date').agg(sum('revenue').alias('total_revenue'))
    return df


# WRITING THE ABOVE SAME CODE USING SQL
# -- Materialized View: src_sales
# CREATE MATERIALIZED VIEW sdp_catalog.target.src_sales AS
# SELECT
#   *,
#   CAST(date AS DATE) AS date
# FROM sdp_catalog.source.sales;

# -- Materialized View: enr_sales
# CREATE MATERIALIZED VIEW sdp_catalog.target.enr_sales AS
# SELECT
#   *,
#   revenue * 1.05 AS revenue
# FROM sdp_catalog.target.src_sales;

# -- Materialized View: cur_sales
# CREATE MATERIALIZED VIEW sdp_catalog.target.cur_sales AS
# SELECT
#   date,
#   SUM(revenue) AS total_revenue
# FROM sdp_catalog.target.enr_sales
# GROUP BY date;
