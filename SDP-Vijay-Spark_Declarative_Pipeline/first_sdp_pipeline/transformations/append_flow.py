from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

#creating empty streaming table
dp.create_streaming_table("Total_sales")

#Appending north sales to the Total Sales table
@dp.append_flow(target ='Total_sales')
def north_sales():
    df = spark.readStream.table("sdp_catalog.source.sales_north")
    return df


#Appending south sales to the Total Sales table
@dp.append_flow(target ='Total_sales')
def south_sales():
    df = spark.readStream.table("sdp_catalog.source.sales_south")
    return df