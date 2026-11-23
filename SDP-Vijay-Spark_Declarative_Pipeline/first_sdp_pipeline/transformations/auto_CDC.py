from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Create empty streaming table SCD 2
dp.create_streaming_table('products_stream_scd_2')


#Create empty streaming table SCD 1
dp.create_streaming_table('products_stream_scd_1')


# Streaming View on Source

@dp.temporary_view()
def products_source_view():
    df = spark.readStream.table("sdp_catalog.source.products")
    return df

#SCD TYPE 2
dp.create_auto_cdc_flow(
    target="products_stream_scd_2",
    source="products_source_view", # temp view created at line 15
    keys=["product_id"],
    sequence_by="updated_AT",
    except_column_list=["updated_AT"],
    stored_as_scd_type=2 # For SCD Type 2
)

#SCD TYPE 1
dp.create_auto_cdc_flow(
    target="products_stream_scd_1",
    source="products_source_view",
    keys=["product_id"],
    sequence_by="updated_AT",
    except_column_list=["updated_AT"],
    stored_as_scd_type=1 # For SCD Type 1
)
















