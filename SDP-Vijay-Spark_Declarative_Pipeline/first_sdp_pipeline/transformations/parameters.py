from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
import ast


list_var = spark.conf.get("tables_list")


list_var_list = ast.literal_eval(list_var)

for i in list_var_list:
    @dp.table(name=f"table_{i}")
    def table():
        df = spark.readStream.table('sdp_catalog.source.sales')
        return df
