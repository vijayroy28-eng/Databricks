from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Define Rules

rules = {
    'rule1' : 'product_id is not null',
    'rule2' : 'updated_AT is NOT null'
}


@dp.table(name = 'products_table')
@dp.expect_all_or_drop(rules)

def products_table():
    df = spark.read.table('sdp_catalog.source.products')
    return df