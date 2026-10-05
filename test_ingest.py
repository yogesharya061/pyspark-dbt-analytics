from decimal import Decimal
from pyspark.sql import SparkSession
from ingest import clean,aggregate,SCHEMA

def test_quarantine_dedup_and_reconciliation():
 s=SparkSession.builder.master('local[2]').config('spark.sql.shuffle.partitions','2').getOrCreate()
 try:
  df=s.read.option('header',True).schema(SCHEMA).csv('data/orders.csv')
  good,bad=clean(df)
  assert df.count()==7 and good.count()==4 and bad.count()==2
  assert good.filter("order_id='O1'").first().amount==Decimal('120.00')
  dims=s.createDataFrame([('C1','North'),('C2','South'),('C3','West')],'customer_id string, region string')
  assert sorted(aggregate(good,dims,False).collect())==sorted(aggregate(good,dims,True).collect())
  assert sum(r.revenue for r in aggregate(good,dims,True).collect())==Decimal('290.00')
 finally:s.stop()
