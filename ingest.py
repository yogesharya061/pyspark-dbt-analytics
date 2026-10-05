"""Deterministic retail ETL; run from this directory."""
import argparse, json, time
from pathlib import Path
from pyspark.sql import SparkSession, Window, functions as F
SCHEMA = 'order_id string, customer_id string, amount decimal(18,2), updated_at timestamp'

def clean(df):
    valid = (F.col('order_id').isNotNull() & (F.trim('order_id') != '') &
             F.col('customer_id').isNotNull() & (F.trim('customer_id') != '') &
             F.col('amount').isNotNull() & (F.col('amount') >= 0) & F.col('updated_at').isNotNull())
    flagged = df.withColumn('_valid', F.coalesce(valid, F.lit(False)))
    rejected = flagged.filter(~F.col('_valid')).drop('_valid')
    # Deterministic tie-break policy: latest timestamp, then customer, then amount.
    w = Window.partitionBy('order_id').orderBy(F.desc('updated_at'), F.desc('customer_id'), F.desc('amount'))
    accepted = flagged.filter('_valid').drop('_valid').withColumn('_rn',F.row_number().over(w)).filter('_rn = 1').drop('_rn')
    return accepted, rejected

def aggregate(orders, customers, optimized):
    dim = F.broadcast(customers) if optimized else customers.hint('merge')
    return orders.join(dim,'customer_id','left').fillna({'region':'UNKNOWN'}).groupBy('region').agg(
        F.count('*').alias('orders'),F.sum('amount').alias('revenue'))

def run(input_path='data/orders.csv', out='output'):
    spark = SparkSession.builder.master('local[2]').appName('RetailETL').config('spark.sql.shuffle.partitions','4').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    try:
        df = spark.read.option('header',True).schema(SCHEMA).csv(input_path)
        accepted,rejected = clean(df)
        customers = spark.createDataFrame([('C1','North'),('C2','South'),('C3','West')], 'customer_id string, region string')
        accepted.write.mode('overwrite').parquet(out+'/silver')
        rejected.write.mode('overwrite').json(out+'/quarantine')
        gold = aggregate(accepted,customers,True)
        gold.write.mode('overwrite').parquet(out+'/gold')
        metrics={'input_rows':df.count(),'silver_rows':accepted.count(),'invalid_rows':rejected.count()}
        metrics['duplicate_rows_removed']=metrics['input_rows']-metrics['silver_rows']-metrics['invalid_rows']
        Path(out,'metrics.json').write_text(json.dumps(metrics,indent=2))
        gold.orderBy('region').show();print(json.dumps(metrics))
    finally: spark.stop()

if __name__ == '__main__':
    p=argparse.ArgumentParser();p.add_argument('--input',default='data/orders.csv');p.add_argument('--output',default='output')
    a=p.parse_args();run(a.input,a.output)
