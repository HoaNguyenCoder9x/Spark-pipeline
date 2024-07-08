from utilities import *
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, col
import varriable as vars
def build_spark_session():
    findspark.init()
    spark = SparkSession.builder\
            .appName(name='spark-examples')\
            .getOrCreate()
    return spark

def extract(sparksession,data_path, file_format = 'csv', file_deli = '\t'):
    if file_format == 'csv':
        df = sparksession.read.csv(path=data_path,sep=file_deli, header=True, inferSchema=True)
    elif file_format == 'parquet':
        pass
    elif file_format == 'xlsx':
        pass
    else:
        return 'Recheck your file format'
    
    # columns name transformation    
    df = df.select([col(c).alias(columns_formatted(df).get(c, c)) for c in df.columns])
    return df

def transform(df):
    pass

def load(df , mode = "overwrite" ,is_partition = False , sink_path = None ):

    if is_partition:
        df.write.mode(mode).parquet(sink_path)
        print('Write partition success')
    else:
        print('Nothing to do')



def closed_spark_session(spark):
    return spark.stop()






# Buid Spark Session
sparkSession = build_spark_session()
# Extract data and do a bit transformation
data = extract(sparksession=sparkSession,data_path=vars.data_path, file_format= 'csv')
# Write data to file system
load(df=data,is_partition= True, sink_path= vars.sink_path)
# Close Spark Session
closed_spark_session(sparkSession)
