from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DecimalType, IntegerType, StructType, StructField

########################################################################
##################         BUILD SPARK          ########################
########################################################################

spark = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('Query_Stocks') \
        .getOrCreate()


########################################################################
########################     STOCK SCHEMA       ########################
########################################################################

yf_stock_schema = StructType([StructField('date_time', StringType(), True),
                            StructField('open', DecimalType(precision=17, scale=14), True),
                            StructField('high', DecimalType(precision=17, scale=14), True),
                            StructField('low', DecimalType(precision=17, scale=14), True),
                            StructField('close', DecimalType(precision=17, scale=14), True),
                            StructField('adj_close', DecimalType(precision=17, scale=14), True),
                            StructField('volume', IntegerType(), True)                         
])


########################################################################
########################     DIRECTORIES        ########################
########################################################################

hdfs_full_path = 'hdfs://localhost:9000/stock_data/*.csv'


########################################################################
########################       FUNCTIONS        ########################
########################################################################

def query(hdfs_path: str):
    """
    Performs query on 'high' trade column, returns DataFrame with min and max trade price on 'high' column.
    :param hdfs_path: The path to stock data on HDFS in the format 'hdfs://<driver ip>:<port>/<directory>'
    :type hdfs_path: str
    """
    # hdfs getconf -nnRpcAddresses  # get hadoop namenode:port values 

    stock_data_df = spark.read.option('header', False)\
        .schema(yf_stock_schema)\
        .csv(hdfs_path)

    stock_data_df.printSchema()
    stock_data_df.show(truncate=False)

    stock_data_df.select('high').summary('min','max').show()


if __name__ == '__main__':

    query(hdfs_full_path)