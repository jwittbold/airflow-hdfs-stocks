from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta 
import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DecimalType, IntegerType, StructType, StructField
import pendulum


########################################################################
##################         BUILD SPARK          ########################
########################################################################

spark = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('Query_Stocks') \
        .getOrCreate()


########################################################################
##################       TIME / DATE VAR        ########################
########################################################################

now_EST = pendulum.now('US/Eastern')
today_EST = pendulum.today('US/Eastern')
today_EST_str = today_EST.strftime('%Y-%m-%d')


########################################################################
########################          DAG           ########################
########################################################################

custom_args = {
    'owner': 'Jack Wittbold',
    'email': ['jwittbold@gmail.com'],
    'email_on_failure': True,
    'email_on_success': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


# start_date=datetime(2021, 8, 13),
yf_stock_dag = DAG(
    dag_id='marketvol',
    start_date=now_EST,
    default_args=custom_args,
    description='Download M-F Yahoo Finance stock data for: AAPL, TSLA',
    schedule_interval='0 18 * * 1-5' # run at 6PM M-F  
)


########################################################################
########################     DIRECTORIES        ########################
########################################################################

local_dir = f'/tmp/data/{today_EST_str}'
hdfs_dir = '/stock_data'
hdfs_full_path = 'hdfs://localhost:9000/stock_data/*.csv'



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
########################       FUNCTIONS        ########################
########################################################################



def download_stock_data(symbol: str, destination: str):


    start_date = today_EST
    end_date = start_date + timedelta(days=1)

    filename = f'{symbol}_{today_EST_str}.csv'

    stock_df = yf.download(f'{symbol}', start=start_date, end=end_date, interval='1m')
    df_to_csv = stock_df.to_csv(f'{destination}/{filename}', header=False)


def query(hdfs_path: str):

    # hdfs getconf -nnRpcAddresses  # get hadoop namenode:port values 

    stock_data_df = spark.read.option('header', False)\
        .schema(yf_stock_schema)\
        .csv(hdfs_path)

    stock_data_df.printSchema()
    stock_data_df.show(truncate=False)

    stock_data_df.select('high').summary('min','max').show()



########################################################################
########################         TASKS          ########################
########################################################################

# t0 creates private temp directory on local file system '/private/tmp/data/${current_date}'
t0 = BashOperator(
    task_id='mkdir_tmp',
    bash_command='scripts/create_tmp_dir.sh',
    dag=yf_stock_dag
)


# download AAPL stock data to temp directory
t1 = PythonOperator(
    task_id='download_AAPL',
    python_callable=download_stock_data,
    op_kwargs={'symbol': 'AAPL', 'destination': local_dir},
    dag=yf_stock_dag
)


# download TSLA stock data to temp directory
t2 = PythonOperator(
    task_id='download_TSLA',
    python_callable=download_stock_data,
    op_kwargs={'symbol': 'TSLA', 'destination': local_dir},
    dag=yf_stock_dag
)

# copy AAPL stock data from temp directory to HDFS
t3 = BashOperator(
    task_id='move_AAPL',
    bash_command=f'hdfs dfs -copyFromLocal -d {local_dir}/AAPL_{today_EST_str}.csv {hdfs_dir}',
    dag=yf_stock_dag
)

# copy TSLA stock data from temp directory to HDFS
t4 = BashOperator(
    task_id='move_TSLA',
    bash_command=f'hdfs dfs -copyFromLocal -d {local_dir}/TSLA_{today_EST_str}.csv {hdfs_dir}',
    dag=yf_stock_dag
)

# run Spark query on (all) stock data in HDFS to return min/max prices for the day
t5 = PythonOperator(
    task_id='query_stocks',
    python_callable=query,
    op_kwargs={'hdfs_path': hdfs_full_path},
    dag=yf_stock_dag
)


# tasks
t0 >> [t1, t2]
t1 >> t3
t2 >> t4
t5