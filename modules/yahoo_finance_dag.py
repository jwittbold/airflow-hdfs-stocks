from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import timedelta 
import yfinance as yf
import pendulum


########################################################################
##################       TIME / DATE VAR        ########################
########################################################################

# now_EST = pendulum.now('US/Eastern')
today_EDT = pendulum.today('US/Eastern')
today_EDT_str = today_EDT.strftime('%Y-%m-%d')


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

yf_stock_dag = DAG(
    dag_id='marketvol',
    start_date=today_EDT,
    default_args=custom_args,
    description='Download M-F Yahoo Finance stock data for: AAPL, TSLA',
    schedule_interval='0 18 * * 1-5' # run at 6PM M-F  
)


########################################################################
########################     DIRECTORIES        ########################
########################################################################

local_dir = f'/tmp/data/{today_EDT_str}'
hdfs_dir = '/stock_data'


########################################################################
########################       FUNCTIONS        ########################
########################################################################

def download_stock_data(symbol: str, destination: str):
    """
    Downloads daily stock data for AAPL and TSLA symbols.
    :param symbol: The stock ticker symbol to be downloaded. 
    :type symbol: str
    :param destination: The directory to which stock data will be downloaded to.
    :type destination: str
    """

    start_date = today_EDT
    end_date = start_date + timedelta(days=1)

    filename = f'{symbol}_{today_EDT_str}.csv'

    stock_df = yf.download(f'{symbol}', start=start_date, end=end_date, interval='1m')
    df_to_csv = stock_df.to_csv(f'{destination}/{filename}', header=False)


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
    bash_command=f'hdfs dfs -copyFromLocal -d {local_dir}/AAPL_{today_EDT_str}.csv {hdfs_dir}',
    dag=yf_stock_dag
)

# copy TSLA stock data from temp directory to HDFS
t4 = BashOperator(
    task_id='move_TSLA',
    bash_command=f'hdfs dfs -copyFromLocal -d {local_dir}/TSLA_{today_EDT_str}.csv {hdfs_dir}',
    dag=yf_stock_dag
)

# run Spark query on (all) stock data in HDFS to return min/max prices for the day
t5 = SparkSubmitOperator(
    task_id='spark_submit_query',
    application= '/Users/jackwittbold/airflow/dags/spark_jobs/spark_query_stocks.py',
    conn_id= 'spark_default',
    dag=yf_stock_dag
)


# tasks
t0 >> [t1, t2]
t1 >> t3
t2 >> t4
[t3, t4] >> t5