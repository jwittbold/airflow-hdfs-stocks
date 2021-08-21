from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta 
import yfinance as yf
import pendulum


########################################################################
##################       TIME / DATE VAR        ########################
########################################################################

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
    start_date=datetime(2021, 8, 18),
    default_args=custom_args,
    description='Download M-F Yahoo Finance stock data for: AAPL, TSLA',
    schedule_interval='0 22 * * 1-5' # run at 6PM M-F EDT (+ 4hrs for UTC offset)
)


########################################################################
########################     DIRECTORIES        ########################
########################################################################

local_dir = f'/tmp/data/{today_EDT_str}'
hdfs_dir = '/stock_data'


########################################################################
########################       FUNCTIONS        ########################
########################################################################

def download_stock_data(ticker: str,  period: str, interval: str, destination: str):
    """
    Downloads daily stock data for specified ticker for defined period and interval.
    Saves as {TICKER}_{DATE}.csv (date is EDT). File written to destination param.
    :param ticker: The stock ticker to be downloaded. 
    :type ticker: str
    :param period: Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max (yfincance download method param) 
    :type period: str
    :param interval: Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo (yfincance download method param)
    :type interval: str
    :param destination: The directory to which stock data will be downloaded to.
    :type destination: str
    """

    filename = f'{ticker}_{today_EDT_str}.csv'

    stock_df = yf.download(f'{ticker}', period=f'{period}', interval=f'{interval}')
    stock_df.to_csv(f'{destination}/{filename}', header=False)


########################################################################
########################         TASKS          ########################
########################################################################

# creates directory on local file system '/private/tmp/data/${current_date}' 
# ${current_date} is set off current date for EDT time zone at time of script execution.
t0 = BashOperator(
    task_id='mkdir_tmp',
    bash_command='scripts/create_tmp_dir.sh',
    dag=yf_stock_dag
)

# download AAPL stock data to temp directory
t1 = PythonOperator(
    task_id='download_AAPL',
    python_callable=download_stock_data,
    op_kwargs={'ticker': 'AAPL', 'period': '1d', 'interval': '1m', 'destination': local_dir},
    dag=yf_stock_dag
)

# download TSLA stock data to temp directory
t2 = PythonOperator(
    task_id='download_TSLA',
    python_callable=download_stock_data,
    op_kwargs={'ticker': 'TSLA', 'period': '1d', 'interval': '1m', 'destination': local_dir},
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
    application='~/airflow/dags/spark_jobs/spark_query_stocks.py',
    conn_id='spark_default',
    dag=yf_stock_dag
)


# tasks
t0 >> [t1, t2]
t1 >> t3
t2 >> t4
[t3, t4] >> t5