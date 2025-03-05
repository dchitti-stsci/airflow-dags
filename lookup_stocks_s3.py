from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup

aws_conn_id = "class_datasets_aws_bucket"
bucket_name = "classdatasets"
stocks_filename = "stocks.txt"
subfolder = "instructor"
stock_prices_filename = "stock_prices.txt"
stock_volumes_filename = "stock_volumes.txt"


def get_stock_symbols():
    hook = S3Hook(aws_conn_id=aws_conn_id)
    local_download_path = Path("/tmp")

    csv_local_path = hook.download_file(
        bucket_name=bucket_name,
        key=stocks_filename,
        local_path=str(local_download_path),
    )

    print(f"csv_local_path: {csv_local_path}")

    with open(csv_local_path, "r", encoding="UTF-8") as file:
        return [line.strip() for line in file.readlines()]


def lookup_stock_prices(symbols):
    import yfinance as yf

    prices = {}
    for symbol in symbols:
        stock = yf.Ticker(symbol)
        prices[symbol] = stock.history(period="1d")["Close"][0]

    with open(stock_prices_filename, "w") as file:
        for symbol, price in prices.items():
            file.write(f"{symbol}: {price}\n")
    print(f"Saved stock prices to {stock_prices_filename}")

    hook = S3Hook(aws_conn_id=aws_conn_id)
    key = f"{subfolder}/{stock_prices_filename}"
    hook.load_file(
        filename=stock_prices_filename,
        key=key,
        bucket_name=bucket_name,
        replace=True,
    )
    print(f"Uploaded {stock_prices_filename} to {key}")


def lookup_stock_volumes(symbols):
    import yfinance as yf

    volumes = {}
    for symbol in symbols:
        stock = yf.Ticker(symbol)
        volumes[symbol] = stock.history(period="1d")["Volume"][0]

    with open(stock_volumes_filename, "w") as file:
        for symbol, price in volumes.items():
            file.write(f"{symbol}: {price}\n")
    print(f"Saved stock volumes to {stock_volumes_filename}")

    hook = S3Hook(aws_conn_id=aws_conn_id)
    key = f"{subfolder}/{stock_volumes_filename}"
    hook.load_file(
        filename=stock_volumes_filename,
        key=key,
        bucket_name=bucket_name,
        replace=True,
    )
    print(f"Uploaded {stock_volumes_filename} to {key}")


with DAG(
    dag_id="lookup_stocks_s3",
    schedule=None,
    start_date=pendulum.datetime(2023, 3, 3, tz="UTC"),
    catchup=False,
) as dag:
    install_deps_task = BashOperator(
        task_id="install_deps_task",
        bash_command="pip install yfinance",
    )

    get_stock_symbols_task = PythonOperator(
        task_id="get_stock_symbols_task", python_callable=get_stock_symbols
    )

    with TaskGroup(group_id="lookup_tasks") as lookup_stock_tasks:
        lookup_stock_prices_task = PythonOperator(
            task_id="lookup_stock_prices_task",
            python_callable=lookup_stock_prices,
            op_args=[get_stock_symbols_task.output],
        )

        lookup_stock_volumes_task = PythonOperator(
            task_id="lookup_stock_volumes_task",
            python_callable=lookup_stock_volumes,
            op_args=[get_stock_symbols_task.output],
        )

    install_deps_task >> get_stock_symbols_task >> lookup_stock_tasks
