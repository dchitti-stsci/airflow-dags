import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def get_stock_symbols():
    return ["AAPL", "MSFT", "NVDA", "TSLA"]


def lookup_stock_prices(symbols):
    import yfinance as yf

    prices = {}
    for symbol in symbols:
        stock = yf.Ticker(symbol)
        prices[symbol] = stock.history(period="1d")["Close"][0]
    print(prices)


def lookup_stock_volumes(symbols):
    import yfinance as yf

    volumes = {}
    for symbol in symbols:
        stock = yf.Ticker(symbol)
        volumes[symbol] = stock.history(period="1d")["Volume"][0]
    print(volumes)


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
