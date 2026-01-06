from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import logging

# LOGGING
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MOEX SETTINGS
TICKERS = ["GAZP"]
INTERVAL = 24
BASE_URL = "https://iss.moex.com/iss/engines/stock/markets/shares/securities"

# MOEX COLLECTOR
def fetch_moex_candles(
    ticker: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    url = (
        f"{BASE_URL}/{ticker}/candles.json"
        f"?from={start_date}&till={end_date}&interval={INTERVAL}"
    )

    logger.info(f"Запрос MOEX: {ticker} {start_date} → {end_date}")
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()
    candles = data.get("candles", {})
    columns = candles.get("columns", [])
    rows = candles.get("data", [])

    if not rows:
        logger.warning(f"Нет данных MOEX для {ticker}")
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=columns)

    df = df.rename(columns={
        "begin": "date",
        "open": "open",
        "close": "close",
        "high": "high",
        "low": "low",
        "value": "value",
        "volume": "volume",
    })

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["ticker"] = ticker

    return df[[
        "date", "open", "close", "high", "low", "value", "volume", "ticker"
    ]]

# POSTGRES LOADER
def load_to_postgres(df):
    # Используем hook
    hook = PostgresHook(postgres_conn_id="postgres_stocks")
    engine = hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        result = conn.execute("SELECT current_database(), current_schema();")
        print(result.fetchone())
    df.to_sql(
        name="stock_markets",
        schema="stocks",
        con=engine,
        if_exists="append",
        index=False,
        method="multi"
    )

    logger.info(f"Загружено строк: {len(df)}")

# DAG
with DAG(
    dag_id="moex_prices_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["moex", "stocks", "prices"],
) as dag:

    @task
    def collect_moex(execution_date=None):
        """
        Берём данные за предыдущий день
        """
        if execution_date is None:
            execution_date = datetime.utcnow()

        end_date = execution_date.date()
        start_date = end_date - timedelta(days=50)

        all_frames = []

        for ticker in TICKERS:
            df = fetch_moex_candles(
                ticker=ticker,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
            )
            all_frames.append(df)

        if not all_frames:
            return pd.DataFrame()

        return pd.concat(all_frames, ignore_index=True)

    @task
    def load_moex(df: pd.DataFrame):
        load_to_postgres(df)

    load_moex(collect_moex())