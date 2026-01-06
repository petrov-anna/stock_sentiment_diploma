from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd
import logging

# LOGGING
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SMARTLAB UTILS
MONTHS_RU = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}

def normalize_datetime(raw: str | None, now: datetime | None = None) -> str | None:
    if not raw:
        return None
    raw = raw.strip()
    if now is None:
        now = datetime.now()

    if ":" in raw and len(raw) <= 5 and raw.replace(":", "").isdigit():
        hour, minute = map(int, raw.split(":"))
        return now.replace(hour=hour, minute=minute).strftime("%Y-%m-%d %H:%M")

    if raw.lower().startswith("вчера"):
        time_part = raw.split(",")[1].strip()
        hour, minute = map(int, time_part.split(":"))
        return (now - timedelta(days=1)).replace(
            hour=hour, minute=minute
        ).strftime("%Y-%m-%d %H:%M")

    try:
        date_part, time_part = raw.split(",", 1)
        tokens = date_part.split()
        day = int(tokens[0])
        month = MONTHS_RU[tokens[1].lower()]
        year = int(tokens[2]) if len(tokens) > 2 else now.year
        hour, minute = map(int, time_part.strip().split(":"))
        return datetime(year, month, day, hour, minute).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return None

# SMARTLAB COLLECTOR
def collect_smartlab_comments() -> list[dict]:
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    BASE_URL = "https://smart-lab.ru"
    forum_url = f"{BASE_URL}/forum"

    results = []

    logger.info("Загружаем список тредов SmartLab")
    soup = BeautifulSoup(session.get(forum_url, timeout=10).text, "html.parser")

    for post in soup.select(".post"):
        link_tag = post.select_one("a.post_inner")
        if not link_tag:
            continue

        href = link_tag.get("href")
        thread_url = urljoin(BASE_URL, href.split("#")[0])
        ticker = thread_url.rstrip("/").split("/")[-1]

        logger.info(f"Тикер: {ticker}")

        first_page_html = BeautifulSoup(
            session.get(thread_url, timeout=10).text, "html.parser"
        )

        pag = first_page_html.select_one("#pagination")
        pages = []
        if pag:
            for a in pag.select("a.page"):
                if a.text.isdigit():
                    pages.append(int(a.text))
        last_page = max(pages) if pages else 1

        limit = 200
        start_page = max(1, last_page - limit + 1)

        for page in range(start_page, last_page + 1):
            page_url = f"{thread_url}/page{page}/"

            try:
                soup_comm = BeautifulSoup(
                    session.get(page_url, timeout=10).text, "html.parser"
                )
            except Exception:
                logger.warning(f"Пропуск страницы {page_url}")
                continue

            for comm in soup_comm.select("li.cm_wrap[data-type='comment']"):
                author_tag = comm.select_one("a.a_name") or comm.select_one("a.trader_other")
                text_div = comm.select_one(".cmt_body .text") or comm.select_one(".cmt_body")
                time_tag = comm.select_one("a.a_time")

                if not text_div:
                    continue

                results.append({
                    "datetime": normalize_datetime(time_tag.text if time_tag else None),
                    "text": text_div.get_text(" ", strip=True),
                    "ticker": ticker,
                    "author": author_tag.get_text(strip=True) if author_tag else None,
                })

    logger.info(f"Собрано комментариев: {len(results)}")
    return results

# POSTGRES LOADER
def load_to_postgres(rows: list[dict]):
    if not rows:
        logger.info("Нет данных для загрузки")
        return
    # Используем hook
    hook = PostgresHook(postgres_conn_id="postgres_stocks")
    engine = hook.get_sqlalchemy_engine()
    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

    df.to_sql(
        name="comments_raw",
        schema="stocks",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )

    logger.info(f"Загружено строк: {len(df)}")

# DAG
with DAG(
    dag_id="smartlab_comments_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["smartlab", "stocks", "comments"],
) as dag:

    with TaskGroup("smartlab"):

        @task
        def fetch_smartlab():
            return collect_smartlab_comments()

        @task
        def load_smartlab(rows):
            load_to_postgres(rows)

        load_smartlab(fetch_smartlab())