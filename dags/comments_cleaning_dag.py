from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import re
from bs4 import BeautifulSoup
from bs4 import MarkupResemblesLocatorWarning
from nltk.corpus import stopwords
import nltk
from pymorphy3 import MorphAnalyzer
import warnings
import logging

# НАСТРОЙКИ
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)
nltk.download("stopwords")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# STOPWORDS
RUS_STOPWORDS = set(stopwords.words("russian"))

FIN_STOPWORDS = {
    "акция", "акции", "рынок", "рынке", "курс", "котировка",
    "компания", "компании", "инвестор", "инвесторы",
    "купить", "продать", "финансы", "рост", "падение",
    "доллар", "рубль", "рублей", "деньги"
}

ALL_STOPWORDS = RUS_STOPWORDS.union(FIN_STOPWORDS)
morph = MorphAnalyzer()

MIN_TEXT_LENGTH = 10
CHUNK_SIZE = 2_000


# CLEANING FUNCTIONS
def clean_html(text: str) -> str:
    return BeautifulSoup(text, "lxml").get_text(" ")

def remove_links(text: str) -> str:
    return re.sub(r"https?://\S+|www\.\S+", " ", text)

def remove_special_chars(text: str) -> str:
    return re.sub(r"[^а-яА-ЯёЁa-zA-Z0-9\s]", " ", text)

def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()

def lemmatize_text(text: str) -> str:
    lemmas = []
    for word in text.split():
        p = morph.parse(word)[0]
        lemmas.append(p.normal_form)
    return " ".join(lemmas)

def remove_stopwords(text: str) -> str:
    return " ".join([w for w in text.split() if w not in ALL_STOPWORDS])

def clean_text(text: str) -> str:
    text = str(text)
    text = clean_html(text)
    text = remove_links(text)
    text = remove_special_chars(text)
    text = text.lower()
    text = lemmatize_text(text)
    text = remove_stopwords(text)
    text = normalize_whitespace(text)
    return text


# DAG
with DAG(
    dag_id="comments_cleaning",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["nlp", "sentiment", "cleaning"],
) as dag:

    @task
    def clean_comments(execution_date=None):
        hook = PostgresHook(postgres_conn_id="postgres_stocks")
        engine = hook.get_sqlalchemy_engine()

        if execution_date is None:
            execution_date = datetime.utcnow()

        since = execution_date - timedelta(days=1)

        logger.info(f"Очистка комментариев с {since}")

        query = f"""
            SELECT
                datetime,
                text AS text_raw,
                ticker,
                author
            FROM stocks.comments_raw
            WHERE datetime >= '{since}'
        """

        total_written = 0

        for chunk in pd.read_sql(query, engine, chunksize=CHUNK_SIZE):
            logger.info(f"Обработка чанка: {len(chunk)} строк")

            chunk["text_clean"] = chunk["text_raw"].astype(str).apply(clean_text)

            chunk = chunk[chunk["text_clean"].str.strip() != ""]
            chunk = chunk[chunk["text_clean"].str.len() >= MIN_TEXT_LENGTH]
            chunk = chunk.drop_duplicates(subset=["text_clean", "ticker"])

            if chunk.empty:
                continue

            chunk = chunk[[
                "datetime",
                "text_raw",
                "text_clean",
                "ticker",
                "author"
            ]]

            chunk.to_sql(
                name="comments_clean",
                schema="stocks",
                con=engine,
                if_exists="append",
                index=False,
                method="multi",
            )

            total_written += len(chunk)
            logger.info(f"Записано строк: {len(chunk)}")

        logger.info(f"Всего записано: {total_written}")

    clean_comments()
