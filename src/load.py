import numpy as np
import pandas as pd
import logging
from sqlalchemy import text
from src.database.db_connect import get_engine

logger = logging.getLogger(__name__)

def load_price(df_price):
    if df_price.empty:
        return
    print(df_price.columns)
    
    engine = get_engine()

    query = text(""" 
        INSERT INTO fact_prices (asset_id, datetime, price_close, volume)
        VALUES (:asset_id, :Datetime, :Close, :Volume)
        ON CONFLICT (asset_id, datetime) 
        DO UPDATE SET 
            price_close = EXCLUDED.price_close,
            volume = EXCLUDED.volume;
    """)

    with engine.begin() as conn:
        conn.execute(query, df_price.to_dict('records'))


def load_raw_news(parquet_path):
    
    df_news = pd.read_parquet(parquet_path)

    query = text("""
        INSERT INTO stg_news (author, source_id, source_name, title, url, raw_content, published_at)
        VALUES (:author, :source_id, :source_name, :title, :url, :full_content, :publishedAt)
        ON CONFLICT (url)
        DO NOTHING;
    """)

    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(query, df_news.to_dict('records'))


def load_embedded_news(ti):
    try:
        vector_path = ti.xcom_pull(task_ids='vectorize_step')
    except:
        vector_path = ti

    df_news = pd.read_parquet(vector_path)

    df_news['embedded_content'] = df_news['embedded_content'].apply(
        lambda x: x.tolist() if isinstance(x, np.ndarray) else x
    )
    engine = get_engine()
    
    query = text("""
        INSERT INTO fact_news_vector (stg_id, datetime, title, embedding, url)
        VALUES (:id, :published_at, :title, :embedded_content, :url)
        ON CONFLICT (stg_id)
        DO NOTHING;
    """)
    
    with engine.begin() as conn:
        conn.execute(query, df_news.to_dict('records'))

    processed_ids = df_news['id'].tolist()

    if processed_ids:
        with engine.begin() as conn:
            update_query = text("""
                UPDATE stg_news 
                SET is_vectorized = TRUE 
                WHERE stg_id = ANY(:ids);
            """)

            conn.execute(update_query, {"ids": processed_ids})
            print(f"Successfully updated {len(processed_ids)} rows in stg_news")