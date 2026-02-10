import os
import numpy as np
import pandas as pd
from src.database.db_connect import get_engine
from pathlib import Path
from sentence_transformers import SentenceTransformer
from sqlalchemy import text
from src.database.db_connect import get_engine
import logging


def vectorize_news(execution_date):

    engine = get_engine()

    query = text("""
        SELECT id, author, source_id, source_name, title, url, raw_content, is_vectorized, published_at
        FROM stg_news
        WHERE is_vectorized=FALSE;
    """)

    df_raw = pd.read_sql(query, con=engine)
    # print(df_raw)
    df_raw['raw_content'] = df_raw['title'] + df_raw['raw_content'].fillna("")

    model = SentenceTransformer('all-MiniLM-L6-v2')
    embeddings = model.encode(df_raw['raw_content'].tolist())
    
    df_raw['embedded_content'] = embeddings.tolist()
    df_raw = df_raw[['id', 'published_at', 'title', 'embedded_content', 'url']]

    # df_raw['embedded_content'] =  df_raw['embedded_content'].apply(lambda x: x.tolist() if hasattr(x, "tolist") else x)
    df_raw['embedded_content'] = df_raw['embedded_content'].tolist()

    if os.path.exists('/opt/airflow'):
        base_path = Path('/opt/airflow/data')
    else:
        base_path = Path(os.getcwd()) / '../data'

    base_path = f"{base_path}/processed"
    os.makedirs(base_path, exist_ok=True)
    parquet_path = f"{base_path}/embedded_news_{execution_date}.parquet" 

    df_raw.to_parquet(parquet_path, index=False) 

    return parquet_path