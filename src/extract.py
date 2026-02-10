import os
import numpy as np
import pandas as pd
import time
from newsapi import NewsApiClient
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta
from newspaper import Article
from sentence_transformers import SentenceTransformer
from src.database.db_connect import get_engine
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import text
import logging

NEWS_API_KEY = os.getenv('NEWS_API_KEY')
newsapi = NewsApiClient(api_key=NEWS_API_KEY)

logger = logging.getLogger(__name__)

def get_asset_id():
    engine = get_engine()
    query = 'SELECT asset_id FROM dim_assets'

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    return df['asset_id'].to_list()


def get_asset_name():
    engine = get_engine()
    query = 'SELECT asset_name FROM dim_assets'

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    return df['asset_name'].to_list()


def fetch_price(execution_date):

    start_dt = execution_date
    end_dt = (pd.to_datetime(execution_date) + timedelta(days=1)).strftime('%Y-%m-%d')

    assets = get_asset_id()
    # print(assets)
    logging.info('fetch stock list successfully')

    all_df = []

    for asset in assets:
        try:
            ticker = yf.Ticker(asset)
            data = ticker.history(start=start_dt, end=end_dt, interval="1h")

            if data.empty:
                logger.info(f"Cannot fetch {asset} -- No Data Found from {start_dt} to {end_dt}")
                time.sleep(2)
                continue
            else:
                data = data.reset_index()
                data['asset_id'] = asset
                data = data[['asset_id', 'Datetime', 'Close', 'Volume']]

                all_df.append(data)
                logger.info(f"Fetch {len(data)} rows successfully for {asset}")
                time.sleep(2)

        except Exception as e:
            logger.info(f"Cannot fetch {asset} from YF -- Error happened")
            time.sleep(2)
            continue

    
    if all_df:
        final_df = pd.concat(all_df, ignore_index=True)
        return final_df
    else:
        return pd.DataFrame()    
         
    
def fetch_raw_news(execution_date):

    start_dt = execution_date
    end_dt = (pd.to_datetime(execution_date) + timedelta(days=1)).strftime('%Y-%m-%d')
    assets_name = get_asset_name()
    # print(assets)

    query_cmd = '"XAG/USD" OR '

    for name in assets_name:
        query_cmd = query_cmd + f'"{name}"'

        if name != assets_name[-1]:
            query_cmd = query_cmd + " OR "
    
    query = f'({query_cmd})'
    # print(query)

    raw_news = newsapi.get_everything(
        q=query,
        language='en',
        sort_by='publishedAt', 
        page_size=100,
        from_param=start_dt, 
        to=end_dt
    )

    articles = raw_news.get('articles', [])

    df_news = pd.json_normalize(articles)
    if df_news.empty:
        return None
    
    df_news = df_news.rename(columns={'source.id':'source_id', 'source.name':'source_name'})

    def extract_full_content(url):
        try:
            article = Article(url)
            article.download()
            article.parse()
            return article.text
        except:
            return np.nan
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        df_news['full_content'] = list(executor.map(extract_full_content, df_news['url']))

    # df_news['title'] = df_news['title'].fillna('')
    # df_news['description'] = df_news['description'].fillna('')
    # df_news['content'] = df_news['content'].fillna('')

    # df_news.loc[df_news[df_news['full_content'].isna()].index, 'full_content'] = df_news['title'].fillna(" ") + df_news['description'].fillna(" ") + df_news['content'].fillna(" ")

    df_news = df_news.replace({pd.NA: None, np.nan: None})

    if os.path.exists('/opt/airflow'):
        base_path = Path('/opt/airflow/data')
    else:
        base_path = Path(os.getcwd()) / '../data'

    base_path = f"{base_path}/raw"
    os.makedirs(base_path, exist_ok=True)
    parquet_path = f"{base_path}/raw_news_{execution_date}.parquet" 

    df_news.to_parquet(parquet_path, index=False)    
    
    return parquet_path