from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import requests
import time
import yfinance as yf
from db_connect import get_engine
from sentence_transformers import SentenceTransformer

DB_URL = 'postgresql://admin:password123@db:5432/silver_warehouse'
engine = create_engine(DB_URL)

def init_database():
    with engine.connect() as conn:
        with conn.begin():

            # SETTING PGVECTOR
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))

            # DIMENSION
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_assets (
                    asset_id VARCHAR(10) PRIMARY KEY, 
                    asset_name VARCHAR(100),
                    asset_type VARCHAR(20),         
                    country VARCHAR(20),
                    description TEXT,
                    embedded_description vector(384)
                );
            """))

            # FACT PRICE -> stock price 
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_prices (
                    id SERIAL PRIMARY KEY,
                    asset_id VARCHAR(10) REFERENCES dim_assets(asset_id),
                    datetime TIMESTAMPTZ,
                    price_close DECIMAL(18, 4),
                    volume BIGINT,
                    UNIQUE(asset_id, datetime) -- ป้องกันข้อมูลราคาซ้ำในวันเดียวกัน
                );
            """))

            # RAW NEWS
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS stg_news (
                    id SERIAL PRIMARY KEY,
                    author TEXT,
                    source_id TEXT,
                    source_name VARCHAR(100),
                    title TEXT,
                    url TEXT UNIQUE,
                    raw_content TEXT,
                    is_vectorized BOOLEAN DEFAULT FALSE,
                    published_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """))

            # FACT NEWS VECTOR
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_news_vector (
                    stg_id INT PRIMARY KEY,
                    datetime TIMESTAMPTZ,
                    title TEXT,
                    embedding vector(384), -- เก็บ Vector สำหรับค้นหาด้วยความหมาย
                    url TEXT          -- ใช้ URL เป็นตัวเช็คข่าวซ้ำ (Deduplication)
                );
            """))

            #  INSERT STOCK TRIGGLE
            conn.execute(text("""
                INSERT INTO dim_assets (asset_id, asset_name, asset_type, country, description)
                VALUES 
                    ('HL', 'Hecla Mining', 'Stock', 'USA', $$Hecla Mining Company, together with its subsidiaries, provides precious and base metals in the United States, Canada, Japan, Korea, and China. The company mines for silver, gold, lead, and zinc concentrates, as well as carbon material containing silver and gold for custom smelters, metal traders, and third-party processors; and doré containing silver and gold. Its flagship project is the Greens Creek mine located on Admiralty Island in southeast Alaska. The company was incorporated in 1891 and is headquartered in Coeur d'Alene, Idaho.$$),
                    ('CDE', 'Coeur Mining', 'Stock', 'USA', $$Coeur Mining, Inc. operates as a gold and silver producer in the United States, Canada, and Mexico. It operates through Palmarejo, Rochester, Kensington, Wharf, Silvertip, and Las Chispas segments. The company explores for gold, silver, zinc, lead, and other related metals. It markets and sells its concentrates to third-party customers, including refiners and smelters, under off-take agreements. The company was formerly known as Coeur d'Alene Mines Corporation and changed its name to Coeur Mining, Inc. in May 2013. Coeur Mining, Inc. was incorporated in 1928 and is headquartered in Chicago, Illinois.$$),
                    ('PAAS', 'Pan American Silver', 'Stock', 'USA', $$Pan American Silver Corp. engages in the exploration, mine development, extraction, processing, refining, and reclamation of mines in Canada, Mexico, Peru, Bolivia, Argentina, Chile, and Brazil. It explores for silver, gold, zinc, lead, and copper deposits. The company was formerly known as Pan American Minerals Corp. and changed its name to Pan American Silver Corp. in April 1995. Pan American Silver Corp. was incorporated in 1979 and is headquartered in Vancouver, Canada.$$),
                    ('AG', 'First Majestic Silver', 'Stock', 'USA', $$First Majestic Silver Corp. engages in the acquisition, exploration, development, and production of mineral properties in North America. The company explores for silver and gold deposits. Its projects include the San Dimas silver/gold mine covering an area of approximately 71,867 hectares located in Durango State, Mexico; the Santa Elena silver/gold mine that covers an area of approximately 102,244 hectares located in Sonora State, Mexico; and the La Encantada silver mine covering an area of approximately 4,076 hectares located in Coahuila State, Mexico. The company was formerly known as First Majestic Resource Corp. and changed its name to First Majestic Silver Corp. in November 2006. First Majestic Silver Corp. was incorporated in 1979 and is headquartered in Vancouver, Canada.$$),
                    ('EXK', 'Endeavour Silver', 'Stock', 'USA', $$Endeavour Silver Corp., a silver mining company, engages in the acquisition, exploration, development, extraction, processing, refining, and reclamation of mineral properties in Mexico, Chile, Peru, and the United States. It explores for gold and silver deposits, and precious metals, as well as polymetals. The company was formerly known as Endeavour Gold Corp. and changed its name to Endeavour Silver Corp. in September 2004. Endeavour Silver Corp. was incorporated in 1981 and is headquartered in Vancouver, Canada.$$),
                    ('FRES.L', 'Fresnillo plc', 'Stock', 'MEX', $$Fresnillo plc mines, develops, and produces non-ferrous minerals in Mexico. It operates through seven segments: Fresnillo, Saucito, Ciénega, Herradura, Noche Buena, San Julián, and Juanicipio. The company primarily explores for silver, gold, lead, and zinc concentrates. Its projects include the Fresnillo silver mine located in the state of Zacatecas; Saucito silver mine situated in the state of Zacatecas; Ciénega silver-gold mine located in the state of Durango; Herradura gold mine situated in the state of Sonora; Noche Buena gold mine located in the state of Sonora; San Julián silver-gold mine situated on the border of Chihuahua/Durango states; and Juanicipio silver mine located in the state of Zacatecas. The company also leases mining equipment; produces gold/silver doré bars; and provides administrative services. Fresnillo plc was founded in 1887 and is headquartered in Mexico City, Mexico. Fresnillo plc operates as a subsidiary of Industrias Peñoles, S.A.B. de C.V.$$),
                    ('KGH.WA', 'KGHM Polska Miedź', 'Stock', 'POL', $$KGHM Polska Miedz S.A. engages in the exploration and mining of copper, nickel, precious metals, and non-ferrous metals in Poland and internationally. The company offers copper cathodes, concentrate, wire rod and Cu-Ofe, and Cu-Ag wires; silver and gold; molybdenum; nickel; ETP/OFE nuggets; molybdenum oxides; oxygen-free copper rods; ammonium perrhenate, metallic rhenium, and rhenium powder; and lead, sulphuric acid, copper and nickel sulphate, and selenium products, as well as platinum, palladium, and rock salt products. It holds a portfolio of mines located in Poland, Canada, Chile, and the United States. The company was founded in 1961 and is headquartered in Lubin, Poland.$$)
                    
                ON CONFLICT (asset_id) DO UPDATE SET description = EXCLUDED.description;
            """))

    print("DB Created Successfully")

if __name__ == "__main__":
    init_database()

    # session = requests.Session()
    # session.headers.update({
    # 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
# })
    query = "SELECT * FROM dim_assets" 

    with engine.connect() as conn:
        raw_conn = conn.connection 
        df = pd.read_sql_query(query, raw_conn)

    tickers = df['asset_id'].to_list()

    # description_lst = []
    # for ticker in tickers:
    #     ticker = yf.Ticker(ticker)

    #     info = ticker.info
    #     description = info.get('longBusinessSummary')

    #     description_lst.append(description)

    #     time.sleep(10)
    
    # df['description'] = description_lst
    # print(df)

    # print(df['description'])

    model = SentenceTransformer('all-MiniLM-L6-v2')
    embeddings = model.encode(df['description'].tolist())
    
    
    df['embedded_description'] = embeddings.tolist()
    df['embedded_description'] = df['embedded_description'].apply(
        lambda x: x.tolist() if isinstance(x, np.ndarray) else x
        )

    query = text("""INSERT INTO dim_assets (asset_id, asset_name, asset_type, country, description, embedded_description) \
        VALUES (:asset_id, :asset_name, :asset_type, :country, :description, :embedded_description) \
        ON CONFLICT (asset_id) \
        DO UPDATE SET  \
            description = EXCLUDED.description, \
            embedded_description = EXCLUDED.embedded_description;\
        """)
    with engine.connect() as conn:
        conn.execute(query, df.to_dict('records'))

