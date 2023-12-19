from prefect import flow, task, variables, serve 
from prefect.filesystems import RemoteFileSystem

import pandas as pd
import requests
import datetime
import io
from typing import Optional

import sqlalchemy as db


@task(log_prints=True, retries=3)
def extra_stock_lisk_task():
    key = variables.get('alpha_key')
    CSV_URL = f'https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={key}'
    fs = RemoteFileSystem.load("stock-meta")

    with requests.Session() as s:
        download = s.get(CSV_URL)
        decoded_content = download.content.decode('utf-8')
        df = pd.read_csv(io.StringIO(decoded_content))
        nowd = datetime.datetime.now().strftime('%Y-%m-%d')
        buf = io.BytesIO() 
        name = f'{nowd}-America-sotck-list.csv'
        df.to_csv(buf)
        fs.write_path(name, buf.getvalue())


@task(log_prints=True, retries=3)
def inject_data_into_db(f_name: str, conn: db.Connection):
    fs = RemoteFileSystem.load("stock-meta")
    x = fs.read_path(f_name)
    df = pd.read_csv(io.BytesIO(x), index_col=0, header=0)
    df['country'] = 'US'
    df['last_update'] = datetime.datetime.now()
    df.ipoDate = pd.to_datetime(df.ipoDate)
    df.delistingDate = pd.to_datetime(df.delistingDate)

    conn.execute(db.text("""
        CREATE TABLE IF NOT EXISTS stock_meta (
            symbol TEXT PRIMARY KEY,
            name TEXT,
            exchange TEXT,
            assetType TEXT,
            ipoDate TIMESTAMP,
            delistingDate TIMESTAMP,
            status TEXT,
            country TEXT,
            last_update TIMESTAMP)
    """))

    for index, item in df.fillna('NULL').iterrows(): 
        delistingDate = f"'{item.delistingDate}'" if item.delistingDate != 'NULL' else 'NULL'
        insert_stmt = db.text(f"""
            INSERT INTO stock_meta (
                symbol,
                name, 
                exchange, 
                assetType,
                ipoDate,
                delistingDate,
                status,
                country,
                last_update
            ) VALUES (
                '{item.symbol}',
                '{item['name'].replace("'", "''")}',
                '{item.exchange}',
                '{item.assetType}',
                '{item.ipoDate}',
                {delistingDate},
                '{item.status}',
                '{item.country}',
                '{item.last_update}'
            )
            ON CONFLICT (symbol) DO UPDATE
            SET
                delistingdate = EXCLUDED.delistingDate,
                status = EXCLUDED.status,
                last_update = EXCLUDED.last_update
            WHERE
                stock_meta.delistingdate <> EXCLUDED.delistingDate OR
                stock_meta.delistingdate IS NULL;
        """)
        conn.execute(insert_stmt)

    conn.commit()

@flow
def extra_american_sotck_list():
    extra_stock_lisk_task()


@flow
def inject_american_data_into_db(datetime_str: Optional[str] = None):
    if datetime_str is None:
        datetime_str = datetime.datetime.now().strftime('%Y-%m-%d')
    name = f'{datetime_str}-America-sotck-list.csv'
    engine = db.create_engine(variables.get('q_data_db_connect_url'))
    with engine.connect() as conn:
        inject_data_into_db(name, conn)
        conn.commit()  


if __name__ == "__main__":
    extra_stock_list_deploy = extra_american_sotck_list.to_deployment(name='extra_stock_list')
    inject_american_date_into_db_deploy = inject_american_data_into_db.to_deployment(name='inject_american_stock_data')
    
    serve(extra_stock_list_deploy, inject_american_date_into_db_deploy)