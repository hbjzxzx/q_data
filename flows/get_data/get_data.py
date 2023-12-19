from prefect import flow, task, variables
from prefect.filesystems import RemoteFileSystem
from prefect.tasks import task_input_hash

import pandas as pd
import requests
from typing import Tuple, Sequence, Optional, List
import datetime
import io
import os

import sqlalchemy as db
import pandas_market_calendars as mcal
import yfinance as yf
from requests.exceptions import HTTPError

import sqlalchemy.orm as orm

class Base(orm.DeclarativeBase):
    ...


class DownResult(Base):
    __tablename__ = 'yf_bad_record'

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True, autoincrement=True)
    bucket_name: orm.Mapped[str]
    error_code: orm.Mapped[int]
    error_msg: orm.Mapped[str]
    symbol: orm.Mapped[str]    
    exchange: orm.Mapped[str]
    file_path: orm.Mapped[str]
    file_type: orm.Mapped[Optional[str]]
    data_date: orm.Mapped[Optional[datetime.date]]


@task(log_prints=True, retries=3, tags=['yahooh'])
def download_one_day(symbol: str, 
                     exchange: str, 
                     start_time: datetime.date,
                     interval: str) -> List[DownResult]:

    end_time = start_time + datetime.timedelta(days=1.0)
    if exchange.startswith('NYSE'):
        exchange = 'NYSE'
    nyse = mcal.get_calendar(exchange)
    if 0 == nyse.valid_days(start_time, end_time).size:
        print(f'symbol: {symbol} on exchange: {exchange} on date: {start_time}~{end_time} do not open trade')
        return []

    tickers = yf.Ticker(symbol)

    result_list = []

    try:
        buf = io.BytesIO()
        tickers.history(start=start_time, end=end_time, interval=interval, prepost=True).to_csv(buf)
        save_path = os.path.join(interval, symbol, f'stock_basic_price-{start_time.strftime("%y-%m-%d")}.csv')
        fs = RemoteFileSystem.load("yfinance-daily")
        fs.write_path(save_path, buf.getvalue())


        result_list.append(DownResult(
            bucket_name='yfinance-daily',
            error_code=0,
            error_msg='',
            symbol=symbol,
            exchange=exchange,
            file_path=save_path,
            file_type='1m-stock',
            data_date=start_time
            ))
        
    except HTTPError as e: 
        result_list.append(DownResult(
            bucket_name='yfinance-daily',
            error_code=e.response.status_code,
            error_msg=e.response.content.decode(),
            symbol=symbol,
            exchange=exchange,
            file_path=''))
    else:
        try:
            info = tickers.info
            if 'companyOfficers' in info:
                del info['companyOfficers']
            info_df = pd.DataFrame(info, index=[0])
            info_buf = io.BytesIO()
            info_df.to_csv(info_buf)

            info_save_path = os.path.join(interval, symbol, f'stock_basic_info-{start_time.strftime("%y-%m-%d")}.csv')
            fs.write_path(info_save_path, info_buf.getvalue())


            result_list.append(DownResult(
            bucket_name='yfinance-daily',
            error_code=0,
            error_msg='',
            symbol=symbol,
            exchange=exchange,
            file_path=save_path,
            file_type='info',
            data_date=start_time))
        except Exception as e:
            result_list.append(DownResult(
                bucket_name='yfinance-daily',
                error_code=2,
                error_msg=str(e),
                symbol=symbol,
                exchange=exchange,
                file_path=''))
    
    return result_list
            

@task()
def download_option(symbol: str, 
                     exchange: str, 
                     start_time: datetime.date):

    end_time = start_time + datetime.timedelta(days=1.0)
    if exchange.startswith('NYSE'):
        exchange = 'NYSE'
    nyse = mcal.get_calendar(exchange)
    if 0 == nyse.valid_days(start_time, end_time).size:
        print(f'symbol: {symbol} on exchange: {exchange} on date: {start_time}~{end_time} do not open trade')
        return
    
    tickers = yf.Ticker(symbol)
    options_date_list = tickers.options
    for option_date in options_date_list:
        opt = tickers.option_chain(date=option_date)
        call, put = opt.calls, opt.puts
        call_buf = io.BytesIO() 
        put_buf = io.BytesIO()
        
        call.to_csv(call_buf)
        put.to_csv(put_buf)

        call_save_path = os.path.join('') 


@task(tags=['record_write_db'])
def record_download_result(engine: db.Engine, record_list: List[DownResult]):
    with orm.Session(engine) as session:
        session.add_all(record_list)
        session.commit()


@task(cache_key_fn=task_input_hash)
def make_sure_result_table_exist(engine: db.Engine, conn: db.Connection):
    if engine.dialect.has_table(conn, DownResult.__tablename__):
        return
    
    DownResult.metadata.create_all(engine)
                 

@flow(log_prints=True)
def download_last_day_1m_data_part(symbol_list: List[Tuple[str, str]], 
                                    start_date: Optional[datetime.date] = None):  
    engine = db.create_engine(variables.get('q_data_db_connect_url'))
    conn = engine.connect()

    make_sure_result_table_exist(engine, conn)
    if start_date is None:
        start_date = datetime.date.today() - datetime.timedelta(days=1.0)
    interval = '1m'

    future_task_list = [
         download_one_day.submit(symbol, exchange, start_date, interval)
         for symbol, exchange in symbol_list
    ]
    
    record = [record_download_result.submit(engine, f.result()) for f in future_task_list]
    [r.result() for r in record]
    # for symbol, exchange in get_symbol_list(conn):
    #     result_list = download_one_day(symbol, exchange, start_date, interval)
    #     record_download_result(engine, result_list)     


def chunk_list(lst, chunk_size):
    return [lst[i:i+chunk_size] for i in range(0, len(lst), chunk_size)]


def get_symbol_list(conn: db.Connection, query_sql: Optional[str] = None) -> Sequence[Tuple[str, str]]:
    if query_sql is None:
        query = db.text("""
                        SELECT symbol, exchange 
                        FROM stock_meta
                        WHERE delistingdate is NULL
                        LIMIT 10;
                        """)
    else:
        query = db.text(query_sql)
        
    r = conn.execute(query)
    return r.fetchall()
      

@flow()
def download_last_day_1m_date_all(start_date: Optional[datetime.date] = None,
                                  query_sql: Optional[int] = None,
                                  chunk_size: int = 2):
    engine = db.create_engine(variables.get('q_data_db_connect_url'))
    conn = engine.connect()
    symbol_list = get_symbol_list(conn, query_sql)
    for sub_list in chunk_list(symbol_list, chunk_size): 
        download_last_day_1m_data_part(sub_list,
                                       start_date)
    

if __name__ == "__main__":
    download_last_day_1m_date_all.serve(name='extra_stock_list')