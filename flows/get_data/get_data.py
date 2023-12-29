from prefect import flow, task, variables, serve, get_run_logger
from prefect.filesystems import RemoteFileSystem
from prefect.tasks import task_input_hash
from prefect.states import Completed

import traceback 

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

SymbolName = str
ExchangeName = str


@task(log_prints=True, 
      retries=3, 
      tags=['yahooh'], 
      cache_result_in_memory=False,
      persist_result=False)
def download_one_day(symbol: str, 
                     exchange: str, 
                     start_time: datetime.date,
                     interval: str) -> List[DownResult]:
    logger = get_run_logger()
    end_time = start_time + datetime.timedelta(days=1.0)
    if exchange.startswith('NYSE'):
        exchange = 'NYSE'
    nyse = mcal.get_calendar(exchange)
    if 0 == nyse.valid_days(start_time, end_time).size:
        logger.warning(f'symbol: {symbol} on exchange: {exchange} on date: {start_time}~{end_time} do not open trade')
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
    except Exception as e:
        result_list.append(DownResult(
            bucket_name='yfinance-daily',
            error_code=3,
            error_msg=f"error: {e} trace_back: {traceback.format_exc()}",
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

    try:
        # dump to db the result
        engine = db.create_engine(variables.get('q_data_db_connect_url')) 
        record_download_result(engine, result_list)
    except Exception as e:     
        logger.exception(f'download 1m data for symbol {symbol} on date: {start_time} fail.')
    finally:
        engine.dispose()


@task(log_prints=True, 
      cache_result_in_memory=False,
      persist_result=False)
def download_option(symbol: str, 
                     exchange: str, 
                     start_time: datetime.date):
    logger = get_run_logger()

    end_time = start_time + datetime.timedelta(days=1.0)
    if exchange.startswith('NYSE'):
        exchange = 'NYSE'
    nyse = mcal.get_calendar(exchange)
    if 0 == nyse.valid_days(start_time, end_time).size:
        logger.warning(f'symbol: {symbol} on exchange: {exchange} on date: {start_time}~{end_time} do not open trade')
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


def record_download_result(engine: db.Engine, record_list: List[DownResult]):
    with orm.Session(engine) as session:
        session.add_all(record_list)
        session.commit()
    

@flow(log_prints=True, retries=3, timeout_seconds=600)
def download_1m_data_part(symbol_list: List[Tuple[SymbolName, ExchangeName]], 
                          start_date: Optional[datetime.date] = None):  
    engine = db.create_engine(variables.get('q_data_db_connect_url'))
    conn = engine.connect()
    logger = get_run_logger()
    try:
        if not db.inspect(engine).has_table(DownResult.__tablename__):
            DownResult.metadata.create_all(engine)
        if start_date is None:
            start_date = datetime.date.today() - datetime.timedelta(days=1.0)
        interval = '1m'

        futures = [download_one_day.submit(symbol, exchange, start_date, interval)
                for symbol, exchange in symbol_list]
        [f.result() for f in futures]
        return None

    except:
        logger.exception(f'download 1m data for symbol list: {symbol_list} on date: {start_date} fail.')
    finally:
        conn.close()
        engine.dispose()


def chunk_list(lst, chunk_size):
    return [lst[i:i+chunk_size] for i in range(0, len(lst), chunk_size)]


def get_symbol_list(conn: db.Connection, query_sql: Optional[str] = None) -> Sequence[Tuple[SymbolName, ExchangeName]]:
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
      

@flow(retries=4)
def download_1m_date_all(start_date: Optional[datetime.date] = None,
                                  query_symbol_sql: Optional[str] = None,
                                  chunk_size: int = 20):
    if start_date is None:    
        start_date = datetime.date.today()

    engine = db.create_engine(variables.get('q_data_db_connect_url'))
    conn = engine.connect()
    logger = get_run_logger()
    try:
        symbol_list = get_symbol_list(conn, query_symbol_sql)

        for sub_list in chunk_list(symbol_list, chunk_size): 
            download_1m_data_part(sub_list,
                                           start_date)
    except:
        logger.exception(f'donwload 1m data with query_sql: {query_symbol_sql} on date: {start_date} failed')
    finally:
        conn.close()
        engine.dispose()

@task(log_prints=True, 
      retries=3, 
      tags=['inject_OHLC_into_db'],
      cache_result_in_memory=False,
      persist_result=False)
def inject_a_symbol_data(symbol: str, 
                         date_time: datetime.date
                         ):
    logger = get_run_logger()    

    engine = db.create_engine(variables.get('q_data_db_connect_url'))
    conn = engine.connect()
    table_name_of_symbol = symbol.lower().replace('-', '_')
    
    def create_time_scale_table(table_name: str, conn: db.Connection):
        create_table = db.text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                datetime TIMESTAMPTZ NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                dividends REAL,
                stock_splits REAL);
                """)

        make_it_timescale = db.text(f"""
            SELECT create_hypertable('{table_name}', by_range('datetime'));
            """)

        make_table_index = db.text(f"""
            CREATE INDEX ix_symbol_time ON {table_name} (datetime DESC);
            """)
        conn.execute(create_table)
        conn.execute(make_it_timescale)
        # conn.execute(make_table_index)    
    

    def inset_row(df: pd.DataFrame, table_name: str, conn: db.Connection):
        for _index, item in df.iterrows(): 
            insert_stmt = db.text(f"""
                INSERT INTO {table_name} (
                    "datetime",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "dividends",
                    "stock_splits") VALUES (
                    '{item.Datetime}',
                    '{item.Open}',
                    '{item.High}',
                    '{item.Low}',
                    '{item.Close}',
                    '{item.Volume}',
                    '{item.Dividends}',
                    '{item["Stock Splits"]}'
                ) ON CONFLICT DO NOTHING
            """)
            conn.execute(insert_stmt)

    try:
        if not db.inspect(engine).has_table(table_name_of_symbol):
            create_time_scale_table(table_name_of_symbol, conn)

        r = conn.execute(db.text(f"""
            SELECT DISTINCT file_path FROM yf_bad_record
            WHERE symbol='{symbol}' AND file_type like '%m-stock' AND bucket_name='yfinance-daily' AND error_code=0 AND data_date='{date_time}';
             """))
        result = list(r.fetchall())
        if len(result) == 0: 
            logger.warning(f'warning! symbol: {symbol} has no batch date on: {date_time}, no nothing!')
            return
        elif len(result) > 1:
            logger.critical(f'error! symbol: {symbol} has many data selected batch date on: {date_time}, which is ambiguous. Select result is: {result}')
            return
        file_path = result[0][0] 
        logger.info(f'symbol: {symbol} on date: {date_time} will use path: {file_path} on bucket yfinance-daily to inject data')
        fs = RemoteFileSystem.load("yfinance-daily")
        x = fs.read_path(file_path)
        df = pd.read_csv(io.BytesIO(x), header=0)
        if len(df) == 0:
            logger.warning(f'symbol: {symbol} on date: {date_time} in file_path: {file_path} has no dateitems, do nothing')
            return

        df.Datetime = pd.to_datetime(df.Datetime)
        inset_row(df, table_name_of_symbol, conn) 
    except Exception:
        logger.exception(f'error! happends when try to inject symbol: {symbol} on date: {date_time}')
    finally:
        conn.commit()
        conn.close()
        engine.dispose()


@flow(log_prints=True)
def inject_data_part(symbol_list: List[Tuple[SymbolName, ExchangeName]],
                     date_time: datetime.date 
                     ):
    return [inject_a_symbol_data.submit(sym, date_time) for sym, _ in symbol_list] 
    

@flow(log_prints=True)
def inject_data_all(query_symbol_sql: Optional[str] = None, 
                    date_time: Optional[datetime.date] = None,
                    chunk_size: int = 100):
    if date_time is None:     
        date_time = datetime.date.today()

    engine = db.create_engine(variables.get('q_data_db_connect_url'))
    conn = engine.connect()
    logger = get_run_logger()
    try: 
        all_symbol_list =  get_symbol_list(conn, query_symbol_sql)
        conn.close() 
        engine.dispose()
        for sub_list in chunk_list(all_symbol_list, chunk_size):
            inject_data_part(sub_list, date_time)
    except:
        logger.exception(f'inject data using symbol query sql: {query_symbol_sql} on date: {date_time} failed')
    finally:
        engine.dispose()
        conn.close()




if __name__ == "__main__":
    down_load_data = download_1m_date_all.to_deployment(name='down_load_1m_data')
    inject_data = inject_data_all.to_deployment(name='inject_data_all')
    serve(down_load_data, inject_data)