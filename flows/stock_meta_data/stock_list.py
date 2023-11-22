from prefect import flow, task, variables
from prefect.filesystems import RemoteFileSystem

import pandas as pd
import requests
import datetime
import io


@task
def extra_stock_lisk_task(retries=3, log_prints=True):
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


@flow
def extra_american_sotck_list():
    extra_stock_lisk_task()


if __name__ == "__main__":
    extra_american_sotck_list.serve(name='extra_stock_list')