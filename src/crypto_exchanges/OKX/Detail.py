import concurrent.futures
import pandas as pd
import re
import logging
import time

from src.config import RESULT, DATA_DIR, TRANSACT_PERIOD
from src.crypto_exchanges.OKX.config import COMPANY

import requests
from requests.exceptions import ConnectionError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


company = file_name = COMPANY
transact_period = TRANSACT_PERIOD[0]
url_api_get_pnl = "https://www.okx.com/priapi/v5/ecotrade/public/yield-pnl?latestNum=7&uniqueName="
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en,en-CA;q=0.9,vi;q=0.8,en-US;q=0.7,en-GB;q=0.6",
    "sec-ch-ua": "\"Not)A;Brand\";v=\"99\", \"Google Chrome\";v=\"127\", \"Chromium\";v=\"127\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
}

def request_url(url):
    pattern = r'/account/([A-Za-z0-9]+)\?'
    roi = pnl = user_id = None
    if url:
        match = re.search(pattern, url)
        # Extract the matched string
        if match:
            user_id = match.group(1)
    user_api = url_api_get_pnl + user_id
    try:
        response = requests.get(user_api, headers=headers)
        time.sleep(1.5)
        if response.status_code == 200:
            user_data = response.json()['data']
            latest_date_data = user_data[-1]
            roi = latest_date_data['ratio']
            pnl = latest_date_data['pnl']
    except (requests.ConnectionError, IndexError) as e:
        logger.info(f'{e} in {user_api}')
        time.sleep(5)
        user_id = None
    return url, user_id, roi, pnl


def Detail(df) -> pd.DataFrame:
    # https://www.okx.com/priapi/v5/ecotrade/public/week-pnl?uniqueName=224A4DC6488F67B8 daily
    # https://www.okx.com/priapi/v5/ecotrade/public/yield-pnl?latestNum=7&uniqueName=664BD292A54718C3 accumulative api

    def update_dataframe(*args) -> None:
        url = args[0]
        user_id = args[1]
        roi = args[2]
        pnl = args[3]
        if url and user_id:
            df.loc[df["url"] == url, "user_id"] = user_id
            df.loc[df["url"] == url, "ROI"] = roi
            df.loc[df["url"] == url, "PNL"] = pnl
            logger.info(f'writing user_id {user_id}')
        return None
    
    df['user_id'] = ""
    urls = df['url'].to_numpy()

    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
        futures = [executor.submit(request_url, url) for url in urls]
        for future in concurrent.futures.as_completed(futures):
            url, user_id, roi, pnl = future.result()
            if user_id:
                update_dataframe(url, user_id, roi, pnl)
    df['crypto_exchange'] = company
    df['transact_period'] = transact_period
    return df