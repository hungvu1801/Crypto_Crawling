import concurrent.futures
import copy
from datetime import datetime
import json
import pandas as pd
import logging
import re
import random

from sqlalchemy.sql import text
from src.config import RESULT, DATA_DIR, TRANSACT_PERIOD, USER_AGENTS
from src.crypto_exchanges.OKX.config import COMPANY, headers, url_api_get_pnl, headers_position, url_api_get_history
from src.data_push.load_data import create_engine_db, get_db_data
from src.data_push.scripts.query import select_transaction_filter_by_order_id_and_status_okx
from src.data_pipeline.TransactionDataPipeline import TransactionDataPipeline
from src.utility.helper import request_get_wrapper, get_timestamp_now

import time

import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
today = datetime.now().strftime("%y%m%d")
# handler = logging.FileHandler(f'log/{today}/okxdetail.log')
# logger.addHandler(handler)
# logger.setLevel(logging.INFO)

company = file_name = COMPANY
transact_period = TRANSACT_PERIOD[0]


def request_url(url):
    pattern = r'/account/([A-Za-z0-9]+)\?'
    roi = pnl = user_id = user_api = None
    try:
        if url:
            match = re.search(pattern, url)
            # Extract the matched string
            if match:
                user_id = match.group(1)
        user_api = url_api_get_pnl + user_id
        user_agent = random.choice(USER_AGENTS)
        headers["user-agent"] = user_agent
        response = requests.get(user_api, headers=headers)
        time.sleep(1.5)
        if response.status_code == 200:
            user_data = response.json()['data']
            latest_date_data = user_data[-1]
            roi = latest_date_data['ratio']
            pnl = latest_date_data['pnl']
    except (requests.ConnectionError, IndexError, TypeError) as e:
        logger.info(f'{e} in {user_api}')
        time.sleep(5)
        # user_id = None
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

def Detail_API_get_coin() -> pd.DataFrame:
    return

def Detail_API_get_position(trader_id, page_no=1) -> int:

    engine = create_engine_db()
    logger.info(f"Start {trader_id}")
    transaction_data_pipeline = TransactionDataPipeline(
        table_name="position_history_OKX", 
        exchange="OKX", 
        engine=engine,
        storage_queue_limit=100)
    query = text(select_transaction_filter_by_order_id_and_status_okx)
    params = {
        "teacher_id": trader_id
        }
    with engine.begin() as conn:
        df_partially_closed = get_db_data(conn, query, params)
    oldest_order_no = ""
    if not df_partially_closed.empty:
        df_partially_closed['open_time'] = pd.to_numeric(df_partially_closed['open_time'])
        oldest_row = df_partially_closed.loc[df_partially_closed['open_time'].idxmin()]
        oldest_order_no = oldest_row['order_no']
    logger.info(f"oldest_order_no {trader_id} : {oldest_order_no}")
    is_data_duplicated = False
    epoch_time_now = get_timestamp_now(milisec=True)
    next_page = ""
    try:
        while True:
            logger.info(f"{trader_id}: page - {page_no}")
            ##### break
            if is_data_duplicated:
                break
            user_agent = random.choice(USER_AGENTS)

            full_ulr_api_get_history = url_api_get_history + next_page + f"size=200&instType=SWAP&uniqueName={trader_id}&t={epoch_time_now}"

            referer = f'https://www.okx.com/copy-trading/account/{trader_id}?tab=swap'
            headers = copy.deepcopy(headers_position)
            headers["User-Agent"] = user_agent
            headers["Referer"] = referer

            response = request_get_wrapper(full_ulr_api_get_history, headers)
            if not response:
                break
            response_content = response.content.decode('utf-8', errors='ignore')
            data = json.loads(response_content)
            transaction_history = data["data"]

            if not transaction_history:
                break
            last_item_id = transaction_history[-1]['id']
            next_page = f"after={last_item_id}&"

            for transaction in transaction_history:
                transaction_data = {
                    'order_no': transaction["id"],
                    'ccy': transaction["ccy"],
                    'close_avg_px': transaction["closeAvgPx"],
                    'close_pnl': transaction["closePnl"],
                    'contract_val': transaction["contractVal"],
                    'deal_volume': transaction["dealVolume"],
                    'fee': transaction["fee"],
                    'funding_fee': transaction["fundingFee"],
                    'symbol': transaction["instId"],
                    'lever': transaction["lever"],
                    'liquidation_fee': transaction["liquidationFee"],
                    'margin': transaction["margin"],
                    'margin_mode': transaction["mgnMode"],
                    'multiplier': transaction["multiplier"],
                    'open_avg_px': transaction["openAvgPx"],
                    'open_time': transaction["openTime"],
                    'close_time': transaction["uTime"],
                    'pnl': transaction["pnl"],
                    'pnl_ratio': transaction["pnlRatio"],
                    'position': transaction["posSide"],
                    'pos_type': transaction["posType"],
                    'side': transaction["side"],
                    'type': transaction["type"],
                    'teacher_id': trader_id,
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                }
                is_data_duplicated = transaction_data_pipeline.add_transaction(transaction_data)
                logger.info(f"is_data_duplicated : {is_data_duplicated}")
                if oldest_order_no == transaction["id"]:
                    break
            ##### break
            if not oldest_order_no:
                if is_data_duplicated:
                    break
            page_no += 1
        transaction_data_pipeline.close_pipeline()
        engine.dispose()
        return 0

    except Exception as e:
        logger.info(f"error: {e}")
        logger.info(f"error at url {full_ulr_api_get_history}")
        transaction_data_pipeline.close_pipeline()
        engine.dispose()
        return 1