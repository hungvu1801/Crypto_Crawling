import pandas as pd
import logging
import json
from datetime import datetime
import time
from src.config import USER_AGENTS
from src.crawling.findElement import find_element_with_retry
from src.data_pipeline.TransactionDataPipeline import TransactionDataPipeline
from src.data_push.load_data import create_engine_db, get_db_data
from src.data_push.scripts.query import select_transaction_filter_by_order_id_and_status_Bybit
from src.utility.helper import get_timestamp_now
from sqlalchemy.sql import text
from urllib.parse import quote
import re
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def Detail_API(df) -> pd.DataFrame: 
    return df

def Detail_selem(driver, df) -> pd.DataFrame:
    return df

def Detail_API_get_position(trader_id, driver_list, next_page="first_page"):

    logger.info(f"Start {trader_id}")
    engine = create_engine_db()
    quote_trader_id = quote(trader_id)
    driver = driver_list.pop()
    transaction_data_pipeline = TransactionDataPipeline(
        table_name="position_history_Bybit", 
        exchange="Bybit", 
        engine=engine,
        storage_queue_limit=50)
    is_data_duplicated = False
    epoch_time_now = get_timestamp_now(milisec=True)
    query = text(select_transaction_filter_by_order_id_and_status_Bybit)
        
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
    try:
        next_page_cursor = ""
        while True:
            if is_data_duplicated:
                break
            url_api_get_history = f"https://api2.bybit.com/fapi/beehive/public/v1/common/leader-history?timeStamp={epoch_time_now}&leaderMark={quote_trader_id}" \
                                  + next_page_cursor + f"&pageAction={next_page}&pageSize=50"
         
            driver.get(url_api_get_history)
            time.sleep(1)
            response_elem = find_element_with_retry(driver, 'pre')
            
            response_content = response_elem.text
            data = json.loads(response_content)
            has_next = data['result']['hasNext']
            transaction_history = data['result']["data"]
            if has_next:
                next_page = "next"
                cursor_id = data['result']['cursor']
                next_page_cursor = f"&cursor={cursor_id}"

            if not transaction_history:
                break
            for transaction in transaction_history:
                transaction_data = {
                    'order_no': transaction["orderId"],
                    'symbol': transaction["symbol"],
                    'side': transaction["side"],
                    'is_isolated': transaction["isIsolated"],
                    'leverage': transaction["leverageE2"],
                    'entry_price': transaction["entryPrice"],
                    'closed_price': transaction["closedPrice"],
                    'size': transaction["size"],
                    'pnl': transaction["closedPnlE8"],
                    'open_time': transaction["startedTimeE3"],
                    'close_time': transaction["closedTimeE3"],
                    'cum_funding_fee': transaction["cumFundingFeeE8"],
                    'open_cum_exec_fee': transaction["openCumExecFeeE8"],
                    'close_cum_exec_fee': transaction["closeCumExecFeeE8"],
                    'closed_type': transaction["closedType"],
                    'follower_num': transaction["followerNum"],
                    'order_cost': transaction["orderCostE8"],
                    'order_net_profit': transaction["orderNetProfitE8"],
                    'order_net_profit_rate': transaction["orderNetProfitRateE4"],
                    'position_entry_price': transaction["positionEntryPrice"],
                    'position_cycle_version': transaction["positionCycleVersion"],
                    'cross_seq': transaction["crossSeq"],
                    'position_idx': transaction["positionIdx"],
                    'pos_closed_time': transaction["posClosedTimeE3"],
                    'has_multi_close_order': transaction["hasMultiCloseOrder"],
                    'status': transaction["fullClosed"],
                    'teacher_id': trader_id,
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                }
                if transaction_data['status']:
                    transaction_data['status'] = 'Closed'
                else:
                    transaction_data['status'] = 'Opened'
                is_data_duplicated = transaction_data_pipeline.add_transaction(transaction_data)
                logger.info(f"is_data_duplicated : {is_data_duplicated}")
                # break
                if oldest_order_no == transaction["orderId"]:
                    break
            ##### break
            if not oldest_order_no:
                if is_data_duplicated:
                    break
            if not has_next:
                break

        transaction_data_pipeline.close_pipeline()
        engine.dispose()
        driver_list.append(driver)
        return 0

    except Exception as e:
        logger.info(f"error: {e}")
        logger.info(f"error at url {url_api_get_history}")
        transaction_data_pipeline.close_pipeline()
        engine.dispose()
        driver_list.append(driver)
        return 1
    
def Detail_API_get_position_save2json(trader_id, driver_list, done_list, next_page="first_page"):
    logger.info(f"Start {trader_id}")
    quote_trader_id = quote(trader_id)
    driver = driver_list.pop()
    epoch_time_now = get_timestamp_now(milisec=True)
    try:
        next_page_cursor = ""
        saved_json = []
        while True:
            url_api_get_history = f"https://api2.bybit.com/fapi/beehive/public/v1/common/leader-history?timeStamp={epoch_time_now}&leaderMark={quote_trader_id}" \
                                  + next_page_cursor + f"&pageAction={next_page}&pageSize=50"
        
            driver.get(url_api_get_history)
            time.sleep(1.5)
            response_elem = find_element_with_retry(driver, 'pre')
            
            response_content = response_elem.text
            data = json.loads(response_content)
            has_next = data['result']['hasNext']
            transaction_history = data['result']["data"]
            if has_next or has_next == 'true':
                next_page = "next"
                cursor_id = data['result']['cursor']
                next_page_cursor = f"&cursor={cursor_id}"

            if not transaction_history:
                break
            for transaction in transaction_history:
                transaction_data = {
                    'order_no': transaction["orderId"],
                    'symbol': transaction["symbol"],
                    'side': transaction["side"],
                    'is_isolated': transaction["isIsolated"],
                    'leverage': transaction["leverageE2"],
                    'entry_price': transaction["entryPrice"],
                    'closed_price': transaction["closedPrice"],
                    'size': transaction["size"],
                    'pnl': transaction["closedPnlE8"],
                    'open_time': transaction["startedTimeE3"],
                    'close_time': transaction["closedTimeE3"],
                    'cum_funding_fee': transaction["cumFundingFeeE8"],
                    'open_cum_exec_fee': transaction["openCumExecFeeE8"],
                    'close_cum_exec_fee': transaction["closeCumExecFeeE8"],
                    'closed_type': transaction["closedType"],
                    'follower_num': transaction["followerNum"],
                    'order_cost': transaction["orderCostE8"],
                    'order_net_profit': transaction["orderNetProfitE8"],
                    'order_net_profit_rate': transaction["orderNetProfitRateE4"],
                    'position_entry_price': transaction["positionEntryPrice"],
                    'position_cycle_version': transaction["positionCycleVersion"],
                    'cross_seq': transaction["crossSeq"],
                    'position_idx': transaction["positionIdx"],
                    'pos_closed_time': transaction["posClosedTimeE3"],
                    'has_multi_close_order': transaction["hasMultiCloseOrder"],
                    'status': transaction["fullClosed"],
                    'teacher_id': trader_id,
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                }
                if transaction_data['status']:
                    transaction_data['status'] = 'Closed'
                else:
                    transaction_data['status'] = 'Opened'
                saved_json.append(transaction_data)
            if not has_next:
                break
            
        driver_list.append(driver)
        if saved_json:
            cleaned_trader_id = re.sub('/', '', trader_id)
            with open(f'crawling_data/Bybit/Detail/{cleaned_trader_id}.json', 'w') as f:
                json.dump(saved_json, f)
        done_list.append(trader_id)
    except Exception as e:
        logger.info(f"error: {e}")
        logger.info(f"error at url {url_api_get_history}")
        driver_list.append(driver)

def Detail_API_get_position_push_toDB(file):
    engine = create_engine_db()

    transaction_data_pipeline = TransactionDataPipeline(
        table_name="position_history_Bybit", 
        exchange="Bybit", 
        engine=engine,
        storage_queue_limit=1)
    try:
        with open(file, 'r') as json_file:
            transaction_history = json.load(json_file)
        if transaction_history:
            for transaction in transaction_history:
                if transaction['status']:
                    transaction['status'] = 'Closed'
                else:
                    transaction['status'] = 'Opened'

                transaction_data_pipeline.add_transaction(transaction)

        transaction_data_pipeline.close_pipeline()
        engine.dispose()
        return 0
    except Exception as e:
        logger.info(f"error: {e}")
        logger.info(f"error file {file}")
        transaction_data_pipeline.close_pipeline()
        engine.dispose()
        return 1