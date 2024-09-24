# import brotli
from datetime import datetime
import pandas as pd
import json
import logging
# import requests
# import re
# import time
# import zlib
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from sqlalchemy.sql import text
from src.config import DATA_DIR, TRANSACT_PERIOD, USER_AGENTS
from src.crypto_exchanges.Binance.config import COMPANY, url_api_get_history, headers_position
from src.data_pipeline.TransactionDataPipeline import TransactionDataPipeline
from src.data_push.load_data import create_engine_db, get_db_data
from src.data_push.scripts.query import select_transaction_filter_by_order_id_and_status_Binance
import copy
from src.utility.helper import request_post_wrapper
import random

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
today = datetime.now().strftime("%y%m%d")
# handler = logging.FileHandler(f'log/{today}/binancedetail.log')
# logger.addHandler(handler)
# logger.setLevel(logging.INFO)


company = file_name = COMPANY
transact_period = TRANSACT_PERIOD[0]

def Detail_API_get_position(trader_id, page_no=1) -> int:

    logger.info(f"Start {trader_id}")
    engine = create_engine_db()
    transaction_data_pipeline = TransactionDataPipeline(
        table_name="position_history_Binance", 
        exchange="Binance", 
        engine=engine,
        storage_queue_limit=50)
    query = text(select_transaction_filter_by_order_id_and_status_Binance)
        
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
        while True:
            if is_data_duplicated:
                break
            logger.info(f"{trader_id}: page - {page_no}")
            payload = {"pageNumber":page_no, "pageSize":50, "portfolioId":f"{trader_id}", "sort":"OPENING"}
            logger.info(f"payload : {payload}")
            user_agent = random.choice(USER_AGENTS)
            referer = f"https://www.binance.com/en/copy-trading/lead-details/{trader_id}"
            headers = copy.deepcopy(headers_position)
            headers["User-Agent"] = user_agent
            headers["Referer"] = referer

            response = request_post_wrapper(url_api_get_history, payload, headers)
            if not response:
                break
            response_content = response.content.decode('utf-8', errors='ignore')
            data = json.loads(response_content)

            transaction_history = data["data"]["list"]

            if not transaction_history:
                break

            for transaction in transaction_history:
                transaction_data = {
                    'order_no': transaction["id"],
                    'symbol': transaction["symbol"],
                    'type': transaction["type"],
                    'open_time': transaction["opened"],
                    'close_time': transaction["closed"],
                    'avg_cost': transaction["avgCost"],
                    'avg_close_price': transaction["avgClosePrice"],
                    'closing_pnl': transaction["closingPnl"],
                    'max_open_interest': transaction["maxOpenInterest"],
                    'closed_volume': transaction["closedVolume"],
                    'isolated': transaction["isolated"],
                    'side': transaction["side"],
                    'status': transaction["status"],
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
        transaction_data_pipeline.close_pipeline()
        engine.dispose()
        return 1