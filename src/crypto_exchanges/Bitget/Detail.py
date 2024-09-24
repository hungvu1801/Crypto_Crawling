import brotli
from datetime import datetime
import pandas as pd
import json
import logging
import requests
import re
import time
import zlib

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from src.data_push.load_data import create_engine_db
from src.config import DATA_DIR, TRANSACT_PERIOD, USER_AGENTS
from src.data_pipeline.TransactionDataPipeline import TransactionDataPipeline
from src.crypto_exchanges.Bitget.config import COMPANY, url, headers, url_api_get_history, headers_position
from src.utility.helper import request_post_wrapper
import copy

import random
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
today = datetime.now().strftime("%y%m%d")
# handler = logging.FileHandler(f'log/{today}/bitgetdetail.log')
# logger.addHandler(handler)
# logger.setLevel(logging.INFO)

company = file_name = COMPANY
transact_period = TRANSACT_PERIOD[0]
    
def Detail_API(df) -> pd.DataFrame:

    for url in df['url']:
        roi = pnl = ""
        pattern = r"trader/([a-zA-Z0-9]+)/futures"
        # Use re.search to find the match
        match = re.search(pattern, url)
        if match:
            user_id = match.group(1)
        else:
            continue
        payload = {
            "languageType": 0, 
            "triggerUserId": f"{user_id}", 
            "cycleTime": 7}
        logger.info(f"user_id: {user_id}")
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        time.sleep(1)
        if response.headers.get('Content-Encoding') == 'gzip':
            response_content = zlib.decompress(response.content, zlib.MAX_WBITS | 16)
        elif response.headers.get('Content-Encoding') == 'deflate':
            response_content = zlib.decompress(response.content)
        elif response.headers.get('Content-Encoding') == 'br':
            response_content = brotli.decompress(response.content)
        else:
            response_content = response.content

        response_content = response_content.decode('utf-8', errors='ignore')
        data = json.loads(response_content)
        try:
            roi = data['data']['roiRows']['rows'][-1]['amount']
            pnl = data['data']['pnlRows']['rows'][-1]['amount']
        except Exception:
            logger.info(f"Error parsing roi or pnl user_id : {user_id}")
            continue

        df.loc[df["url"] == url, "ROI"] = roi
        df.loc[df["url"] == url, "PNL"] = pnl
    df.to_csv(f"{DATA_DIR}/{today}/{file_name}_detail.csv", index=None)
    return df

def Detail_selem(driver, df) -> pd.DataFrame:
    for url in df['url']:
        try:
            roi = pnl = ""
            driver.get(url)
            transact_button = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located(
                    (By.XPATH, '(//div[contains(@class, "bit-radio-group")])[2]/label[1]')))
            transact_button.click()
            time.sleep(1)
            performance_elem = driver.find_elements(By.XPATH, "//div[@class='flex justify-between pt-[16px] pb-[12px]']")
            if performance_elem:
                roi_elem = driver.find_element(
                    By.XPATH, "//div[@class='flex justify-between pt-[16px] pb-[12px]']/div[1]/span")
                pnl_elem = driver.find_element(
                    By.XPATH, "//div[@class='flex justify-between pt-[16px] pb-[12px]']/div[2]/span")
                roi = roi_elem.text
                pnl = pnl_elem.text
                df.loc[df["url"] == url, "ROI"] = roi
                df.loc[df["url"] == url, "PNL"] = pnl
        except Exception as e:
            logger.info(f"Exception in url {url}")
            continue
    # df.to_csv(f"{DATA_DIR}/{today}/{file_name}_detail.csv", index=None)
    return df

def Detail_API_get_position(trader_id, page_no=1) -> int:


    engine = create_engine_db()
    logger.info(f"Start {trader_id}")
    transaction_data_pipeline = TransactionDataPipeline(
        table_name="position_history_Bitget", 
        exchange="Bitget", 
        engine=engine)
    is_data_duplicated = False
    try:
        while True:
            if is_data_duplicated:
                break
            logger.info(f"{trader_id}: page - {page_no}")
            payload = {"languageType":0, "traderUid":trader_id, "pageNo":page_no, "pageSize":20}
            logger.info(f"payload : {payload}")
            user_agent = random.choice(USER_AGENTS)
            referer = f"https://www.bitget.com/copy-trading/trader/{trader_id}/futures-order"
            headers = copy.deepcopy(headers_position)
            headers["User-Agent"] = user_agent
            headers["Referer"] = referer

            response = request_post_wrapper(url_api_get_history, payload, headers)
            if not response:
                break
            response_content = response.content.decode('utf-8', errors='ignore')
            data = json.loads(response_content)

            transaction_history = data["data"]["rows"]

            if not transaction_history:
                break

            for transaction in transaction_history:
                transaction_data = {
                    'achieved_profits': transaction['achievedProfits'],
                    'close_avg_price': transaction['closeAvgPrice'],
                    'close_deal_count': transaction['closeDealCount'],
                    'close_fee': transaction['closeFee'],
                    'close_time': transaction['closeTime'],
                    'net_profit': transaction['netProfit'],
                    'open_avg_price': transaction['openAvgPrice'],
                    'open_deal_count': transaction['openDealCount'],
                    'open_fee': transaction['openFee'],
                    'open_margin_count': transaction['openMarginCount'],
                    'open_time': transaction['openTime'],
                    'open_level': transaction['openLevel'],
                    'order_no': transaction['orderNo'],
                    'margin_mode': transaction['marginMode'],
                    'position': transaction['position'],
                    'left_symbol': transaction['leftSymbol'],
                    'token_id': transaction['tokenId'],
                    'position_average': transaction['positionAverage'],
                    'product_code': transaction['productCode'],
                    'return_rate': transaction['returnRate'],
                    'user_name': transaction['userName'],
                    'teacher_id': transaction['teacherId'],
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                }

                is_data_duplicated = transaction_data_pipeline.add_transaction(transaction_data)
                logger.info(f"is_data_duplicated : {is_data_duplicated}")
                # break
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


def Detail_API_get_coin() -> pd.DataFrame:
    return