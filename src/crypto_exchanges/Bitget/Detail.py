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

from src.config import DATA_DIR, TRANSACT_PERIOD
from src.crypto_exchanges.Bitget.config import COMPANY

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

today = datetime.now().strftime('%y%m%d')
company = file_name = COMPANY
transact_period = TRANSACT_PERIOD[0]

def Detail_API(df) -> pd.DataFrame:
    
    url = "https://www.bitget.com/v1/trigger/trace/public/cycleData"
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Content-Type": "application/json;charset=UTF-8",
        "DNT": "1",
        "Host": "www.bitget.com",
        "Origin": "null",
        "Priority": "\"u=0, i\"",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-GPC": "1",
        "TE": "trailers",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    }

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