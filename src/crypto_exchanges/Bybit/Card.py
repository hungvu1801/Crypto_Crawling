
import copy
from datetime import datetime
import time

import json
import logging
import pandas as pd

from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException

from src.config import RESULT, TRANSACT_PERIOD
from src.crawling.findElement import find_element_with_retry
from src.crypto_exchanges.Bybit.config import COMPANY
from src.defines.urls import url_dic
from src.utility.helper import scroll_page_down, get_timestamp_now

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

today = datetime.now().strftime('%y%m%d')
company = file_name = COMPANY
transact_period = TRANSACT_PERIOD[0]

def Card_selem(driver) -> pd.DataFrame:
    result_main = copy.deepcopy(RESULT)
    url_index = url_dic[company]
    driver.get(url_index)
    time.sleep(5)
    scroll_page_down(driver)

    button_view_all_traders = driver.find_element(By.XPATH, "//div[@class='leader-list__recommend-seeMore-inside']")
    button_view_all_traders.click()
    time.sleep(1)

    driver.execute_script(f"window.scrollTo(0, 0);") # scroll up

    page_count = 1
    # select
    while True:
        logger.info(f"page : {page_count}")
        page_count += 1
        cards_elem = driver.find_elements(
            By.XPATH, 
            "//div[contains(@class, 'trader-list__Item ')]")
        scroll_page_down(driver, 1500)

        for card in cards_elem:
            trader_name = roi = url = pnl = ""
            try:
                url_elem = card.find_elements(By.XPATH, ".//a")
                if url_elem:
                    url = card.find_element(By.XPATH, ".//a").get_attribute('href')

                name_elem = card.find_elements(
                    By.XPATH,
                    ".//a/div/div/div/div[2]/div/div")
                if name_elem:
                    trader_name = card.find_element(
                        By.XPATH, 
                        ".//a/div/div/div/div[2]/div/div").text
                    
                    logger.info(f"trader_name: {trader_name}")

            except StaleElementReferenceException:
                continue
        result_main['cryto_exchange'] = [company for _ in range(len(result_main['trader_name']))]
        result_main['transact_period'] = [transact_period for _ in range(len(result_main['trader_name']))]
        # cards_elem[0].click()
        df = pd.DataFrame(result_main, dtype=str)
        return df
    
def Card_API(driver) -> pd.DataFrame:
    result_main = copy.deepcopy(RESULT)
    result_main['user_id'] = []
    epoch_time_now = get_timestamp_now()
    start_page = 1
    total_page_count = 0
    while True:
        api_url = f"https://api2.bybit.com/fapi/beehive/public/v1/common/dynamic-leader-list?timeStamp={epoch_time_now}&pageNo={start_page}&pageSize=64&userTag=&dataDuration=DATA_DURATION_SEVEN_DAY&leaderTag=&code=&leaderLevel="
        logger.info(f"url: {api_url}")
        logger.info(f"current page: {start_page}")
        driver.get(api_url)
        time.sleep(2)
        content_elem = find_element_with_retry(driver, 'pre')

        content = content_elem.text
        
        parsed_content = json.loads(content)
        with open(f'crawling_data/Bybit/{today}/Bybit_page_{start_page}.json', 'w') as f:
            json.dump(parsed_content, f)
        
        if start_page == 1:
            total_page_count = parsed_content["result"]["totalPageCount"]
            if total_page_count.isnumeric():
                total_page_count = int(total_page_count)
                logger.info(f"total_page_count: {total_page_count}")
        users = parsed_content["result"]["leaderDetails"]
        for user in users:
            try:
                trader_name = roi = pnl = url = user_id = ""

                user_id = user["leaderUserId"]
                trader_name = user["nickName"].strip()
                roi = user["metricValues"][0]
                pnl = user["metricValues"][1]
                url = user["leaderMark"]

                result_main['user_id'].append(user_id)
                result_main['trader_name'].append(trader_name)
                result_main['ROI'].append(roi)
                result_main['PNL'].append(pnl)
                result_main['url'].append(url)
            except IndexError:
                logger.info(f'IndexError at user: {user} page {start_page}')
                continue

        start_page += 1
        if start_page > total_page_count:
            break
    
    result_main['crypto_exchange'] = [company for _ in range(len(result_main['trader_name']))]
    result_main['transact_period'] = [transact_period for _ in range(len(result_main['trader_name']))]
    # cards_elem[0].click()
    df = pd.DataFrame(result_main, dtype=str)
    return df