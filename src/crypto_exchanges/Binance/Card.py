import copy
from datetime import datetime
import requests
import json
import logging
import time
import pandas as pd

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import StaleElementReferenceException

from src.config import RESULT, DATA_DIR, TRANSACT_PERIOD, USER_AGENTS
from src.crypto_exchanges.Binance.config import COMPANY, url_api, headers, payload
from src.defines.urls import url_dic
from src.utility.helper import scroll_page_down
import random

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
today = datetime.now().strftime("%y%m%d")
# handler = logging.FileHandler(f'log/{today}/binancedetail.log')
# logger.addHandler(handler)
# logger.setLevel(logging.INFO)


company = file_name = COMPANY
transact_period = TRANSACT_PERIOD[0]

def select_transact_date(driver) -> None:
    button_select_transact_date = driver.find_element(
        By.XPATH, "//div[@class='bn-select-field data-single data-size-middle data-line']")
    button_select_transact_date.click() #activate

    time.sleep(1)

    selector_elem = driver.find_element(
        By.XPATH, 
        "//div[@class='bn-bubble bn-bubble__unset shadow data-font-14 bn-tooltips active bn-select-bubble']")
    
    buttons = selector_elem.find_elements(
        By.XPATH, 
        ".//div[@class='bn-select-overlay-options']/div")
    
    buttons[0].click() # Select for 7D
    time.sleep(1)
    button_select_transact_date.click()
    time.sleep(2)
    return

def Card_selem(driver) -> dict:
    result_main = copy.deepcopy(RESULT)
    url = url_dic[company]
    driver.get(url)
    time.sleep(5)
    select_transact_date(driver)
    page_count = 0
    # select
    while True:
        cards_elem = driver.find_elements(
            By.XPATH, 
            "//div[@class='mt-[24px] grid gap-[16px] grid-cols-1 md:grid-cols-2 lg:grid-cols-3 w-full mb-[26px]']/div")
        scroll_page_down(driver)
        for card in cards_elem:
            trader_name = roi = url = pnl = ""
            try:
                name_elem = card.find_elements(
                    By.XPATH, 
                    ".//div[@class='bn-flex items-center mb-[2px] gap-[2px]']/div[1]")
                if name_elem:
                    trader_name = card.find_element(
                        By.XPATH, 
                        ".//div[@class='bn-flex items-center mb-[2px] gap-[2px]']/div[1]").text.strip()
                    
                    logger.info(f"trader_name: {trader_name}")

                ratios_elem = card.find_elements(
                    By.XPATH, 
                    ".//div[@class='bn-flex gap-[16px] justify-between items-center']/div[1]")
                if ratios_elem:
                    ratios_elem_ = card.find_element(By.XPATH, ".//div[@class='bn-flex gap-[16px] justify-between items-center']/div[1]")

                    pnl_elem = ratios_elem_.find_elements(By.XPATH, './/div[2]')
                    if pnl_elem:
                        pnl = ratios_elem_.find_element(By.XPATH, './/div[2]').text
                        # pnl = re.sub('[\+|\-|\,]', '', pnl)
                    
                    roi_elem = ratios_elem_.find_elements(By.XPATH, './/div[3]')
                    if roi_elem:
                        roi = ratios_elem_.find_element(By.XPATH, './/div[3]').text
                        # roi = re.sub(r'ROI\n', '', roi)
            except StaleElementReferenceException:
                continue

            result_main['trader_name'].append(trader_name)
            result_main['ROI'].append(roi)
            result_main['PNL'].append(pnl)
            result_main['url'].append(url)
            logger.info(f"length of result_main = {len(result_main['trader_name'])}")

        pagination = driver.find_element(
            By.XPATH, "//div[@class='bn-pagination']")
        pagination_next = pagination.find_element(By.XPATH, ".//div[contains(@class, 'bn-pagination-next')]")
        if 'disabled' in pagination_next.get_attribute('class'):
            break
        pagination_next.click()
        page_count += 1
        logger.info(f"page : {page_count}")
        time.sleep(5)


    result_main['cryto_exchange'] = [company for _ in range(len(result_main['trader_name']))]
    result_main['transact_period'] = [transact_period for _ in range(len(result_main['trader_name']))]
    # cards_elem[0].click()
    df = pd.DataFrame(result_main)
    df.to_csv(f"{DATA_DIR}/{today}/{file_name}_card.csv")

    return df

def Card_API() -> dict:
    result_main = copy.deepcopy(RESULT)
    result_main['user_id'] = []
    start_page = 1
    while True:
        payload["pageNumber"] = start_page
        user_agent = random.choice(USER_AGENTS)
        headers["User-Agent"] = user_agent
        response = requests.post(url_api, data=json.dumps(payload), headers=headers)
        time.sleep(2)
        # logger.info(f"current page: {start_page}")
        # logger.info(f"payload: {payload}")
        content = response.content.decode('utf-8', errors='ignore')
        parsed_content = json.loads(content)
        with open(f'crawling_data/{company}/{today}/{company}_page_{start_page}.json', 'w') as f:
            json.dump(parsed_content, f)
        # logger.info("content found")
        users = parsed_content["data"]["list"]
        if not users:
            break
        for user in users:
            try:
                trader_name = roi = pnl = url = user_id = ""
                user_id = user['leadPortfolioId']
                trader_name = user['nickname'].strip()
                roi = user['roi']
                pnl = user['pnl']
                url = f"https://www.binance.com/en/copy-trading/lead-details/{user_id}?timeRange=7D"
                result_main['user_id'].append(user_id)
                result_main['trader_name'].append(trader_name)
                result_main['ROI'].append(roi)
                result_main['PNL'].append(pnl)
                result_main['url'].append(url)
            except IndexError:
                logger.info(f'IndexError at user: {user} page {start_page}')
                continue

        start_page += 1
    result_main['crypto_exchange'] = [company for _ in range(len(result_main['trader_name']))]
    result_main['transact_period'] = [transact_period for _ in range(len(result_main['trader_name']))]
    df = pd.DataFrame(result_main, dtype=str)
    df.to_csv(f"{DATA_DIR}/{today}/{file_name}.csv", index=None)