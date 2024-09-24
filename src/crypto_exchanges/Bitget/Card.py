import copy
from datetime import datetime
import pandas as pd
import re
import logging
import time

from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException

from src.config import RESULT, DATA_DIR, TRANSACT_PERIOD
from src.crypto_exchanges.Bitget.config import COMPANY
from src.defines.urls import url_dic
from src.utility.helper import scroll_page_down

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
today = datetime.now().strftime("%y%m%d")
# handler = logging.FileHandler(f'log/{today}/bitgetdetail.log')
# logger.addHandler(handler)
# logger.setLevel(logging.INFO)


company = file_name = COMPANY
transact_period = TRANSACT_PERIOD[0]

def Card(driver) -> pd.DataFrame:
    result_main = copy.deepcopy(RESULT)
    result_main['user_id'] = []
    url_index = url_dic[company]
    driver.get(url_index)
    time.sleep(5)

    driver.find_element(By.XPATH, "//div[@class='bit-dropdown']").click()
    time.sleep(1)
    # transact_elems = driver.find_elements(By.XPATH, "//ul[@id='bit-id-100-12']/li")
    transact_elems = driver.find_elements(By.XPATH, "//ul[@class='bit-dropdown-menu']/li")
    
    if transact_elems:
        transact_elems[0].click()
        time.sleep(2)
    page_count = 1
    # select
    while True:
        logger.info(f"page : {page_count}")
        page_count += 1
        cards_elem = driver.find_elements(
            By.XPATH, 
            "//div[contains(@class, '_carditem')]")
        scroll_page_down(driver, 1500)

        for card in cards_elem:
            trader_name = roi = url = pnl = user_id = ""
            try:
                url_elem = card.find_elements(By.XPATH, ".//a")
                if url_elem:
                    url = card.find_element(By.XPATH, ".//a").get_attribute('href')
                    pattern = r"trader/([a-zA-Z0-9]+)/futures"
                    # Use re.search to find the match
                    match = re.search(pattern, url)
                    if match:
                        user_id = match.group(1)

                name_elem = card.find_elements(
                    By.XPATH,
                    ".//a/div/div/div/div[2]/div/div")
                if name_elem:
                    trader_name = card.find_element(
                        By.XPATH, 
                        ".//a/div/div/div/div[2]/div/div").text.strip()
                    
                    # logger.info(f"trader_name: {trader_name}")

            except StaleElementReferenceException:
                continue

            result_main['trader_name'].append(trader_name)
            result_main['user_id'].append(user_id)
            result_main['ROI'].append(roi)
            result_main['PNL'].append(pnl)
            result_main['url'].append(url)

        logger.info(f"length of result_main = {len(result_main['trader_name'])}")

        pagination = driver.find_elements(
            By.XPATH, "//div[contains(@class, 'bit-pagination')]")
        if pagination:
            button_nxt_page = driver.find_element(
                By.XPATH, 
                "//div[contains(@class, 'bit-pagination')]/button[@class='btn-next is-last']")
            if button_nxt_page.get_attribute('aria-disabled') == "true":
                break
            else:
                next_page_url = f"{url_index}/{page_count}"
                driver.get(next_page_url)
        else:
            break
        time.sleep(2)

    result_main['crypto_exchange'] = [company for _ in range(len(result_main['trader_name']))]
    result_main['transact_period'] = [transact_period for _ in range(len(result_main['trader_name']))]

    df = pd.DataFrame(result_main, dtype=str)
    df.to_csv(f"{DATA_DIR}/{today}/{file_name}_card.csv", index=None)
    return df