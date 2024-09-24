
import copy
from datetime import datetime
import logging
import time
import pandas as pd

from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from src.defines.urls import url_dic
from src.utility.helper import scroll_page_down

from src.config import RESULT, DATA_DIR, TRANSACT_PERIOD
from src.crypto_exchanges.OKX.config import COMPANY

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
today = datetime.now().strftime("%y%m%d")
# handler = logging.FileHandler(f'log/{today}/okxdetail.log')
# logger.addHandler(handler)
# logger.setLevel(logging.INFO)

today = datetime.now().strftime('%y%m%d')
company = file_name = COMPANY
transact_period = TRANSACT_PERIOD[0]

def Card(driver) -> pd.DataFrame:
    result_main = copy.deepcopy(RESULT)
    url = url_dic[company]
    driver.get(url)
    time.sleep(5)

    page_count = 0
    # select
    while True:
        cards_elem = driver.find_elements(
            By.XPATH, 
            "//div[@class='index_list__hDx8D ']/div")
        scroll_page_down(driver, 500)
        for card in cards_elem:
            try:
                trader_name = roi = url = pnl = ""
                name_elem = card.find_elements(
                    By.XPATH, 
                    ".//div[@class='index_headLeft__MYsXa']/span")
                if name_elem:
                    trader_name = card.find_element(
                        By.XPATH, 
                        ".//div[@class='index_headLeft__MYsXa']/span").text.strip()
                    
                    # logger.info(f"trader_name: {trader_name}")

                url_elem = card.find_elements(By.XPATH, ".//a")
                if url_elem:
                    url = card.find_element(By.XPATH, ".//a").get_attribute('href')
            except StaleElementReferenceException:
                logger.info(f"{StaleElementReferenceException} at {page_count}")
                continue
            if url:
                result_main['trader_name'].append(trader_name)
                result_main['ROI'].append(roi)
                result_main['PNL'].append(pnl)
                result_main['url'].append(url)

        logger.info(f"length of result_main = {len(result_main['trader_name'])}")


        pagination = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located(
                (By.XPATH, "//ul[@class='okui-pagination okui-pagination-center']")
                ))

        pagination_next = pagination.find_element(
            By.XPATH, ".//a[contains(@class, 'okui-powerLink okui-pagination-next')]")
        
        if 'disabled' in pagination_next.get_attribute('class'):
            break
        pagination_next.click()
        page_count += 1
        logger.info(f"page : {page_count}")
        time.sleep(2)

    # cards_elem[0].click()
    df = pd.DataFrame(result_main, dtype=str)
    df.to_csv(f"{DATA_DIR}/{today}/{file_name}_card.csv", index=None)
    return df