import copy
from datetime import datetime, timedelta
from functools import wraps
import logging
import os
import pandas as pd
import re
import time
import requests
import json
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from src.config import DATA_DIR, RESULT
from src.utility.saveFile import createDirectory

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def decorator_catch_exception(func):
    '''This is decorator to catch exception of executing a function'''
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            logger.info(f"{func.__name__} {e}")
            pass
    return wrapper

def is_element_located_css(soup, css_selector) -> bool:
    ''' This function checks element by css selector. 
    If an element exists, returns True, otherwise returns False.
    '''
    status = False
    elem = soup.select_one(css_selector)
    if elem:
        status = True
    return status

def is_element_located_xpath(driver, xpath) -> bool:
    '''This function checks element by css xpath. 
    If an element exists, returns True, otherwise returns False.
    '''
    status = False
    try:
        elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, xpath)))
        if elements:
            status = True
    except Exception:
        pass
    return status

def check_elements_css(*args) -> dict:
    '''This function checks list of elements by css selector.
    '''
    result_css = copy.deepcopy(RESULT)
    soup = args[0]
    elements_to_check = args[1]
    url = args[2]

    for name, css in elements_to_check.items():
        result_css["elem_name"].append(name)
        result_css["elem_selector"].append(css)
        is_element = is_element_located_css(soup, css)
        result_css["status"].append(is_element)
    result_css["url"] = [url for _ in range(len(result_css["status"]))]
    return result_css

def check_elements_xpath(*args) -> dict:
    '''This function checks list of elements by xpath.
    '''
    result_xpath = copy.deepcopy(RESULT)
    driver = args[0]
    elements_to_check = args[1]
    for name, xpath in elements_to_check.items():
        result_xpath["elem_name"].append(name)
        result_xpath["elem_selector"].append(xpath)
        is_element = is_element_located_xpath(driver, xpath)
        result_xpath["status"].append(is_element)
    result_xpath["url"] = [driver.current_url for _ in range(len(result_xpath["status"]))]
    logger.info(f"result_xpath = {result_xpath}")
    return result_xpath

def find_elements_with_retry(driver, xpath):
    from selenium.common.exceptions import TimeoutException
    attempt = 0 
    elements = ''
    while attempt < 2:
        try:
            elements = WebDriverWait(driver, 10 + attempt*2).until(
                EC.presence_of_all_elements_located((By.XPATH, xpath))
            )
            if elements: break
        except TimeoutException as e:
            print(f'Detail page scrapping Error: {type(e).__name__}')
            attempt += 1
            driver.implicitly_wait(10)
            print(f'No Element Located. No. of {attempt}')
            driver.get(driver.current_url)
    return elements

@decorator_catch_exception
def remove_files() -> None:
    '''This function removes all files inside a directory'''
    for filename in os.listdir(DATA_DIR):
        file_path = os.path.join(DATA_DIR, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')

    
@decorator_catch_exception
def print_report(file_path=f"{DATA_DIR}/data_merge.csv"):
    import pandas as pd

    df = pd.read_csv(file_path)
    df["url"] = df["url"].str[:45] # Truncate to proper size

    column_widths = {col: max(df[col].astype(str).map(len).max(), len(col)) for col in df.columns}
    dash = '   '.join(f"{'-'*column_widths[col]}" for col in df.columns)

    # Print column headers
    header = " | ".join(f"{col.center(column_widths[col])}" for col in df.columns)
    print(header)
    print(dash)
    # Print each row
    for _, row in df.iterrows():
        row_str = " | ".join(f"{str(row[col]).ljust(column_widths[col])}" for col in df.columns)
        print(row_str)
    print(dash)

def check_critical_result(*args) -> None:
    from math import prod
    result_dict = args[0]
    filename = args[1]
    if not prod(result_dict['status']): # Check whether all login elements are presented
        df = pd.DataFrame(result_dict)
        df.to_csv(f"{DATA_DIR}/{filename}.csv", index=None)
        raise Exception("CRITICAL : Login elements are not presented. Please check csv file.")
    return None

def scroll_page_down(driver, height=None):
    import time
    import random
    # choice = random.randint(0, 1)
    # if choice:
    SCROLL_PAUSE_TIME = 2
    # Get scroll height
    if not height:
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            # Scroll down to bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Wait to load page
            time.sleep(SCROLL_PAUSE_TIME)

            # Calculate new scroll height and compare with last scroll height
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
    else:
        driver.execute_script(f"window.scrollTo(0, {height});")


def control_VPN(status="close") -> None:
    import subprocess
    from src.config import VPN_DIR
    try:
        if status == "open":
            result = subprocess.run(
                [VPN_DIR, "connect"], 
                check=True, 
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,)
        else:
            result = subprocess.run(
                [VPN_DIR, "disconnect"], 
                check=True, 
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,)
        return result.returncode
    except Exception as e:
        logger.info(f'{e}: Can not connect to VPN.')
        return 1

def run_crawler(func):
    try:
        _ = func()
    except Exception as err:
        return f"{func.__code__.co_filename} : Crawling Error. {err}" # func.__code__.co_filename returns function directory 
    return f"{func.__code__.co_filename} : Crawling Successfully."

# def run_crawler_concurrent(crypto_exchange):
#     import concurrent.futures
#     with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
#         futures = [executor.submit(run_crawler, crypto) for crypto in crypto_exchange]
#         for future in concurrent.futures.as_completed(futures):
#             print(future.result())

def run_func_concurrently(func, list_arguments, max_workers=4):
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(func, item) for item in list_arguments]
        for future in concurrent.futures.as_completed(futures):
            print(future.result())

@decorator_catch_exception
def wrapper_control_VPN(crypto_exchange, required_VPN=False):
    '''This is decorator to catch exception of executing a function'''
    if not required_VPN:
        run_func_concurrently(func=run_crawler, list_arguments=crypto_exchange)
    else:
        try:
            status = control_VPN("open")
            time.sleep(5)
            if status == 0:
                run_func_concurrently(func=run_crawler, list_arguments=crypto_exchange)
                control_VPN()
        except Exception as e:
            raise e
        
def my_timer(orig_func):
    ''' Count the time each function running'''
    import time
    @wraps(orig_func) # place @wraps() it before wrapper function
    def wrapper(*args, **kwargs):
        t1 = time.time()
        result = orig_func(*args, **kwargs)
        t2 = time.time() - t1
        print(f'{orig_func.__name__} ran in {t2} seconds.')
        return result
    return wrapper

def request_post_wrapper(*args):
    url_api_get_history = args[0]
    payload = args[1]
    headers = args[2]
    attempt_count = 0
    connection_error = 0
    while True:
        if attempt_count > 5:
            if connection_error > 2:
                logger.info(f">>>>>>>>>>> CHECK THIS PAYLOAD <<<<<<<<<<<\n\t\t{payload}")
            return None
        try:
            response = requests.post(url_api_get_history, data=json.dumps(payload), headers=headers, timeout=5)
            time.sleep(2)
            logger.info(f"status : {response.status_code}")
            if response.status_code != 200:
                attempt_count += 1
                time.sleep(5 * attempt_count)
            else:
                return response
        except requests.exceptions.ConnectionError:
            attempt_count += 1
            connection_error += 1
            logger.info(f"Connection error payload : {payload}")
            time.sleep(5 * attempt_count)

def request_get_wrapper(*args):
    url_api_get_history = args[0]
    headers = args[1]

    attempt_count = 0
    connection_error = 0
    while True:
        if attempt_count > 5:
            if connection_error > 2:
                logger.info(f">>>>>>>>>>> CHECK THIS URL <<<<<<<<<<<\n\t\t{url_api_get_history}")
            return None
        try:
            response = requests.get(url_api_get_history, headers=headers, timeout=5)
            logger.info(f"request {url_api_get_history}")
            time.sleep(2)
            logger.info(f"status : {response.status_code}")
            if response.status_code != 200:
                attempt_count += 1
                time.sleep(5 * attempt_count)
            else:
                return response
        except requests.exceptions.ConnectionError:
            attempt_count += 1
            connection_error += 1
            logger.info(f"Connection error url : {url_api_get_history}")
            time.sleep(5 * attempt_count)

def get_timestamp_now(milisec=False):
    if not milisec:
        return round(datetime.timestamp(datetime.now()))
    else:
        return round(datetime.timestamp(datetime.now())*1000)