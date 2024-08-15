import copy
from datetime import datetime
from functools import wraps
import logging
import os
import pandas as pd
import re
import time

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
    def wrapper(*args):
        try:
            result = func(*args)
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
            print(f'페이지를 새로고침합니다. {attempt}회 시도')
            driver.get(driver.current_url)
    return elements

@decorator_catch_exception
def data_merge(today) -> None:
    '''This function find all .csv files and merge into one file'''
    auction_datas = list()
    for file in os.listdir(f"{DATA_DIR}/{today}"):
        if os.path.splitext(file)[1] == ".csv" and (
            not re.search('card', file) and not re.search('detail', file) and not re.search('data_merge', file)):
            auction_datas.append(os.path.join(f"{DATA_DIR}/{today}", file))
            print(file)
    
    if auction_datas:
        df_list = list()
        for filename in auction_datas:
            df = pd.read_csv(filename, index_col=None, header=0)
            df_list.append(df)
        df = pd.concat(df_list, axis=0, ignore_index=True)
        
        df.to_csv(f"{DATA_DIR}/{today}/data_merge.csv", 
            index=None)
        
@decorator_catch_exception
def data_merge_new(today) -> None:
    '''This function find all .csv files and merge into one file'''
    companies = ['OKX', 'Binance', 'Bitget', 'Bybit']
    df_list = list()
    for company in companies:
        try:
            df = pd.read_csv(
                f"crawling_data/{today}/{company}.csv", 
                header=0, 
                index_col=None, 
                dtype={'user_id': str})
            
        except Exception:
            return
        if company == 'OKX':
            df["ROI"] = df["ROI"] * 100
        if company == 'Bitget' or company == 'Bybit':
            df["ROI"] = df['ROI'].str.replace('[\%,]', '', regex=True).astype(float)
            df['PNL'] = df['PNL'].str.replace('[\$,]', '', regex=True).astype(float)
        df_list.append(df)
    df = pd.concat(df_list, axis=0, ignore_index=True)
    df.to_csv(f"{DATA_DIR}/{today}/data_merge.csv",
        index=None)   

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

def create_directories(today) -> None:
    # Create a folder to contain src result for each auction
    createDirectory(f"{DATA_DIR}/{today}")
    company_list = ["Binance", "Bybit", "OKX", "Bitget"]
    for company in  company_list:
        createDirectory(f"{DATA_DIR}/{company}/{today}")

def pre_process_result(today) -> None:
    df_OKX = pd.read_csv(f"crawling_data/{today}/OKX.csv", header=0)
    # df_Binance = pd.read_csv(f"crawling_data/{today}/Binance.csv", header=0)
    df_Bitget = pd.read_csv(f"crawling_data/{today}/Bitget.csv", header=0)
    df_Bybit = pd.read_csv(f"crawling_data/{today}/Bybit.csv", header=0)

    df_OKX["ROI"] = df_OKX["ROI"] * 100
    try:
        df_Bitget["ROI"] = df_Bitget['ROI'].str.replace('%', '').astype(float)
    except AttributeError:
        pass 
    try:
        df_Bitget['PNL'] = df_Bitget['PNL'].str.replace('[\$,]', '', regex=True).astype(float)
    except AttributeError:
        pass
    try:
        df_Bybit["ROI"] = df_Bybit['ROI'].str.replace('%', '').astype(float)
    except AttributeError:
        pass
    try:
        df_Bybit['PNL'] = df_Bybit['PNL'].str.replace('[\$,]', '', regex=True).astype(float)
    except AttributeError:
        pass
    df_OKX.to_csv(f"crawling_data/{today}/OKX.csv", index=None)
    # df_Binance = pd.read_csv(f"crawling_data/{today}/Binance.csv", index=None)
    df_Bitget.to_csv(f"crawling_data/{today}/Bitget.csv", index=None)
    df_Bybit.to_csv(f"crawling_data/{today}/Bybit.csv", index=None)

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

def run_crawler_concurrent(crypto_exchange):
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(run_crawler, crypto) for crypto in crypto_exchange]
        for future in concurrent.futures.as_completed(futures):
            print(future.result())

@decorator_catch_exception
def wrapper_control_VPN(crypto_exchange, required_VPN=False):
    '''This is decorator to catch exception of executing a function'''
    if not required_VPN:
        run_crawler_concurrent(crypto_exchange)
    else:
        try:
            status = control_VPN("open")
            time.sleep(5)
            if status == 0:
                run_crawler_concurrent(crypto_exchange)
                control_VPN()
        except Exception as e:
            raise e