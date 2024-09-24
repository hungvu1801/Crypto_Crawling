# directory
import os
import shutil
# exchange scraped urls to s3 ones
import re
import time
import urllib.request
from urllib.parse import quote
from urllib.error import ContentTooShortError
import ssl
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def createDirectory(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print("Directory not exists.")

def create_directories(today) -> None:
    from src.config import DATA_DIR
    # Create a folder to contain src result for each auction
    createDirectory(f"{DATA_DIR}/{today}")
    company_list = ["Binance", "Bybit", "OKX", "Bitget"]
    for company in  company_list:
        createDirectory(f"{DATA_DIR}/{company}/{today}")

def removeDirectory(directory):
    if os.path.exists(directory):
        print("임시폴더 삭제")
        shutil.rmtree(directory)

def toExcel(df, directory, name):
    createDirectory(directory)
    df.to_excel(directory + name, engine = 'xlsxwriter', index=False)
    print(f"저장경로 : {directory}\n파일명 : {name}")

def toJpg(**kwargs):
    # **kwargs is a dictionary contains parameters
    url = kwargs["url"]
    directory = kwargs["directory"]
    name = kwargs["name"]
    ####### for solving Error "SSL: CERTIFICATE_VERIFY_FAILED" #######
    ssl._create_default_https_context = ssl._create_unverified_context
    ##################################################################
    opener = urllib.request.build_opener()
    opener.addheaders=[('User-Agent','Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36')]
    urllib.request.install_opener(opener) # Add user-agent for each request
    num_of_tried = 0 # Count number of time the request is sent for each image
    while num_of_tried < 2:
        time.sleep(1)
        try:
            if type(url) == str:
                url = quote(url, safe = ':/')
                logger.info(f"Downloading: url : {url}, directory : {directory}, lot : {name}")
                urllib.request.urlretrieve(url, f"{directory}/{name}.jpg")
                break
            else:
                print("url을 확인해주세요 :", url)
                return
        except ContentTooShortError: # exception when image is broken, try send request again
            logger.info(f"{ContentTooShortError} -- Retry Download S3 lot = {name}; s3_path = {directory}")
            num_of_tried += 1
        except ValueError: # exception when url is not downloadable
            logger.info(f"url is not valid : {url}")
            print("url을 확인해주세요 :", url)
            return
    if num_of_tried >= 2:
        raise ContentTooShortError