from dotenv import load_dotenv
import time
import os
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from defines.urls import url_dic
from defines.msg import print_msg
from crawling.ignoreError import get_page_with_retry
# login은 element 선택 시 popup 로그인창에 로그인하거나, /login 인 페이지에서 로그인하거나 두가지 방식이 있다.
# kanauction은 로그인 없이 수집 가능


def login_k(driver):
    load_dotenv()
    print_msg("contact")
    driver.get(url_dic["KAuction"]["kor"])
    
    # ignore popup 1st
    try:
        driver.find_element(By.XPATH, "//*[@class='maincl-btn']"
            ).send_keys(Keys.ENTER)
    except: pass
    
    print_msg("login")
    driver.find_element(By.XPATH, "//div[@class='header-util']/a").click()
    time.sleep(1)

    # Type username
    driver.find_element(
        By.XPATH, "//input[@id='modal-login-id']"
        ).send_keys(os.environ.get("auctionID1"))

    # Type password
    driver.find_element(
        By.XPATH, "//input[@id='modal-login-pwd']"
        ).send_keys(os.environ.get("auctionPW1"))
    
    # Click button LogIn
    driver.find_element(
        By.XPATH,
        "//button[@class='btn btn-block btn-primary btn-lg m-t-10 m-b-10']"
    ).click()

    # ignore popup 2nd
    try:
        driver.find_element(By.XPATH, "//*[@class='maincl-btn']"
            ).send_keys(Keys.ENTER)
    except: pass
    

def login_seoul(driver):
    load_dotenv()
    print_msg("contact")

    driver.get(url_dic["SeoulAuction"]["login"])
    print_msg("login")
    driver.find_element(
        By.XPATH, "//*[@id='loginId']").send_keys(
        os.environ.get("auctionID2")
    )
    driver.find_element(By.XPATH, "//*[@id='password']").send_keys(
        os.environ.get("auctionPW2")
    )
    
    driver.find_element(
        By.XPATH, '//*[@id="contents"]/section/div/div/div/article/div/a'
        ).send_keys(Keys.ENTER)
    
    # ignore popup alerting changing password
    try:
        driver.find_element(By.XPATH, '/html/body/div/div/header/nav/section[1]/div[1]/ul/li[1]/a'
            ).send_keys(Keys.ENTER)
    except: pass
    


def login_i(driver):
    load_dotenv()
    print_msg("contact")
    driver.get(url_dic["IAuction"]["login"])
    
    print_msg("login")
    driver.find_element(By.XPATH, "//*[@id='id']").send_keys(
        os.environ.get("auctionID2")
    )
    driver.find_element(By.XPATH, "//*[@id='pw']").send_keys(
        os.environ.get("auctionPW3")
    )
    driver.execute_script("javascript:fnc_login();")
    
    
def login_a(driver):
    load_dotenv()
    print_msg("contact")
    driver.get(url_dic["AAuction"]["login"])
    
    print_msg("login")
    driver.find_element(By.XPATH, "//*[@id='login_id']").send_keys(
        os.environ.get("auctionID3")
    )
    driver.find_element(By.XPATH, "//*[@id='login_pw']").send_keys(
        os.environ.get("auctionPW4")
    )
    driver.find_element(By.XPATH, "//*[@type='submit']").click()
    
    
    
def login_myart(driver):
    load_dotenv()
    print_msg("contact")
    
    get_page_with_retry(driver, url_dic["MyartAuction"]["login"])
    
    print_msg("login")
    driver.find_element(By.XPATH, "//*[@name='user_id']").send_keys(
        os.environ.get("auctionID2")
    )
    driver.find_element(By.XPATH, "//*[@name='password']").send_keys(
        os.environ.get("auctionPW2")
    )
    time.sleep(1)
    driver.find_element(By.XPATH, "//*[@class='bt_login']").click()
    
    

def login_raiz(driver):
    load_dotenv()
    print_msg("contact")
    driver.get(url_dic["RaizartAuction"]["login"])
    
    print_msg("login")
    driver.find_element(By.XPATH, "//*[@name='login_id']").send_keys(
        os.environ.get("auctionID2")
    )
    driver.find_element(By.XPATH, "//*[@name='login_password']").send_keys(
        os.environ.get("auctionPW2")
    )
    driver.find_element(By.XPATH, "//*[@class='login-btn']").click()
    
    
def login_mallet(driver):
    load_dotenv()
    print_msg("contact")
    
    driver.get(url_dic["MalletAuction"]["login"])
    
    # ignore popup which select language
    driver.find_element(By.XPATH, '//*[@id="modal_2"]/div[2]/div/div/button[2]').send_keys(Keys.ENTER)
    
    driver.find_element(By.CSS_SELECTOR, 'div.panel-body form.form-horizontal div:nth-of-type(1) input').send_keys(
        os.environ.get("auctionID_japan")
    )
    driver.find_element(By.CSS_SELECTOR, 'div.panel-body form.form-horizontal div:nth-of-type(2) input').send_keys(
        os.environ.get("auctionPW_Mallet")
    )
    
    driver.find_element(By.XPATH, '/html/body/article/section/div/div/div/div/div[2]/form/div[3]/div/button').click()