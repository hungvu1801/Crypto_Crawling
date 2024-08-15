from dotenv import load_dotenv
import time
import os
from bs4 import BeautifulSoup as bs
from selenium.webdriver.common.by import By
from defines.msg import print_msg
from defines.urls import url_dic

def logout_k(driver):
    print_msg("logout")
    driver.get(url_dic["KAuction"]["logout"])
    
def logout_seoul(driver):
    print_msg("logout")
    driver.execute_script("arguments[0].click();", driver.find_element(By.XPATH, "//*[@class='utility-login']/a[1]"))
    
def logout_i(driver):
    print_msg("logout")
    driver.get(url_dic["IAuction"]["logout"])
    
def logout_a(driver):
    print_msg("logout")
    driver.get(url_dic["AAuction"]["logout"])
    
def logout_myart(driver):
    print_msg("logout")
    driver.get(url_dic["MyartAuction"]["logout"])
    
def logout_raiz(driver):
    print_msg("logout")
    driver.get(url_dic["RaizartAuction"]["logout"])
    
def logout_mallet(driver):
    print_msg("logout")
    driver.get(url_dic["MalletAuction"]["logout"])