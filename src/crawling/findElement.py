from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
import time

def find_element_with_retry(driver, tag_name):
    attempt = 0 
    element = ''
    while attempt < 3:
        try:
            element = WebDriverWait(driver, 10 + attempt*2).until(
                EC.presence_of_element_located((By.TAG_NAME, tag_name))
            )
            if element: return element
        except TimeoutException as e:
            attempt += 1
            driver.get(driver.current_url)
            time.sleep(10)
            continue
    if attempt == 3:
        raise TimeoutException
    
    
def find_visible_element_with_retry(driver, xpath):
    attempt = 0
    element = ''
    while attempt < 3:
        try: 
            element = WebDriverWait(driver, 10 + attempt*2).until(
                EC.visibility_of_element_located((By.XPATH, xpath))
            )
            if element and element.text: return element
        except TimeoutException as e:
            print(f'Detail page scrapping Error: {type(e).__name__}')
            attempt += 1
            driver.implicitly_wait(10)
            print(f'페이지를 새로고침합니다. {attempt}회 시도')
            driver.get(driver.current_url)
            continue
        except StaleElementReferenceException as e:
            print(f'Detail page scrapping Error: {type(e).__name__}')
            attempt += 1
            driver.implicitly_wait(10)
            print(f'페이지를 새로고침합니다. {attempt}회 시도')
            driver.get(driver.current_url)
            continue
    print('Error: element 수집 불가')