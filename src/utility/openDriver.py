from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import random
from src.config import USER_AGENTS
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def open_drivers(headless=False):
    """
    Docstring:
    
    """
    options = webdriver.ChromeOptions()
    user_agent = random.choice(USER_AGENTS)
    try:
        service = Service(ChromeDriverManager().install())
        #### if using capture program, use these options ####
        if headless:
            options.add_argument("headless")
        # options.add_argument("--window-size=1920,1080")
        driver = webdriver.Chrome(service=service, options=options)
        #####################################################
        driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": user_agent})
        driver.maximize_window()

        driver2 = webdriver.Chrome(service=service, options=options)
        driver2.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": user_agent})
        driver2.maximize_window()
    except:
        print("Error : Fail to create a Driver")
        quit()
    return driver, driver2


def open_driver(headless=False):
    """
    Docstring:
    
    """
    from selenium import webdriver
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service

    options = webdriver.ChromeOptions()
    user_agent = random.choice(USER_AGENTS[0])
    # options.add_extension("plugins/Hola-VPN-The-Website-Unblocker.crx")
    # options.add_extension("../Block-image.crx")
    try:
        service = Service(ChromeDriverManager().install())
        #### if using capture program, use these options ####
        if headless:
            options.add_argument("headless")
        options.add_argument('--blink-settings=imagesEnabled=false')
        options.add_argument("--window-size=1920,1080")
        driver = webdriver.Chrome(service=service, options=options)
        #####################################################
        driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": user_agent})
        driver.maximize_window()
        logger.info("Create a Driver successfully.")
    except:
        logger.info("Error : Fail to create a Driver.")
        return None
    return driver

def create_driver_list(num=2, headless=False):
    from src.utility.openDriver import open_driver_test
    driver_list = list()
    for _ in range(num):
        driver = open_driver_test(headless)
        attemp = 0
        while not driver:
            if attemp > 3:
                raise Exception("Can not create driver list.")
            driver = open_driver_test(headless)
            if not driver:
                attemp += 1
            else:
                break
        driver_list.append(driver)
    return driver_list