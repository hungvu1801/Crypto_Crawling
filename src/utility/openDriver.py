from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
def open_driver():
    """
    Docstring:
    
    """
    options = webdriver.ChromeOptions()
    useragent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"

    try:
        service = Service(ChromeDriverManager().install())
        #### if using capture program, use these options ####
        # options.add_argument("headless")
        # options.add_argument("--window-size=1920,1080")
        driver = webdriver.Chrome(service=service, options=options)
        #####################################################
        driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": useragent})
        driver.maximize_window()

        # options.add_argument("headless")
        driver2 = webdriver.Chrome(service=service, options=options)
        driver2.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": useragent})
        driver2.maximize_window()
    except:
        print("Error : 프로그램을 종료합니다.")
        notice = """1. chromedriver가 디렉토리 내에 없습니다, 설치해주세요.
                    2. 리눅스 운영체제입니다. 리눅스는 사용할 수 없습니다.
                    3. selenium 버전이 낮습니다. 최신 버전으로 업데이트 해주세요.
                    4. 기타 원인"""
        print(notice)
        quit()
    return driver, driver2


def open_driver_test():
    """
    Docstring:
    
    """
    from selenium import webdriver
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service

    options = webdriver.ChromeOptions()
    useragent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    # options.add_extension("plugins/Hola-VPN-The-Website-Unblocker.crx")
    # options.add_extension("../Block-image.crx")
    try:
        service = Service(ChromeDriverManager().install())
        #### if using capture program, use these options ####
        # options.add_argument("headless")
        options.add_argument('--blink-settings=imagesEnabled=false')
        options.add_argument("--window-size=1920,1080")
        driver = webdriver.Chrome(service=service, options=options)
        #####################################################
        driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": useragent})
        driver.maximize_window()
    except:
        print("Error : 프로그램을 종료합니다.")
        notice = """1. chromedriver가 디렉토리 내에 없습니다, 설치해주세요.
                    2. 리눅스 운영체제입니다. 리눅스는 사용할 수 없습니다.
                    3. selenium 버전이 낮습니다. 최신 버전으로 업데이트 해주세요.
                    4. 기타 원인"""
        print(notice)
        quit()
    return driver