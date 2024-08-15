from bs4 import BeautifulSoup as bs

def get_soup(driver):
    html = driver.page_source  
    soup = bs(html, "html.parser")  
    return soup