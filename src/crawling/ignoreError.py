import time

from selenium.common.exceptions import WebDriverException

# to ignore 502 gateway in site of MyartAuction
def get_page_with_retry(driver, url=None):
    if not url:
        url = driver.current_url
    max_attempts = 3
    retry_interval = 1
    for attempt in range(max_attempts):
        try:
            driver.get(url)
            soup = driver.page_source
            if '502 bad gateway' in soup.lower():
                print(f"Case 1. A 502 error message in page_source: Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
                retry_interval += 1
                continue
            else:
                return
        except WebDriverException as e:
            print(f"Failed to load page (attempt {attempt+1}/{max_attempts}): {e}")
            if attempt < max_attempts - 1:
                print(f"Case 2. WebDriverException: Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
                retry_interval += 1
            else:
                raise e