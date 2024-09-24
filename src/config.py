# Directory to contain result testing files
DATA_DIR = 'crawling_data'

# Result dictionary
RESULT = {
    # "cryto_exchange": [],
    # "transact_period": [],
    "trader_name": [],
    "ROI": [],
    "PNL": [],
    "url": [],
    }

TRANSACT_PERIOD = ["7D", "30D", "90D"]

VPN_DIR = "C:\\Program Files\\Private Internet Access\\piactl.exe"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36", # Window Chrome
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/130.0", # Window Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0", #edge
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36", # Mac Chrome
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:129.0) Gecko/20100101 Firefox/130.0" # Ubuntu
]