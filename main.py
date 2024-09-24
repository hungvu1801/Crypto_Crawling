from datetime import datetime
import logging

# from src.crypto_exchanges.Binance.main import crawler 
from src.analysts.export_chart import export_charts
from src.data_push.load_data import main as load_data_to_DB

# from src.crypto_exchanges.OKX.main import crawler
# from src.crypto_exchanges.Bitget.main import crawler
# from src.crypto_exchanges.Bybit.main import crawler

from src.crypto_exchanges.Binance.main import crawler as binance
from src.crypto_exchanges.Bybit.main import crawler as bybit
from src.crypto_exchanges.Bitget.main import crawler as bitget
from src.crypto_exchanges.OKX.main import crawler as okx
from src.utility.data_processing import data_merge_new
from src.utility.helper import wrapper_control_VPN
from src.utility.saveFile import createDirectory, create_directories


today = datetime.now().strftime("%y%m%d")
createDirectory("log")
createDirectory(f"log/{today}")

logging.basicConfig(
    filename=f"log/{today}/filelog.log",
	level=logging.INFO, 
	format="%(asctime)s: %(levelname)s : %(message)s ")

crypto_exchange_noVPN = [
    binance, 
    bitget,
    okx,
    bybit
    ]

# crypto_exchange_VPN = [
#     bybit, 
#     ]

def crawler_main_overview() -> None:
    print("crawling overview")
    # today = '240829'

    # Create a folder to contain src result for each auction
    create_directories(today)
    # Run src
    wrapper_control_VPN(crypto_exchange_noVPN)
    
    is_merge_success = data_merge_new(today)
    # is_merge_success = data_merge_new(today="240826", specific=True, company_list=["Bitget"])
    # # Merge all src result
    if not is_merge_success:
        return

    export_charts(today)

    load_data_to_DB()



if __name__ == '__main__':
    # input()
    crawler_main_overview()