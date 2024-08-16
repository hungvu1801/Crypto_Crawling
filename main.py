
from datetime import datetime
# from src.crypto_exchanges.Binance.main import crawler 
from src.crypto_exchanges.OKX.main import crawler
# from src.crypto_exchanges.Bitget.main import crawler
# from src.crypto_exchanges.Bybit.main import crawler

from src.crypto_exchanges.Binance.main import crawler as binance
from src.crypto_exchanges.Bybit.main import crawler as bybit
from src.crypto_exchanges.Bitget.main import crawler as bitget
from src.crypto_exchanges.OKX.main import crawler as okx
from src.utility.helper import create_directories, remove_files, data_merge, pre_process_result, data_merge_new, wrapper_control_VPN
from src.analysts.export_chart import export_charts
from src.utility.saveFile import createDirectory
import logging

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
    ]

crypto_exchange_VPN = [
    bybit, 
    ]

def crawler_main() -> None:
    # Create a folder to contain src result for each auction
    create_directories(today)
    # Run src
    wrapper_control_VPN(crypto_exchange_noVPN)
    
    wrapper_control_VPN(crypto_exchange_VPN, True)

    is_merge_success = data_merge_new(today)
    # # Merge all src result
    if not is_merge_success:
        return

    export_charts(today)
    # # Process file merged, filter False value
    # # Print scanning report to console
    # print_report()

if __name__ == '__main__':
    # input()
    crawler_main()
    # crawler()
