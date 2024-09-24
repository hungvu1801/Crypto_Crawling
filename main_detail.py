
from datetime import datetime
import logging


from src.crypto_exchanges.Bitget.main import crawler_history as bitget_history
from src.crypto_exchanges.Binance.main import crawler_history as binance_history
from src.crypto_exchanges.OKX.main import crawler_history as okx_history
from src.crypto_exchanges.Bybit.main import crawler_history as bybit_history
# from src.crypto_exchanges.Bybit.main import crawler_history_save2json as bybit_history
# from src.crypto_exchanges.Bybit.main import crawler_save2DB_from_json as bybit_history
from src.utility.helper import wrapper_control_VPN
from src.utility.saveFile import createDirectory

today = datetime.now().strftime("%y%m%d")
createDirectory(f"log/{today}")
logging.basicConfig(
    filename=f"log/{today}/filelog.log",
	level=logging.INFO, 
	format="%(asctime)s: %(levelname)s : %(message)s ")


crypto_history_noVPN = [
    # binance_history,
    bitget_history,
    okx_history,
    bybit_history,
]

def crawler_history_main() -> None:
    wrapper_control_VPN(crypto_exchange=crypto_history_noVPN)


def run_main_push_detail_history() -> None:
    print("push data history")
    from src.data_push.load_data import main_push_to_history
    exchanges_lst = ["Binance", "OKX", "Bybit", "Bitget"]
    main_push_to_history(exchanges_lst)


if __name__ == '__main__':
    # input()
    print("Crawling history")
    # crawler_history_main()
    # binance_history()
    # bitget_history()
    # okx_history()
    bybit_history()