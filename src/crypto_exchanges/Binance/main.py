import logging
from src.crypto_exchanges.Binance.Card import Card_API
from src.utility.openDriver import open_driver_test
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

today = datetime.now().strftime('%y%m%d')

def crawler() -> dict:
    # driver = open_driver_test()
    Card_API()
    # df_card.to_csv(f"{DATA_DIR}/{today}/{file_name}_card.csv", index=None)
    # driver.quit()