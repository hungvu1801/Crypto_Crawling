from datetime import datetime
from src.config import DATA_DIR, TRANSACT_PERIOD
from src.crypto_exchanges.Bybit.Card import Card_API
from src.crypto_exchanges.Bybit.config import COMPANY
from src.utility.openDriver import open_driver_test
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

today = datetime.now().strftime('%y%m%d')
company = file_name = COMPANY
transact_period = TRANSACT_PERIOD[0]

def crawler() -> dict:
    driver = open_driver_test()
    df_card = Card_API(driver)
    df_card.to_csv(f"{DATA_DIR}/{today}/{file_name}.csv", index=None)
    # df_card = pd.read_csv(f"{DATA_DIR}/{today}/{file_name}_card.csv", dtype=str)
    # df = Detail_selem(driver, df_card)
    driver.quit()