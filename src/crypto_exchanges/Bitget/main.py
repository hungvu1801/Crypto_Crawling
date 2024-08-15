from datetime import datetime
import logging
import pandas as pd
from src.config import DATA_DIR, TRANSACT_PERIOD
from src.utility.openDriver import open_driver_test

from src.crypto_exchanges.Bitget.config import COMPANY
from src.crypto_exchanges.Bitget.Card import Card
from src.crypto_exchanges.Bitget.Detail import Detail_selem

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

today = datetime.now().strftime('%y%m%d')
company = file_name = COMPANY
transact_period = TRANSACT_PERIOD[0]

def crawler() -> dict:
    driver = open_driver_test()
    df_card = Card(driver)
    # df_card = pd.read_csv(f"{DATA_DIR}/{today}/{file_name}_card.csv", dtype=str)
    df = Detail_selem(driver, df_card)

    df.to_csv(f"{DATA_DIR}/{today}/{file_name}.csv", index=None)

    driver.quit()