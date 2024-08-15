from datetime import datetime
import pandas as pd

from src.config import DATA_DIR, TRANSACT_PERIOD
from src.utility.openDriver import open_driver_test
from src.crypto_exchanges.OKX.Card import Card
from src.crypto_exchanges.OKX.Detail import Detail
from src.crypto_exchanges.OKX.config import COMPANY

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

today = datetime.now().strftime('%y%m%d')
company = file_name = COMPANY
transact_period = TRANSACT_PERIOD[0]

def crawler() -> dict:
    driver = open_driver_test()
    df_card = Card(driver)

    # df_card = pd.read_csv(f"{DATA_DIR}/{today}/{COMPANY}_card.csv", dtype=str, header=0)
    df = Detail(df_card)
    df.to_csv(f"{DATA_DIR}/{today}/{file_name}.csv", index=None)
    driver.quit()