from datetime import datetime
import pandas as pd

from src.config import DATA_DIR, TRANSACT_PERIOD
from src.utility.openDriver import open_driver
from src.crypto_exchanges.OKX.Card import Card
from src.crypto_exchanges.OKX.Detail import Detail
from src.crypto_exchanges.OKX.config import COMPANY

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
today = datetime.now().strftime("%y%m%d")
# handler = logging.FileHandler(f'log/{today}/okxdetail.log')
# logger.addHandler(handler)
# logger.setLevel(logging.INFO)


company = file_name = COMPANY
transact_period = TRANSACT_PERIOD[0]

def crawler() -> dict:
    driver = open_driver()
    df_card = Card(driver)

    # df_card = pd.read_csv(f"{DATA_DIR}/{today}/{COMPANY}_card.csv", dtype=str, header=0)
    df = Detail(df_card)
    df.to_csv(f"{DATA_DIR}/{today}/{file_name}.csv", index=None)
    driver.quit()

def crawler_history() -> None:
    import concurrent.futures
    from src.data_push.load_data import create_engine_db, get_db_data, create_session
    from sqlalchemy.sql import text
    from src.data_push.scripts.query import select_traders_filter_exchange_name
    from src.data_push.models.DataSchema import create_table_history_OKX
    from src.crypto_exchanges.OKX.Detail import Detail_API_get_position

    engine = create_engine_db()
    _, metadata = create_session(engine)
    create_table_history_OKX(engine, metadata)

    query = text(select_traders_filter_exchange_name) 
    params = {
        'exchange_name': 'OKX'
    }

    with engine.begin() as conn:
        traders_info = get_db_data(conn, query, params)
    engine.dispose()

    trader_id_list = traders_info['user_id_foreign'].to_numpy()

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(Detail_API_get_position, trader_id) for trader_id in trader_id_list]
        for future in concurrent.futures.as_completed(futures):
            return future.result()