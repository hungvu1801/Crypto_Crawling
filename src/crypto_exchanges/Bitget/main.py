from datetime import datetime
import logging
import pandas as pd
from src.config import DATA_DIR, TRANSACT_PERIOD
from src.utility.openDriver import open_driver

from src.crypto_exchanges.Bitget.config import COMPANY
from src.crypto_exchanges.Bitget.Card import Card
from src.crypto_exchanges.Bitget.Detail import Detail_selem

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
today = datetime.now().strftime("%y%m%d")
# handler = logging.FileHandler(f'log/{today}/bitgetdetail.log')
# logger.addHandler(handler)
# logger.setLevel(logging.INFO)


company = file_name = COMPANY
transact_period = TRANSACT_PERIOD[0]

def crawler() -> dict:
    driver = open_driver()
    df_card = Card(driver)
    # df_card = pd.read_csv(f"{DATA_DIR}/{today}/{file_name}_card.csv", dtype=str)
    df = Detail_selem(driver, df_card)

    df.to_csv(f"{DATA_DIR}/{today}/{file_name}.csv", index=None)

    driver.quit()

def crawler_history() -> None:
    import concurrent.futures
    from src.data_push.load_data import create_engine_db, get_db_data, create_session
    from sqlalchemy.sql import text
    from src.data_push.scripts.query import select_traders_filter_exchange_name
    from src.data_push.models.DataSchema import create_table_history_Bitget
    from src.crypto_exchanges.Bitget.Detail import Detail_API_get_position

    engine = create_engine_db()
    _, metadata = create_session(engine)
    create_table_history_Bitget(engine, metadata)

    query = text(select_traders_filter_exchange_name) 
    params = {
        'exchange_name': 'Bitget'
    }

    with engine.begin() as conn:
        traders_info = get_db_data(conn, query, params)
    engine.dispose()

    trader_id_list = traders_info['user_id_foreign'].to_numpy()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(Detail_API_get_position, trader_id) for trader_id in trader_id_list]
        for future in concurrent.futures.as_completed(futures):
            return future.result()

    # with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    #     futures = [executor.submit(Detail_API_get_position, trader_id, page_no) for trader_id, page_no in trader_id_list]
    #     for future in concurrent.futures.as_completed(futures):
    #         return future.result()