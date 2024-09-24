from datetime import datetime
from src.config import DATA_DIR, TRANSACT_PERIOD
from src.crypto_exchanges.Bybit.Card import Card_API
from src.crypto_exchanges.Bybit.config import COMPANY
from src.utility.openDriver import open_driver

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

today = datetime.now().strftime('%y%m%d')
company = file_name = COMPANY
transact_period = TRANSACT_PERIOD[0]

def crawler() -> dict:
    driver = open_driver()
    df_card = Card_API(driver)
    df_card.to_csv(f"{DATA_DIR}/{today}/{file_name}.csv", index=None)
    # df_card = pd.read_csv(f"{DATA_DIR}/{today}/{file_name}_card.csv", dtype=str)
    # df = Detail_selem(driver, df_card)
    driver.quit()

def crawler_history() -> None:
    import concurrent.futures
    from sqlalchemy.sql import text
    from src.data_push.load_data import create_engine_db, get_db_data, create_session
    from src.data_push.scripts.query import select_traders_filter_exchange_name
    from src.data_push.models.DataSchema import create_table_history_Bybit
    from src.crypto_exchanges.Bybit.Detail import Detail_API_get_position

    engine = create_engine_db()
    _, metadata = create_session(engine)
    create_table_history_Bybit(engine, metadata)

    query = text(select_traders_filter_exchange_name) 
    params = {
        'exchange_name': 'Bybit'
    }

    with engine.begin() as conn:
        traders_info = get_db_data(conn, query, params)
    engine.dispose()

    trader_id_list = traders_info['url'].to_numpy() # url in Bybit case contains traderid
    driver_list = [open_driver(headless=False) for _ in range(3)] # Create a list a drivers

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
       
        futures = [executor.submit(Detail_API_get_position, trader_id, driver_list) for trader_id in trader_id_list]
        for future in concurrent.futures.as_completed(futures):
            return future.result()
        
def crawler_history_save2json() -> None:
    import concurrent.futures
    import pandas as pd
    from src.crypto_exchanges.Bybit.Detail import Detail_API_get_position_save2json

    trader_info = pd.read_csv('crawling_data/traders_bybit2.csv', dtype=str)

    trader_id_list = trader_info['url'].to_numpy() # url in Bybit case contains traderid

    driver_list = [open_driver(headless=False) for _ in range(2)] # Create a list a drivers
    done_list = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(Detail_API_get_position_save2json, trader_id, driver_list, done_list) for trader_id in trader_id_list]
            for future in concurrent.futures.as_completed(futures):
                return future.result()
    except Exception:
        df_done_list = pd.DataFrame({"trader_id": done_list}, columns=['trader_id'])
        df_done_list.to_csv("crawling_data/Bybit/doneid2.csv", header=0, index=False)
    df_done_list = pd.DataFrame({"trader_id": done_list}, columns=['trader_id'])
    df_done_list.to_csv("crawling_data/Bybit/doneid2.csv", header=0, index=False)

def crawler_save2DB_from_json() -> None:

    import os
    import concurrent
    from src.data_push.load_data import create_engine_db, create_session
    from src.data_push.models.DataSchema import create_table_history_Bybit
    from src.crypto_exchanges.Bybit.Detail import Detail_API_get_position_push_toDB

    engine = create_engine_db()
    _, metadata = create_session(engine)
    create_table_history_Bybit(engine, metadata)
    engine.dispose()
    data_dir = 'crawling_data/Bybit/Detail'
    files = [os.path.join(data_dir , f"{file}") for file in os.listdir(data_dir) if os.path.splitext(file)[1] == ".json"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
        futures = [executor.submit(Detail_API_get_position_push_toDB, file) for file in files]
        for future in concurrent.futures.as_completed(futures):
            return future.result()
    