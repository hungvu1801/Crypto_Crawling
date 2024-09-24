import logging
from src.crypto_exchanges.Binance.Card import Card_API
from src.utility.openDriver import open_driver
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
today = datetime.now().strftime("%y%m%d")
# handler = logging.FileHandler(f'log/{today}/binancedetail.log')
# logger.addHandler(handler)
# logger.setLevel(logging.INFO)


def crawler() -> dict:
    # driver = open_driver()
    Card_API()
    # df_card.to_csv(f"{DATA_DIR}/{today}/{file_name}_card.csv", index=None)
    # driver.quit()

def crawler_history() -> None:
    import concurrent.futures
    from src.data_push.load_data import create_engine_db, get_db_data, create_session
    from sqlalchemy.sql import text
    from src.data_push.scripts.query import select_traders_filter_exchange_name
    from src.data_push.models.DataSchema import create_table_history_Binance
    from src.crypto_exchanges.Binance.Detail import Detail_API_get_position
    logger.info("Enter function")
    engine = create_engine_db()
    _, metadata = create_session(engine)
    create_table_history_Binance(engine, metadata)

    query = text(select_traders_filter_exchange_name) 
    params = {
        'exchange_name': 'Binance'
    }

    with engine.begin() as conn:
        traders_info = get_db_data(conn, query, params)
    engine.dispose()
    
    trader_id_list = traders_info['user_id_foreign'].to_numpy()
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(Detail_API_get_position, trader_id) for trader_id in trader_id_list]
        for future in concurrent.futures.as_completed(futures):
            return future.result()
        

    # with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    #     futures = [executor.submit(Detail_API_get_position, trader_id, page_no) for trader_id, page_no in trader_id_list]
    #     for future in concurrent.futures.as_completed(futures):
    #         return future.result()