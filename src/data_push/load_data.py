from datetime import datetime, timedelta
import logging
import pandas as pd

from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.orm import sessionmaker

from src.data_push.scripts.query import select_staging_get_latest_row, select_staging_get_test_row, select_unloaded_data_from_staging_tbl

from src.data_push.models.DataSchema import create_database, create_database_for_detail_history
from src.utility.data_processing import clean_df_Binance, get_all_merge_df, get_today_df, get_today_df_by_date_delta
from src.utility.helper import run_func_concurrently

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

def create_engine_db():
    import os
    from dotenv import load_dotenv
    load_dotenv()
    username = os.environ.get("DB_username")
    password = os.environ.get("DB_password")
    DB_name = "cryptocrawling"
    engine = create_engine(f"mysql+pymysql://{username}:{password}@localhost:3306")

    with engine.begin() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_name};"))
    engine = create_engine(f"mysql+pymysql://{username}:{password}@localhost:3306/{DB_name}")
    return engine

def create_session(engine):
    Session = sessionmaker(engine)
    session = Session()
    metadata = MetaData()
    return session, metadata

def get_differenciate_dataframe(data_df, data_db, columns) -> pd.DataFrame:

    data_df['source'] = "DF"
    data_db['source'] = "DB"
    combined_df = pd.concat([data_db, data_df])
    difference_df = combined_df.drop_duplicates(subset=columns, keep=False)
    difference_df = difference_df.loc[difference_df['source'] == "DF"].drop(columns=['source'])

    return difference_df


def load_to_main_db_daily(*args):
    engine = args[0]
    pushed_df = pd.read_sql(select_unloaded_data_from_staging_tbl, con=engine)

    ###############################################################################################################
    crypto_df = pushed_df[['crypto_exchange']].drop_duplicates()
    crypto_df.columns = ['exchange_name']
    crypto_db = pd.read_sql('exchanges', con=engine)[['exchange_name']]
   
    insert_crypto_db = get_differenciate_dataframe(crypto_df, crypto_db, ['exchange_name'])

    if not insert_crypto_db.empty:

        insert_crypto_db['updated_at'] = pd.Timestamp.now()
        insert_crypto_db.to_sql(
            name='exchanges',
            con=engine,
            if_exists='append',
            index=False,
            )
    
    ###############################################################################################################
    traders = pushed_df[['trader_name', 'url', 'user_id', 'crypto_exchange']]\
        .drop_duplicates(['user_id', 'crypto_exchange'])\
        .sort_values(['user_id'])
    
    traders.columns = ['trader_name', 'url', 'user_id_foreign', 'exchange_name']
    
    exchanges_db = pd.read_sql('exchanges', con=engine)
    exchanges_db = exchanges_db[['id', 'exchange_name']]
    exchanges_db.columns = ['crypto_exchange_id', 'exchange_name']

    traders_df = pd.merge(
        traders, exchanges_db, on='exchange_name', how='left')\
            .drop(columns = ['exchange_name'])
    
    traders_db = pd.read_sql('traders', con=engine)[['trader_name', 'url', 'user_id_foreign', 'crypto_exchange_id']]

    insert_traders_db = get_differenciate_dataframe(traders_df, traders_db, ['user_id_foreign', 'crypto_exchange_id'])
    if not insert_traders_db.empty:
        insert_traders_db['updated_at'] = pd.Timestamp.now()
        insert_traders_db.to_sql(
            name='traders',
            con=engine,
            if_exists='append',
            index=False,
            )
        
    ###############################################################################################################
    try:
        transactions = pushed_df[['ROI', 'PNL', 'user_id', 'crypto_exchange', 'transact_date']]
    except KeyError:
        transactions = pushed_df[['roi', 'pnl', 'user_id', 'crypto_exchange', 'transact_date']]
    transactions.columns = ['roi', 'pnl', 'user_id_foreign', 'exchange_name', 'transact_date'] # Change column names to match with database field name

    merge_transactions_vs_exchangesdb = pd.merge(
        transactions, exchanges_db, on='exchange_name', how='left')\
            .drop(columns = ['exchange_name'])
    
    traders_db = pd.read_sql('traders', con=engine)
    traders_db = traders_db[['id', 'user_id_foreign', 'crypto_exchange_id']]
    traders_db.columns = ['trader_id', 'user_id_foreign', 'crypto_exchange_id']

    merge_transactions_vs_exchangesdb_vs_tradersdb = pd.merge(
        merge_transactions_vs_exchangesdb, traders_db, 
        on=['user_id_foreign', 'crypto_exchange_id'], how='left')\
            .drop(columns = ['user_id_foreign', 'crypto_exchange_id'])

    merge_transactions_vs_exchangesdb_vs_tradersdb['updated_at'] = pd.Timestamp.now()
    merge_transactions_vs_exchangesdb_vs_tradersdb.to_sql(
        name='transactions',
        con=engine,
        if_exists='append',
        index=False,
        )

def load_to_main_db(*args):
    engine = args[0]
    pushed_df = args[1]
    ###############################################################################################################
    crypto_df = pushed_df[['crypto_exchange']].drop_duplicates()
    crypto_df.columns = ['exchange_name']
    crypto_df['updated_at'] = pd.Timestamp.now()
    crypto_df.to_sql(
        name='exchanges',
        con=engine,
        if_exists='append',
        index=False,
        )
    ###############################################################################################################
    # id, trader_name, roi, pnl, url, user_id, crypto_exchange, transact_date, transact_period
    traders = pushed_df[['trader_name', 'url', 'user_id', 'crypto_exchange']]\
        .drop_duplicates(['user_id', 'crypto_exchange'])\
        .sort_values(['user_id'])
    
    traders.columns = ['trader_name', 'url', 'user_id_foreign', 'exchange_name']
    

    exchanges_db = pd.read_sql('exchanges', con=engine)
    exchanges_db = exchanges_db[['id', 'exchange_name']]
    exchanges_db.columns = ['crypto_exchange_id', 'exchange_name']

    merge_traders_vs_exchanges_db = pd.merge(
        traders, exchanges_db, on='exchange_name', how='left')\
            .drop(columns = ['exchange_name'])
    merge_traders_vs_exchanges_db['updated_at'] = pd.Timestamp.now()
    merge_traders_vs_exchanges_db.to_sql(
        name='traders',
        con=engine,
        if_exists='append',
        index=False,
        )
    ###############################################################################################################
    transactions = pushed_df[['ROI', 'PNL', 'user_id', 'crypto_exchange', 'transact_date']]
    transactions.columns = ['roi', 'pnl', 'user_id_foreign', 'exchange_name', 'transact_date'] # Change column names to match with database field name

    merge_transactions_vs_exchangesdb = pd.merge(
        transactions, exchanges_db, on='exchange_name', how='left')\
            .drop(columns = ['exchange_name'])
    
    traders_db = pd.read_sql('traders', con=engine)
    traders_db = traders_db[['id', 'user_id_foreign', 'crypto_exchange_id']]
    traders_db.columns = ['trader_id', 'user_id_foreign', 'crypto_exchange_id']

    merge_transactions_vs_exchangesdb_vs_tradersdb = pd.merge(
        merge_transactions_vs_exchangesdb, traders_db, 
        on=['user_id_foreign', 'crypto_exchange_id'], how='left')\
            .drop(columns = ['user_id_foreign', 'crypto_exchange_id'])

    merge_transactions_vs_exchangesdb_vs_tradersdb['updated_at'] = pd.Timestamp.now()
    merge_transactions_vs_exchangesdb_vs_tradersdb.to_sql(
        name='transactions',
        con=engine,
        if_exists='append',
        index=False,
        )

def first_load_to_staging_tbl(engine) -> None:
    ###############################################################################################################
    transaction_all_time = get_all_merge_df()
    transaction_all_time = clean_df_Binance(transaction_all_time)
    transaction_all_time.sort_values(by=['transact_date'], ascending=True, inplace=True)
    transaction_all_time.to_sql(
        name='staging_crawling_item',
        con=engine,
        if_exists='append',
        index=False,
        )
    
    ###############################################################################################################
    load_to_main_db_daily(engine)
    load_to_index_tbl(engine)

def daily_load_to_staging_tbl(engine) -> None:
    latest_staging_tbl = pd.read_sql(select_staging_get_latest_row, con=engine)
    num_of_days = (datetime.today().date() - latest_staging_tbl.loc[0, "transact_date"]).days

    if not num_of_days:
        return
    transaction_today = get_today_df(num_of_days)
    if transaction_today.empty:
        return
    transaction_today = clean_df_Binance(transaction_today)
    transaction_today.sort_values(by=['transact_date'], ascending=True, inplace=True)
    transaction_today.to_sql(
        name='staging_crawling_item',
        con=engine,
        if_exists='append',
        index=False,
        )
    load_to_main_db_daily(engine)
    load_to_index_tbl(engine)
    # load_to_index_tbl(engine)

def daily_load_to_staging_tbl_manually(engine) -> None:

    transaction_today = get_today_df_by_date_delta()
    if transaction_today.empty:
        return
    transaction_today = clean_df_Binance(transaction_today)
    transaction_today.to_sql(
        name='staging_crawling_item',
        con=engine,
        if_exists='append',
        index=False,
        )
    load_to_main_db_daily(engine)
    load_to_index_tbl(engine)

def load_to_index_tbl(engine):
    latest_record = pd.read_sql(select_staging_get_latest_row, con=engine)
    cols = ['id', 'startdate']
    index_df = latest_record[['id', 'transact_date']]
    index_df.columns = cols
    index_df.to_sql(
        name='index_update',
        con=engine,
        if_exists='append',
        index=False,
        )
    
def main(*args) -> None:
    engine = create_engine_db()
    _, metadata = create_session(engine)
    create_database(engine, metadata)
    test_record = pd.read_sql(select_staging_get_test_row, con=engine)
    if test_record.empty:
        # Initial loading
        first_load_to_staging_tbl(engine)
        return

    # latest_staging_tbl = pd.read_sql('select * from staging_crawling_item order by id desc limit 1', con=engine)
    # latest_index_tbl = pd.read_sql('select * from index_update order by id desc limit 1', con=engine)
    # id_latest_staging_tbl = int(latest_staging_tbl.iloc[0,0])
    # id_latest_index_tbl = int(latest_index_tbl.iloc[0,0])

    # if id_latest_staging_tbl > id_latest_index_tbl:
    daily_load_to_staging_tbl(engine)
    # daily_load_to_staging_tbl_manually(engine)
    # load_to_main_db_daily(engine)
    engine.dispose()
    
def get_db_data(conn, query, params=None):
    if params:
        result = conn.execute(query, params)
    else:
        result = conn.execute(query)
    cols = result.keys()
    data = result.fetchall()
    return pd.DataFrame(data, columns=cols)

def daily_load_to_tbl_detail_all(exchange_name):
    if exchange_name == "Binance":
        from src.data_push.scripts.query_push_history import insert_binance__tbl_detail as insert__tbl_detail
        from src.data_push.scripts.query_update_index import update_index_history_binance as update_index
        from src.data_push.scripts.query import (
            select_maxid_history_binance as select_maxid_history, 
            select_maxid_index_binance as select_maxid_index)
    elif exchange_name == "OKX":
        from src.data_push.scripts.query_push_history import insert_okx__tbl_detail as insert__tbl_detail
        from src.data_push.scripts.query_update_index import update_index_history_okx as update_index
        from src.data_push.scripts.query import (
            select_maxid_history_okx as select_maxid_history,
            select_maxid_index_okx as select_maxid_index)
    elif exchange_name == "Bitget":
        from src.data_push.scripts.query_push_history import insert_bitget__tbl_detail as insert__tbl_detail
        from src.data_push.scripts.query_update_index import update_index_history_bitget as update_index
        from src.data_push.scripts.query import (
            select_maxid_history_bitget as select_maxid_history,
            select_maxid_index_bitget as select_maxid_index)
    elif exchange_name == "Bybit":
        from src.data_push.scripts.query_push_history import insert_bybit__tbl_detail as insert__tbl_detail
        from src.data_push.scripts.query_update_index import update_index_history_bybit as update_index
        from src.data_push.scripts.query import (
            select_maxid_history_bybit as select_maxid_history,
            select_maxid_index_bybit as select_maxid_index)
    else: return
    engine = create_engine_db()

    with engine.begin() as conn:
        max_id_history = get_db_data(conn, text(select_maxid_history)).iloc[0, 0]
        max_id_index = get_db_data(conn, text(select_maxid_index)).iloc[0, 0]
        if max_id_index < max_id_history:
            conn.execute(text(insert__tbl_detail))
            conn.execute(text(update_index))
        else:
            pass
    engine.dispose()

def main_push_to_history(*args):
    exchanges_lst = args[0]
    engine = create_engine_db()
    _, metadata = create_session(engine)
    create_database_for_detail_history(engine, metadata)
    engine.dispose()
    run_func_concurrently(func=daily_load_to_tbl_detail_all, list_arguments=exchanges_lst)