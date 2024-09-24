from datetime import datetime, timedelta
from functools import wraps
import logging

import pandas as pd
import re

from src.config import DATA_DIR

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def scientific_to_float(value):
    return "{:,.2f}".format(value)

def get_id_Binance(text):
    user_id = ""
    pattern = r"lead-details/([A-Za-z0-9]+)\?"
    match = re.search(pattern, text)
    if match:
        user_id = match.group(1)
    return user_id

def clean_df_Binance(df) -> pd.DataFrame:
    for index, row in df.iterrows():
        crypto_exchange = row["crypto_exchange"]
        if crypto_exchange == "Binance":
            url = row["url"]
            user_id = get_id_Binance(url)
            df.at[index, "user_id"] = user_id
    return df


def pre_process_result(today) -> None:
    df_OKX = pd.read_csv(f"crawling_data/{today}/OKX.csv", header=0)
    # df_Binance = pd.read_csv(f"crawling_data/{today}/Binance.csv", header=0)
    df_Bitget = pd.read_csv(f"crawling_data/{today}/Bitget.csv", header=0)
    df_Bybit = pd.read_csv(f"crawling_data/{today}/Bybit.csv", header=0)

    df_OKX["ROI"] = df_OKX["ROI"] * 100
    try:
        df_Bitget["ROI"] = df_Bitget['ROI'].str.replace('%', '').astype(float)
    except AttributeError:
        pass 
    try:
        df_Bitget['PNL'] = df_Bitget['PNL'].str.replace('[\$,]', '', regex=True).astype(float)
    except AttributeError:
        pass
    try:
        df_Bybit["ROI"] = df_Bybit['ROI'].str.replace('%', '').astype(float)
    except AttributeError:
        pass
    try:
        df_Bybit['PNL'] = df_Bybit['PNL'].str.replace('[\$,]', '', regex=True).astype(float)
    except AttributeError:
        pass
    df_OKX.to_csv(f"crawling_data/{today}/OKX.csv", index=None)
    # df_Binance = pd.read_csv(f"crawling_data/{today}/Binance.csv", index=None)
    df_Bitget.to_csv(f"crawling_data/{today}/Bitget.csv", index=None)
    df_Bybit.to_csv(f"crawling_data/{today}/Bybit.csv", index=None)

def data_merge_new(today, specific=None, company_list=None) -> int:
    '''This function find all .csv files and merge into one file'''
    if not specific:
        companies = ['OKX', 'Binance', 'Bitget', 'Bybit']
    else:
        logger.info("specific")
        companies = company_list
    df_list = list()
    for company in companies:
        try:
            df = pd.read_csv(
                f"{DATA_DIR}/{today}/{company}.csv", 
                header=0, 
                index_col=None, 
                dtype={'user_id': str})
            
            if company == 'OKX':
                df["ROI"] = df["ROI"] * 100
            if company == 'Bitget' or company == 'Bybit':
                df["ROI"] = df['ROI'].str.replace('[\%,]', '', regex=True).astype(float)
                df['PNL'] = df['PNL'].str.replace('[\$,]', '', regex=True).astype(float)
            df_list.append(df)
        except Exception as e:
            logger.info(f"{e}")
            return 0
    df = pd.concat(df_list, axis=0, ignore_index=True)
    # This block is used for process if trader_name is null
    null_trader_name_index_lst = df[df['trader_name'].isnull()].index.tolist()
    if null_trader_name_index_lst:
        for index in null_trader_name_index_lst:
            df.loc[index, "trader_name"] = df.loc[index, "user_id"]
    df.to_csv(f"{DATA_DIR}/{today}/data_merge.csv",
        index=None)
    return 1

def process_merge(df) -> pd.DataFrame:
    df = df.loc[((df['ROI'] != 0) | (df['PNL'] != 0))]
    df = df.dropna().reset_index().drop(columns=['index'], axis=1)
    return df

def clean_df_Binance(df) -> pd.DataFrame:
    for index, row in df.iterrows():
        crypto_exchange = row["crypto_exchange"]
        if crypto_exchange == "Binance":
            url = row["url"]
            user_id = get_id_Binance(url)
            df.at[index, "user_id"] = user_id
    return df

def get_all_merge_df() -> pd.DataFrame:
    df_list = list()
    for date_delta in range(1000):
        date = (datetime.today() - timedelta(days=date_delta)).strftime('%y%m%d')
        try:
            df = pd.read_csv(f"{DATA_DIR}/{date}/data_merge.csv", header=0)
            df['transact_date'] = (datetime.today() - timedelta(days=date_delta)).strftime('%y-%m-%d')
        except Exception:
            continue
        df_list.append(df)

    df = pd.concat(df_list, axis=0, ignore_index=True)
    return df

def get_today_df(num_of_days) -> pd.DataFrame:
    df_list = list()
    for date_delta in range(num_of_days):
        date = (datetime.today() - timedelta(days=date_delta)).strftime('%y%m%d')
        try:
            df = pd.read_csv(f"{DATA_DIR}/{date}/data_merge.csv", header=0)
            df['transact_date'] = (datetime.today() - timedelta(days=date_delta)).strftime('%y-%m-%d')
        except Exception:
            columns = ['id', 'trader_name', 'roi', 'pnl', 'url', 'user_id', 'crypto_exchange', 'transact_date', 'transact_period']
            df = pd.DataFrame(columns=columns)
        df_list.append(df)
    df = pd.concat(df_list, axis=0, ignore_index=True)
    return df

def get_today_df_by_date_delta(num_of_days=0, cummulative=None) -> pd.DataFrame:
    df_list = list()
    if not cummulative:
        date = (datetime.today() - timedelta(days=num_of_days)).strftime('%y%m%d')
        try:
            df = pd.read_csv(f"{DATA_DIR}/{date}/data_merge.csv", header=0)
            df['transact_date'] = (datetime.today()).strftime('%y-%m-%d')
        except Exception:
            columns = ['id', 'trader_name', 'roi', 'pnl', 'url', 'user_id', 'crypto_exchange', 'transact_date', 'transact_period']
            df = pd.DataFrame(columns=columns)
    else:
        for date_delta in range(num_of_days):
            date = (datetime.today() - timedelta(days=date_delta)).strftime('%y%m%d')
            try:
                df = pd.read_csv(f"{DATA_DIR}/{date}/data_merge.csv", header=0)
                df['transact_date'] = (datetime.today()).strftime('%y-%m-%d')
            except Exception:
                columns = ['id', 'trader_name', 'roi', 'pnl', 'url', 'user_id', 'crypto_exchange', 'transact_date', 'transact_period']
                df = pd.DataFrame(columns=columns)
    df_list.append(df)
    df = pd.concat(df_list, axis=0, ignore_index=True)
    return df