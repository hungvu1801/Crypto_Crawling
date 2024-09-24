from typing import List
from typing import Optional
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from datetime import date
from datetime import datetime, timedelta
import logging
import pandas as pd

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Date, text, ForeignKey, DateTime, Boolean, DECIMAL, BIGINT
from sqlalchemy.orm import sessionmaker
# class Base(DeclarativeBase):
#     pass

# class StagingTableItem(Base):
#     __tablename__ = "staging_crawling_item"
#     id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
#     trader_name: Mapped[str] = mapped_column(String(30))
#     roi: Mapped[float]
#     pnl: Mapped[float]
#     url: Mapped[str] = mapped_column(String(255))
#     user_id: Mapped[str] = mapped_column(String(100))
#     crypto_exchange: Mapped[str] = mapped_column(String(20))
#     transact_date: Mapped[date] 

#     # staging_crawling_item = Table("staging_crawling_item", metadata, 
#     #     )
def create_database(engine, metadata):

    staging_crawling_item = Table(
        "staging_crawling_item", metadata, 
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('trader_name', String(100), nullable=False),
        Column('roi', Float),
        Column('pnl', Float),
        Column('url', String(255)),
        Column('user_id', String(100)),
        Column('crypto_exchange', String(20)),
        Column('transact_date', Date),
        Column('transact_period', String(10)),
        )
    
    index_update_staging = Table(
        "index_update", metadata, 
        Column('id', Integer, nullable=False),
        Column('startdate', Date, nullable=False),
        )
    
    exchanges = Table(
        "exchanges", metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('exchange_name', String(20), nullable=False),
        Column('updated_at', DateTime, nullable=False),
        )
    
    traders = Table(
        "traders", metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('trader_name', String(100), nullable=False),
        Column('url', String(255)),
        Column('user_id_foreign', String(100)),
        Column('crypto_exchange_id', Integer, ForeignKey("exchanges.id"), nullable=False),
        Column('updated_at', DateTime, nullable=False),
        )
    
    transactions = Table(
        "transactions", metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('trader_id', Integer, ForeignKey("traders.id"), nullable=False),
        Column('roi', Float),
        Column('pnl', Float),
        Column('transact_date', Date, nullable=False),
        Column('updated_at', DateTime, nullable=False),
        )
    metadata.create_all(engine)

def create_table_history_Bitget(engine, metadata):
    # 'achievedProfits', 'businessLine', 'capitalFee', 'closeAvgPrice', 'closeDealCount', 'closeFee', 'closeTime', 'displayName', 'header', 'hm', 'isOnlyLow', 'isTgTrader', 'leftSymbol', 'marginMode', 'netProfit', 'openAvgPrice', 'openDealCount', 'openFee', 'openLevel', 'openMarginCount', 'openTime', 'orderNo', 'position', 'positionAverage', 'positionDesc', 'preShareAmount', 'productCode', 'returnRate', 'symbolCodeDisplayName', 'symbolDisplayName', 'symbolId', 'teacherHeadPic', 'teacherId', 'teacherName', 'tokenId', 'userName']
    _ = Table(
        "position_history_Bitget", metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('achieved_profits', String(30)),
        Column('close_avg_price', String(30)),
        Column('close_deal_count', String(30)),
        Column('close_fee', String(30)),
        Column('close_time', String(20)),
        Column('net_profit', String(30)),
        Column('open_avg_price', String(30)),
        Column('open_deal_count', String(30)),
        Column('open_fee', String(30)),
        Column('open_margin_count', String(30)),
        Column('open_time', String(20)),
        Column('open_level', String(5)),
        Column('order_no', String(50)),
        Column('margin_mode', String(2)),
        Column('position', String(2)),
        Column('left_symbol', String(10)),
        Column('token_id', String(10)),
        Column('position_average', String(30)),
        Column('product_code', String(20)),
        Column('return_rate', String(10)),
        Column('user_name', String(50)),
        Column('teacher_id', String(50)),
        Column('updated_at', DateTime),
    )
    # Create tables if they don't exist
    metadata.create_all(engine)

def create_table_history_Binance(engine, metadata):
    _ = Table(
        "position_history_Binance", metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('order_no', String(30)),
        Column('type', String(10)),
        Column('symbol', String(20)),
        Column('open_time', String(20)),
        Column('close_time', String(20)),
        Column('avg_cost', String(30)),
        Column('avg_close_price', String(30)),
        Column('closing_pnl', String(30)),
        Column('max_open_interest', String(20)),
        Column('closed_volume', String(20)),
        Column('isolated', String(10)),
        Column('side', String(10)),
        Column('status', String(20)),
        Column('teacher_id', String(50)),
        Column('updated_at', DateTime),
    )
    metadata.create_all(engine)

def create_table_history_OKX(engine, metadata):
    _ = Table(
        "position_history_OKX", metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('order_no', String(30)),
        Column('ccy', String(10)),
        Column('close_avg_px', String(30)),
        Column('close_pnl', String(30)),
        Column('contract_val', String(30)),
        Column('deal_volume', String(20)),
        Column('fee', String(30)),
        Column('funding_fee', String(30)),
        Column('symbol', String(25)),
        Column('lever', String(5)),
        Column('liquidation_fee', String(30)),
        Column('margin', String(30)),
        Column('margin_mode', String(10)),
        Column('multiplier', String(5)),
        Column('open_avg_px', String(30)),
        Column('open_time', String(20)),
        Column('close_time', String(20)),
        Column('pnl', String(30)),
        Column('pnl_ratio', String(30)),
        Column('position', String(6)),
        Column('pos_type', String(2)),
        Column('side', String(4)),
        Column('type', String(2)),
        Column('teacher_id', String(50)),
        Column('updated_at', DateTime),
    )
    metadata.create_all(engine)

def create_table_history_Bybit(engine, metadata):
    # 'achievedProfits', 'businessLine', 'capitalFee', 'closeAvgPrice', 'closeDealCount', 'closeFee', 'closeTime', 'displayName', 'header', 'hm', 'isOnlyLow', 'isTgTrader', 'leftSymbol', 'marginMode', 'netProfit', 'openAvgPrice', 'openDealCount', 'openFee', 'openLevel', 'openMarginCount', 'openTime', 'orderNo', 'position', 'positionAverage', 'positionDesc', 'preShareAmount', 'productCode', 'returnRate', 'symbolCodeDisplayName', 'symbolDisplayName', 'symbolId', 'teacherHeadPic', 'teacherId', 'teacherName', 'tokenId', 'userName']
    _ = Table(
        "position_history_Bybit", metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('order_no', String(50)),
        Column('symbol', String(25)),
        Column('side', String(4)),
        Column('is_isolated', Boolean),
        Column('leverage', String(20)),
        Column('entry_price', String(30)),
        Column('closed_price', String(30)),
        Column('size', String(20)),
        Column('pnl', String(30)),
        Column('open_time', String(20)),
        Column('close_time', String(30)),
        Column('cum_funding_fee', String(30)),
        Column('open_cum_exec_fee', String(30)),
        Column('close_cum_exec_fee', String(30)),
        Column('closed_type', String(10)),
        Column('follower_num', String(10)),
        Column('order_cost', String(20)),
        Column('order_net_profit', String(20)),
        Column('order_net_profit_rate', String(20)),
        Column('position_entry_price', String(20)),
        Column('position_cycle_version', String(10)),
        Column('cross_seq', String(20)),
        Column('position_idx', String(2)),
        Column('pos_closed_time', String(20)),
        Column('has_multi_close_order', String(5)),
        Column('status', String(6)),
        Column('teacher_id', String(50)),
        Column('updated_at', DateTime),
    )
    # Create tables if they don't exist
    metadata.create_all(engine)

def create_database_for_detail_history(engine, metadata):

    _ = Table(
        "index_history_binance", metadata, 
        Column('id', Integer, nullable=False),
        Column('updatedate', DateTime, nullable=False),
        )
    
    _ = Table(
        "index_history_bybit", metadata, 
        Column('id', Integer, nullable=False),
        Column('updatedate', DateTime, nullable=False),
        )
    
    _ = Table(
        "index_history_okx", metadata, 
        Column('id', Integer, nullable=False),
        Column('updatedate', DateTime, nullable=False),
        )
    
    _ = Table(
        "index_history_bitget", metadata, 
        Column('id', Integer, nullable=False),
        Column('updatedate', DateTime, nullable=False),
        )
    
    _ = Table(
        "tbl_detail_all", metadata, 
        Column('id', Integer, autoincrement=True, primary_key=True),
        Column('order_no', String(50)),
        Column('open_time', DateTime),
        Column('close_time', DateTime),
        Column('open_price', DECIMAL(30, 15)),
        Column('close_price', DECIMAL(30, 15)),
        Column('pnl', DECIMAL(30, 15)),
        Column('token', String(25)),
        Column('transact_ccy', String(25)),
        Column('position', String(5)),
        Column('margin_mode', String(10)),
        Column('trader_id', BIGINT),
        Column('updated_at', DateTime),
        )
    metadata.create_all(engine)