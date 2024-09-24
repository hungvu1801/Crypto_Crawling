import pandas as pd

from sqlalchemy.sql import text
from src.data_push.load_data import get_db_data
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class TransactionDataPipeline:
    def __init__(self, engine, exchange, table_name, storage_queue_limit=20):
        self.storage_queue_insert = []
        self.storage_queue_update = []
        self.storage_queue_limit = storage_queue_limit
        self.engine = engine
        self.table_name = table_name
        self.exchange = exchange

    def save_to_DB(self):
        transaction_to_save = self.storage_queue_insert.copy()
        self.storage_queue_insert.clear()
        if not transaction_to_save:
            return
        saving_df = pd.DataFrame(transaction_to_save)
        saving_df.to_sql(
                    name=self.table_name,
                    con=self.engine,
                    if_exists='append',
                    index=False,
                )
        
    def update_item_DB(self):
        if self.exchange == "Binance":
            from src.data_push.scripts.query import update_transaction_Binance as update_transaction
        elif self.exchange == "Bybit":
            from src.data_push.scripts.query import update_transaction_Bybit as update_transaction
        elif self.exchange == "OKX":
            from src.data_push.scripts.query import update_transaction_OKX as update_transaction

        else:
            return
        transactions_to_update = self.storage_queue_update.copy()
        self.storage_queue_update.clear()
        if not transactions_to_update:
            return
        for transaction in transactions_to_update:
            with self.engine.begin() as conn:
                if self.exchange == "Binance":
                    params = {
                        "order_no": transaction["order_no"],
                        "teacher_id": transaction["teacher_id"],
                        "close_time": transaction["close_time"],
                        "avg_cost": transaction["avg_cost"],
                        "avg_close_price": transaction["avg_close_price"],
                        "closing_pnl": transaction["closing_pnl"],
                        "max_open_interest": transaction["max_open_interest"],
                        "status": transaction["status"],
                    }
                elif self.exchange == "Bybit":
                    params = {
                        "order_no": transaction["order_no"],
                        "teacher_id": transaction["teacher_id"],
                        "closed_price": transaction["closed_price"],
                        "close_time": transaction["close_time"],
                        "cum_funding_fee": transaction["cum_funding_fee"],
                        "close_cum_exec_fee": transaction["close_cum_exec_fee"],
                        "order_net_profit": transaction["order_net_profit"],
                        "order_net_profit_rate": transaction["order_net_profit_rate"],
                        "pos_closed_time": transaction["pos_closed_time"],
                        "status": transaction["status"],
                    }
                elif self.exchange == "OKX":
                    params = {
                        "order_no": transaction["order_no"],
                        "teacher_id": transaction["teacher_id"],
                        "close_avg_px": transaction["close_avg_px"],
                        "close_pnl": transaction["close_pnl"],
                        "fee": transaction["fee"],
                        "funding_fee": transaction["funding_fee"],
                        "close_time": transaction["close_time"],
                        "pnl": transaction["pnl"],
                        "pnl_ratio": transaction["pnl_ratio"],
                    }
                query = text(update_transaction)
                conn.execute(query, params)
                logger.info(f"order_no: {transaction['order_no'],} updated")

    def is_changed(self, transaction_data):
        if self.exchange == "Binance":
            from src.data_push.scripts.query import select_get_status_Binance as select_get_status
            if transaction_data["status"] == "Partially Closed":
                return False
        elif self.exchange == "Bybit":
            from src.data_push.scripts.query import select_get_status_Bybit as select_get_status
            if transaction_data["status"] == "Opened":
                return False
        elif self.exchange == "OKX":
            from src.data_push.scripts.query import select_get_status_OKX as select_get_status
            if transaction_data["close_time"] == "":
                return False
        else:
            return False

        query = text(select_get_status)
        params = {
            "order_no": transaction_data["order_no"],
            "teacher_id": transaction_data["teacher_id"]
        }
        with self.engine.begin() as conn:
            status_result = get_db_data(conn, query, params)
            if status_result.iloc[0, 0] == "Partially Closed" \
                or status_result.iloc[0, 0] == "Opened" \
                or status_result.iloc[0, 0] == "":
                return True
            else:
                return False

    def is_duplicate(self, transaction_data):
        if self.exchange == "Bitget":
            from src.data_push.scripts.query import select_transaction_filter_by_order_id_Bitget as select_transaction
        elif self.exchange == "Binance":
            from src.data_push.scripts.query import select_transaction_filter_by_order_id_Binance as select_transaction
        elif self.exchange == "Bybit":
            from src.data_push.scripts.query import select_transaction_filter_by_order_id_Bybit as select_transaction
        elif self.exchange == "OKX":
            from src.data_push.scripts.query import select_transaction_filter_by_order_id_OKX as select_transaction
        query = text(select_transaction)
        
        params = {
            "order_no" : transaction_data["order_no"],
            "teacher_id" : transaction_data["teacher_id"]
        }

        with self.engine.begin() as conn:
            count_result = get_db_data(conn, query, params)

        return not count_result.empty

    def add_transaction(self, transaction_data):
        is_data_duplicate = self.is_duplicate(transaction_data)
        if not is_data_duplicate:
            self.storage_queue_insert.append(transaction_data)
            if len(self.storage_queue_insert) >= self.storage_queue_limit:
                self.save_to_DB()
        else:
            is_data_changed = self.is_changed(transaction_data)
            if is_data_changed:
                self.storage_queue_update.append(transaction_data)
                if len(self.storage_queue_update) >= self.storage_queue_limit:
                    self.update_item_DB()
        return is_data_duplicate

    def close_pipeline(self):
        if len(self.storage_queue_insert) > 0:
            self.save_to_DB()
        if len(self.storage_queue_update) > 0:
            self.update_item_DB()