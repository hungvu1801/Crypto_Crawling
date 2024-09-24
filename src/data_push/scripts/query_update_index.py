update_index_history_binance = """
INSERT INTO index_history_binance (id, updatedate)
SELECT MAX(id) id, now() updatedate FROM position_history_Binance;"""

update_index_history_okx = """
INSERT INTO index_history_okx (id, updatedate)
SELECT MAX(id) id, now() updatedate FROM position_history_OKX;"""

update_index_history_bybit = """
INSERT INTO index_history_bybit (id, updatedate)
SELECT MAX(id) id, now() updatedate FROM position_history_Bybit;"""

update_index_history_bitget = """
INSERT INTO index_history_bitget (id, updatedate)
SELECT MAX(id) id, now() updatedate FROM position_history_Bitget;"""