select * from index_history_binance; -- 2281695
select * from index_history_okx; -- 4371125
select * from index_history_bybit; -- 2564149
select * from index_history_bitget; -- 616919

select * from tbl_detail_all;

select count(*) from position_history_binance
where id <= 2281695;

select count(*) from position_history_okx
where id <= 4371125;

select * from position_history_bybit
order by id desc
limit 2
;

select count(*) from position_history_binance
where id <= 2281695;