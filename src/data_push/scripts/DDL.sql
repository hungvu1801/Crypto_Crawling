use cryptocrawling;
-- delete 
-- from staging_crawling_item
-- where transact_date = '2024-09-12'
-- and crypto_exchange = 'Bybit'
-- ;
create table tmp_view as (
select a.id
	from transactions a
	join traders b on a.trader_id = b.id
	join exchanges c on b.crypto_exchange_id = c.id
	where 1
	and transact_date = '2024-09-12'
	and exchange_name = 'Bybit');
drop table tmp_view;

-- delete 
-- from transactions
-- where id in (select id from tmp_view);
-- ;

-- SET SQL_SAFE_UPDATES = 0;
-- update index_update
-- set id = 1043430
-- where startdate = '2024-09-12'
-- ;
-- SET SQL_SAFE_UPDATES = 1;


CREATE INDEX idx_order_teacher_OKX ON position_history_OKX (order_no, teacher_id);

CREATE INDEX idx_order_teacher_Binance ON position_history_Binance (order_no, teacher_id);

CREATE INDEX idx_order_teacher_Bitget ON position_history_Bitget (order_no, teacher_id);

CREATE INDEX idx_order_teacher_Bybit ON position_history_Bybit (order_no, teacher_id);

SELECT id
FROM position_history_OKX
WHERE close_time = '';

-- drop table position_history_OKX;

-- drop table tbl_detail_all ;
-- drop table index_history_binance;
-- drop table index_history_okx;
-- drop table index_history_bybit;
-- drop table index_history_bitget;

create table if not exists tbl_detail_all (
		id bigint auto_increment primary key,
        order_no varchar(50),
        open_time DateTime,
        close_time DateTime,
        open_price DECIMAL(30, 15),
        close_price DECIMAL(30, 15),
        pnl DECIMAL(30, 15),
        token varchar(25),
        transact_ccy varchar(25),
        position varchar(5),
        margin_mode varchar(10),
        trader_id bigint,
        updated_at DateTime
);
-- describe tbl_detail_all;
-- SET SQL_SAFE_UPDATES = 0;
-- drop table staging_crawling_item;
-- drop table index_update;
-- drop table transactions;
-- drop table traders;
-- drop table exchanges;
-- SET SQL_SAFE_UPDATES = 1;