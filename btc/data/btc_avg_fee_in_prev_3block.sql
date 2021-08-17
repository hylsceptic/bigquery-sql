-- create table `chaingraph-318604.btc_data.btc_avg_fee_in_prev_3block` 
-- partition by block_timestamp_month as
-- insert into `chaingraph-318604.btc_data.btc_avg_fee_in_prev_3block`
with 
tx as (
select 
    block_number,
    any_value(block_timestamp) as the_block_timestamp,
    any_value(block_timestamp_month) as the_block_timestamp_month,
    sum(fee) as block_fee,
    sum(virtual_size) as block_size
from
    `bigquery-public-data.crypto_bitcoin.transactions`
where true
    -- and block_timestamp_month = '2021-08-01'
    -- and date(block_timestamp) = '2021-08-02'
    and block_timestamp_month <= DATE_TRUNC(DATE_SUB(@run_date, INTERVAL 1 DAY), MONTH)
    and block_timestamp_month >= DATE_TRUNC(DATE_SUB(@run_date, INTERVAL 2 DAY), MONTH)
    and DATE(block_timestamp) <= DATE_SUB(@run_date, INTERVAL 1 DAY)
    and DATE(block_timestamp) >= DATE_SUB(@run_date, INTERVAL 2 DAY)
group by block_number
)
,
avg_tx_fee as (
select 
    block_number,
    the_block_timestamp as block_timestamp,
    the_block_timestamp_month as block_timestamp_month,
    avg(block_fee / block_size) over (order by block_number asc rows between 3 preceding and 1 preceding)  as fee_per_byte,
from 
    tx
)
select 
    a.block_number as block_number,
    a.block_timestamp as block_timestamp,
    a.block_timestamp_month as block_timestamp_month,
    a.fee_per_byte as fee_per_byte
from
(
    avg_tx_fee a
    left join 
    (
        select 
            * 
        from 
            `chaingraph-318604.btc_data.btc_avg_fee_in_prev_3block` 
        where true
            and block_timestamp_month >= DATE_TRUNC(DATE_SUB(@run_date, INTERVAL 2 DAY), MONTH)
    ) b
    on a.block_number = b.block_number
)
where b.fee_per_byte is null