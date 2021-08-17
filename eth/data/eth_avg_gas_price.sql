-- create table `chaingraph-318604.ethereum_data.avg_gas_price` 
-- partition by block_date as
-- insert into `chaingraph-318604.ethereum_data.avg_gas_price`
with 
block_gas_price as (
select 
    block_number,
    any_value(block_timestamp) as the_block_timestamp,
    avg(receipt_effective_gas_price) as block_avg_gas_price
from
    `bigquery-public-data.crypto_ethereum.transactions`
where true
    -- and date(block_timestamp) <= '2021-08-03'
    and DATE(block_timestamp) >= DATE_SUB(@run_date, INTERVAL 2 DAY)
group by block_number
)
,
avg_gas_price as (
select 
    block_number,
    the_block_timestamp as block_timestamp,
    date(the_block_timestamp) as block_date,
    block_avg_gas_price,
    avg(block_avg_gas_price) over (order by block_number asc rows between 3 preceding and 1 preceding) as gas_price_prev3_blk,
    avg(block_avg_gas_price) over (order by block_number asc rows between 6 preceding and 1 preceding) as gas_price_prev6_blk,
    avg(block_avg_gas_price) over (order by block_number asc rows between 12 preceding and 1 preceding) as gas_price_prev12_blk,
from 
    block_gas_price
)
-- select * from avg_gas_price --order by block_number
select 
    a.block_number as block_number,
    a.block_timestamp as block_timestamp,
    date(a.block_timestamp) as block_date,
    a.gas_price_prev3_blk as gas_price_prev3_blk
from
(
    avg_gas_price a
    left join 
    (
        select 
            * 
        from 
            `chaingraph-318604.ethereum_data.avg_gas_price`
        where true
    ) b
    on a.block_number = b.block_number
)
where b.gas_price_prev3_blk is null
;