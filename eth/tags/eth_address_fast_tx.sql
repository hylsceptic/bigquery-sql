create or replace table `chaingraph-318604.ethereum_tags.address_fast_tx` as
with
transactions as (
    select 
        from_address,
        to_address,
        block_number,
    from 
        `bigquery-public-data.crypto_ethereum.transactions`
    where true
        and to_address is not null
        -- and date(block_timestamp) <= "2021-08-04"
        and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
,
out_tx_addr as (
    select distinct 
        to_address as address,
        block_number,
    from
        transactions
)
,
pre_3_tx as (
select 
    address,
    block_number,
    lag(block_number, 2) over (
        partition by address
        order by block_number asc
    ) as pre_3_tx_block_num
from 
    out_tx_addr 
)
,
addr_fast_incoming_tx as (
    select distinct 
        address,
        'addr_fast_incoming_tx' as feature,
        'P111' as plots,
        1 as idx
    from 
        pre_3_tx
    where pre_3_tx_block_num - block_number <= 5
)
,
pre3_same_input_output_addr as (
    (
    select 
        from_address,
        to_address,
        block_number,
        lag(block_number, 2) over (
            partition by from_address, to_address
            order by block_number asc
        ) as pre_3_tx_block_num
    from 
        transactions 
    )
)
,
addr_fast_incoming_tx_from_same_addr as (
    select distinct 
        to_address as address,
        'addr_fast_incoming_tx_from_same_addr' as feature,
        'P111' as plots,
        2 as idx
    from 
        pre3_same_input_output_addr
    where pre_3_tx_block_num - block_number <= 5
)
,
addr_fast_outgoing_tx_to_same_addr as (
    select distinct 
        from_address as address,
        'addr_fast_outgoing_tx_to_same_addr' as feature,
        'P111' as plots,
        3 as idx
    from 
        pre3_same_input_output_addr
    where pre_3_tx_block_num - block_number <= 5
)
,
addr_fast_tx as (
    select * from
    (
        select * from addr_fast_incoming_tx
        union all 
        select * from addr_fast_incoming_tx_from_same_addr
        union all 
        select * from addr_fast_outgoing_tx_to_same_addr
    )
)
select * from
(
    select * from addr_fast_tx
    union distinct 
    select * from `chaingraph-318604.ethereum_tags.address_fast_tx`
)
-- select * from addr_fast_tx order by address limit 10000
-- select * from pre3_same_input_output_addr order by output_address  limit 10000

