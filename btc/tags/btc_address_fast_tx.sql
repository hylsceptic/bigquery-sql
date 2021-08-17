create or replace table `chaingraph-318604.btc_tags.btc_address_fast_tx` as
with
transactions as (
    select 
        `hash`,
        block_number,
        outputs,
        inputs
    from 
        `bigquery-public-data.crypto_bitcoin.transactions`
    where true
        -- and block_timestamp_month = "2021-07-01"
        -- and date(block_timestamp) = '2019-02-01'
        -- and block_timestamp_month = "2019-05-01"
        and block_timestamp_month <= DATE_TRUNC(DATE_SUB(@run_date, INTERVAL 1 DAY), MONTH)
        and block_timestamp_month >= DATE_TRUNC(DATE_SUB(@run_date, INTERVAL 2 DAY), MONTH)
        and DATE(block_timestamp) <= DATE_SUB(@run_date, INTERVAL 1 DAY)
        and DATE(block_timestamp) >= DATE_SUB(@run_date, INTERVAL 2 DAY)
)
,
tx_addr as (
    select distinct 
        `hash` as txhash,
        block_number,
        array_to_string(inputs.addresses, ",") as input_address,
        array_to_string(outputs.addresses, ",") as output_address,
    from
        transactions,
        transactions.outputs as outputs,
        transactions.inputs as inputs
)
,
tx_output_addr as (
    select distinct 
        `hash` as txhash,
        block_number,
        array_to_string(outputs.addresses, ",") as address,
    from
        transactions,
        transactions.outputs as outputs
)
,
pre_3_tx as (
select 
    txhash,
    address,
    block_number,
    lag(block_number, 2) over (
        partition by address
        order by block_number asc
    ) as pre_3_tx_block_num
from 
    tx_output_addr 
)
,
addr_fast_incoming_tx as (
    select distinct 
        address,
        'addr_fast_incoming_tx' as feature,
        null as name,
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
        txhash,
        output_address,
        input_address,
        block_number,
        lag(block_number, 2) over (
            partition by output_address, input_address
            order by block_number asc
        ) as pre_3_tx_block_num
    from 
        tx_addr 
    )
)
,
addr_fast_incoming_tx_from_same_addr as (
    select distinct 
        output_address as address,
        'addr_fast_incoming_tx_from_same_addr' as feature,
        null as name,
        'P111' as plots,
        2 as idx
    from 
        pre3_same_input_output_addr
    where pre_3_tx_block_num - block_number <= 5
)
,
addr_fast_outgoing_tx_to_same_addr as (
    select distinct 
        input_address as address,
        'addr_fast_outgoing_tx_to_same_addr' as feature,
        null as name,
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
    select * from `chaingraph-318604.btc_tags.btc_address_fast_tx`
)
-- order by address
-- select * from addr_fast_tx order by address limit 10000
-- select * from pre3_same_input_output_addr order by output_address  limit 10000

