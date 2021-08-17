create or replace table `chaingraph-318604.btc_tags.btc_address_mimo` as
with
transactions as (
    select 
        outputs,
        inputs
    from 
        `bigquery-public-data.crypto_bitcoin.transactions`
    where true
        -- and date(block_timestamp) = '2021-07-28'
        -- and block_timestamp_month = "2021-07-01"
        and block_timestamp_month = DATE_TRUNC(DATE_SUB(@run_date, INTERVAL 1 DAY), MONTH)
        and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
        and input_count >= 3 
        and output_count >= 3 
)
,
mimo_tx_input_addr as (
    select distinct 
        array_to_string(inputs.addresses, ",") as address,
    from
        transactions,
        transactions.inputs as inputs
)
,
mimo_tx_output_addr as (
    select distinct 
        array_to_string(outputs.addresses, ",") as address,
    from
        transactions,
        transactions.outputs as outputs
)
,
-----------------------------------------------
# All multi-input-multi-output addresses
-----------------------------------------------
mimo_tx_addr as (
    select distinct 
        address,
        'mimo' as feature,
        null as name,
        'P109' as plots
    from
        (
            select address from mimo_tx_input_addr 
            union distinct 
            select address from mimo_tx_output_addr 
        )
)
-- select * from mimo_tx_addr
select distinct 
    * 
from 
    (
        select * from mimo_tx_addr
        union distinct 
        select * from `chaingraph-318604.btc_tags.btc_address_mimo`
    )
;