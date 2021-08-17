-- create or replace table `chaingraph-318604.btc_tags.btc_address_high_fee` as
with 
tx as (
    select 
        *
    from
        `bigquery-public-data.crypto_bitcoin.transactions`
    where true 
        -- and block_timestamp_month = '2021-08-01'
        -- and block_timestamp <= '2021-08-05'
        and block_timestamp_month = DATE_TRUNC(DATE_SUB(@run_date, INTERVAL 1 DAY), MONTH)
        and date(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
,
avg_fee as (
    select 
        block_number,
        fee_per_byte
    from
        `chaingraph-318604.btc_data.btc_avg_fee_in_prev_3block`
    where true 
        -- and block_timestamp_month = '2021-08-01'
        -- and block_timestamp <= '2021-08-06'
        and block_timestamp_month = DATE_TRUNC(DATE_SUB(@run_date, INTERVAL 1 DAY), MONTH)
        and date(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
,
tx_fee as (
    select 
        *
    from
    (
        tx
        join 
        avg_fee
        using (block_number)
    )
)
,
addr_high_fee_in_tx as (
    select distinct 
        single_output_address as address,
        'address_high_fee_in_tx' as feature,
        'P113' as plots,
        1 as idx,
    from 
        tx_fee,
        tx_fee.outputs as outputs,
        unnest(outputs.addresses) as single_output_address
    where true
        and fee / virtual_size >= 5 * fee_per_byte
)
,
addr_high_fee_out_tx as (
    select distinct 
        single_input_address as address,
        'address_high_fee_out_tx' as feature,
        'P113' as plots,
        2 as idx,
    from 
        tx_fee,
        tx_fee.inputs as inputs,
        unnest(inputs.addresses) as single_input_address
    where true
        and fee / virtual_size >= 5 * fee_per_byte
)
,
addr_high_fee_tx as (
    select 
        *
    from 
    (
        select * from addr_high_fee_in_tx
        union distinct
        select * from addr_high_fee_out_tx
    )
)
-- select * from addr_high_fee_tx
select 
    a.address as address,
    a.feature as feature,
    a.plots as plots,
    a.idx as idx
from
(
    addr_high_fee_tx a
    left join 
    (
        select 
            * 
        from 
            `chaingraph-318604.btc_tags.btc_address_high_fee`
    ) b
    on a.address = b.address
)
where b.feature is null