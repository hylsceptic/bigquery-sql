declare excute_day date;
set excute_day = date(timestamp_sub(current_timestamp(), interval 1 day));

-- create table if not exists `chaingraph-318604.btc_tags.frequent_io` as
create or replace table `chaingraph-318604.btc_tags.frequent_io` as

with
tx_inputs as
(
    SELECT 
        inputs.spent_transaction_hash,
        inputs.spent_output_index,
        `hash`,
        transactions.block_number,
        block_timestamp as block_timestamp,
        address
    FROM bigquery-public-data.crypto_bitcoin.transactions as transactions,
        transactions.inputs as inputs,
        unnest(inputs.addresses) as address
    where inputs.type NOT IN ('nulldata', 'nonstandard')
        and 
        (
            (
                block_timestamp_month = DATE_TRUNC(DATE_SUB(excute_day, INTERVAL 1 DAY), MONTH)
                and DATE(block_timestamp) = DATE_SUB(excute_day, INTERVAL 1 DAY)
            ) or
            (
                block_timestamp_month = DATE_TRUNC(excute_day, MONTH)
                and DATE(block_timestamp) = excute_day
            )
        )
),

tx_outputs as
(
    SELECT
        `hash`,
        transactions.block_number,
        outputs.index,
        block_timestamp as block_timestamp,
        address as address
    FROM bigquery-public-data.crypto_bitcoin.transactions as transactions,
        transactions.outputs as outputs,
        unnest(outputs.addresses) as address
    where outputs.type NOT IN ('nulldata', 'nonstandard')
        and 
        (
            (
                block_timestamp_month = DATE_TRUNC(DATE_SUB(excute_day, INTERVAL 1 DAY), MONTH)
                and DATE(block_timestamp) = DATE_SUB(excute_day, INTERVAL 1 DAY)
            ) or
            (
                block_timestamp_month = DATE_TRUNC(excute_day, MONTH)
                and DATE(block_timestamp) = excute_day
            )
        )
),

new_addresses as
(
    select
        address,
        min(gap) as min_gap
    from 
    (
        select
            a.address,
            b.block_timestamp as receive_timestamp,
            a.block_timestamp as spend_timestamp,
            -- b.block_number as receive_height,
            -- a.block_number as spend_height,
            a.block_number - b.block_number as gap
        from tx_inputs a inner join tx_outputs b on a.spent_transaction_hash = b.hash and a.spent_output_index = b.index and a.address = b.address
        where a.block_number - b.block_number <= 12
    ) group by address
    order by min_gap
)

select 
    address,
    min(min_gap) as min_gap
from 
(
    select * from new_addresses 
    union all 
    select * from `chaingraph-318604.btc_tags.frequent_io`
)
group by address