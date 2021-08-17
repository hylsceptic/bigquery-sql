-- create table `chaingraph-318604.btc_data.btc_equal_value_join_tx` as
with
transactions as (
    select 
        -- * 
        `hash`,
        input_count,
        outputs,
        inputs
    from 
        -- `bigquery-public-data.crypto_bitcoin_cash.transactions`
        `bigquery-public-data.crypto_bitcoin.transactions`
    where true
        -- and block_timestamp_month = "2019-02-01"
        -- and `hash` = '9ff901c4d22710ff81ec00e63c82b538ae8be4f08eef0c67014f97eac05a980a' -- one bitcoin cash mimo txhash
        -- and block_timestamp_month = "2019-05-01"
        and block_timestamp_month = DATE_TRUNC(DATE_SUB(@run_date, INTERVAL 1 DAY), MONTH)
        and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
        and input_count >= 3 
        and output_count >= 3 
        and output_count <= input_count * 3
        and output_count >= input_count
),
tmp_min_input_value as (
    select 
        txhash,
        min_input_value
    from
    (
        SELECT
            `hash` as txhash,
            min(inputs.value) as min_input_value,
        FROM 
            transactions,
            transactions.inputs as inputs
        group by txhash
    )
)
,
tmp_tx1 as (
SELECT
    `hash` as txhash,
    outputs.value as output_value,
    min(min_input_value) as min_input_value,
    -- array_to_string(outputs.addresses, ",") as address,
    input_count
FROM 
    (select a.*, b.min_input_value from transactions a join tmp_min_input_value b on a.`hash` = b.txhash) as transactions,
    -- transactions,
    transactions.outputs as outputs
    -- transactions.inputs as inputs
WHERE true
group by array_to_string(outputs.addresses, ","), txhash, output_value, input_count--, array_to_string(inputs.addresses, ",")
)
,
tmp_tx2 as (
    select 
        txhash,
        input_count,
        count(*) as same_output_value_cnt,
        output_value,
        min(min_input_value) as min_input_value,
    from tmp_tx1
    group by output_value, txhash, input_count 
)
-- select * from tmp_tx2 order by txhash
,
mimo_txhash as (
    select 
        txhash
    from
        tmp_tx2 
    where true 
        and same_output_value_cnt = input_count
        and min_input_value >= output_value
),
mimo_tx_input_addr as (
    select distinct 
        `hash` as txhash,
        -- array_to_string(inputs.addresses, ",") as address,
    from
        (select a.* from transactions a join mimo_txhash b on a.`hash` = b.txhash) as transactions
        -- transactions.inputs as inputs
),
mimo_tx_output_addr as (
    select distinct 
        `hash` as txhash,
        -- array_to_string(outputs.addresses, ",") as address,
    from
        (select a.* from transactions a join mimo_txhash b on a.`hash` = b.txhash) as transactions
        -- transactions.outputs as outputs
),
-----------------------------------------------
# All multi-input-multi-output addresses
-----------------------------------------------
-- mimo_tx_addr as (
--     select distinct 
--         *
--     from
--         (
--             select address from mimo_tx_input_addr 
--             union distinct 
--             select address from mimo_tx_output_addr 
--         )
-- ),
mimo_tx_hash as (
    select distinct 
        *
    from
        (
            select txhash from mimo_tx_input_addr 
            union distinct 
            select txhash from mimo_tx_output_addr 
        )
)
-- select * from mimo_tx_input_addr order by txhash
-- select * from mimo_tx_addr
select * from mimo_tx_hash 
;