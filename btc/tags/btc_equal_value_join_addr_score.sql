create or replace table `chaingraph-318604.btc_tags.btc_equal_value_join_addr_score` as
with
raw_transactions as (
    select 
        `hash`,
        block_timestamp,
        input_count,
        outputs,
        inputs
    from 
        `bigquery-public-data.crypto_bitcoin.transactions`
    where true
        -- and block_timestamp_month = "2021-08-01"
        -- and DATE(block_timestamp) <= '2021-08-17'
        and block_timestamp_month = DATE_TRUNC(DATE_SUB(@run_date, INTERVAL 1 DAY), MONTH)
        and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
        and input_count >= 3 
        and output_count >= 3 
        and output_count <= input_count * 3
        and output_count >= input_count
)
,
transactions as (
    select
        a.* except (block_timestamp),
        b.price
    from
        (
            raw_transactions a
            full outer join
            `chaingraph-318604.btc_data.btc_price` b
            on date(a.block_timestamp) = date(b.price_timestamp)
        ) 
)
,
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
    (
    select 
        a.*, 
        b.min_input_value 
    from 
        (
            transactions a 
            join 
            tmp_min_input_value b 
            on a.`hash` = b.txhash
        ) 
    ) as transactions,
    transactions.outputs as outputs
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
,
equal_value_join_txhash as (
    select 
        txhash,
        output_value,
        same_output_value_cnt
    from
        tmp_tx2 
    where true 
        and same_output_value_cnt = input_count
        and min_input_value >= output_value
)
,
equal_value_join_tx as (
    select 
        a.*,
        a.price * b.output_value * pow(10, -8) as coinjoin_output_value_usd,
        b.same_output_value_cnt
    from 
        (
            transactions a 
            join 
            equal_value_join_txhash b 
            on a.`hash` = b.txhash
        ) 
)
,
mimo_tx_input_addr as (
    select distinct 
        array_to_string(inputs.addresses, ",") as address,
        coinjoin_output_value_usd,
        same_output_value_cnt
    from
        equal_value_join_tx as transactions,
        transactions.inputs as inputs
)
,
mimo_tx_output_addr as (
    select distinct 
        array_to_string(outputs.addresses, ",") as address,
        coinjoin_output_value_usd,
        same_output_value_cnt
    from
        equal_value_join_tx as transactions,
        transactions.outputs as outputs
)
,
coinjoin_tx_addr as (
    select * from mimo_tx_input_addr 
    union all 
    select * from mimo_tx_output_addr 
)
,
coinjoin_addr_stat as (
    select 
        address,
        # User-defined score function mapping same_output_value_cnt and coinjoin_output_value_usd to score in range 0~1.
        1 / (1 + exp(-((same_output_value_cnt - 5) / 5 + (coinjoin_output_value_usd - 1000) / 1000))) as score 
    from 
        coinjoin_tx_addr 
)
,
coinjoin_addr_score as (
    select 
        address,
        max(score) as score
    from
        coinjoin_addr_stat 
    group by address
)
,
-----------------------------------------------
-- All multi-input-multi-output addresses
-----------------------------------------------
mimo_tx_addr as (
    select distinct 
        address,
        'mixer' as feature,
        null as name,
        'P015' as plots,
        score
    from
        coinjoin_addr_score
)
-- select * from mimo_tx_addr
select distinct 
    * 
from 
    (
        select * from mimo_tx_addr
        union distinct 
        select * from `chaingraph-318604.btc_tags.btc_equal_value_join_addr_score`
    )
-----------------------------------------------
# All multi-input-multi-output tx
-----------------------------------------------
-- mimo_tx_hash as (
--     select distinct 
--         *
--     from
--         (
--             select txhash from mimo_tx_input_addr 
--             union distinct 
--             select txhash from mimo_tx_output_addr 
--         )
-- )
-- select * from mimo_tx_input_addr order by txhash
-- select * from mimo_tx_hash 
;