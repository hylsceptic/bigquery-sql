-- create or replace table `chaingraph-318604.btc_tags.btc_address_coinbase_miner` as
-- insert into `chaingraph-318604.btc_tags.btc_address_coinbase_miner`
select distinct 
    block_number,
    single_output_address as address,
from 
    `bigquery-public-data.crypto_bitcoin.transactions` as transactions,
    transactions.outputs as outputs,
    unnest(outputs.addresses) as single_output_address
where true
    and is_coinbase = true
    and block_timestamp_month = DATE_TRUNC(DATE_SUB(@run_date, INTERVAL 1 DAY), MONTH)
    and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
    -- and block_timestamp_month <= '2021-08-01'
    -- and array_length(outputs.addresses) > 1
