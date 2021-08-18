declare excute_day date;
set excute_day = date_sub(date(current_timestamp()), interval 1 day);

delete from `chaingraph-318604.ethereum_data.eth_succeed_transfers` where pt = excute_day;

insert into `chaingraph-318604.ethereum_data.eth_succeed_transfers`

with 
eth_transfers as (
    SELECT
        transaction_hash,
        from_address,
        to_address,
        value / pow(10, 18)  as value, 
        block_number,
        block_timestamp
    FROM `bigquery-public-data.crypto_ethereum.traces` WHERE true
    and DATE(block_timestamp) = excute_day 
    and value > 0
    and from_address is not null -- miner rewards
    and to_address is not null -- smart contract creation but failed.
),

new_transfers as
(
    select
        `hash`,
        from_address,
        to_address,
        value,
        usd_value,
        date(block_timestamp) as pt
    from
    (
        select
            a.*,
            a.value * coalesce(b.price, 0) as usd_value
        from 
        (
            eth_transfers a 
            left join 
            (select * from `chaingraph-318604.ethereum_data.eth_token_price` 
            where address = '0x0000000000000000000000000000000000000000') b on date(a.block_timestamp) = price_day
        )
    ) a left join 
    (   
        select 
            a.`hash`,
            case when b.receipt_status is not null then b.receipt_status else a.receipt_status end as receipt_status
        from
        (
            select `hash`, receipt_status from `bigquery-public-data.crypto_ethereum.transactions` 
            where DATE(block_timestamp) = excute_day
        ) a
        left join `chaingraph-318604.ethereum_metrics.tx_before_byzantium` b on a.hash = b.transaction_hash
    ) b
    on a.transaction_hash = b.`hash` where b.receipt_status = 1
)

select * from new_transfers
