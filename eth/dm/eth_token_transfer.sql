-- declare excute_day date;
-- set excute_day = date("2020-01-01");

truncate table `chaingraph-318604.eth_dm.eth_token_transfers`;
insert into `chaingraph-318604.eth_dm.eth_token_transfers`
-- PARTITION BY
--     RANGE_BUCKET(pt, GENERATE_ARRAY(0, 3999, 1))
-- cluster by
--     from_address,
--     to_address
-- as

with 
eth_transfers as 
(
    SELECT
        transaction_hash,
        from_address,
        to_address,
        value / pow(10, 18)  as value, 
        block_number,
        block_timestamp,
        row_number() over (partition by from_address, to_address order by block_timestamp) as idx,
        row_number() over (partition by from_address, to_address order by block_timestamp desc) as idx_desc      
    FROM `bigquery-public-data.crypto_ethereum.traces` WHERE true
    -- and DATE(block_timestamp) = excute_day 
    and value > 0
    and from_address is not null -- miner rewards
    and to_address is not null -- smart contract creation but failed.
),

transfers2 as 
(
    select
        *
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
            -- where DATE(block_timestamp) = excute_day
        ) a
        left join `chaingraph-318604.ethereum_metrics.tx_before_byzantium` b on a.hash = b.transaction_hash
    ) b
    on a.transaction_hash = b.`hash` where b.receipt_status = 1
),

eth_rst as
(
    select
        from_address,
        to_address,
        '0x0000000000000000000000000000000000000000' as token_address,
        'eth' as symbol,
        sum(value) as to_amount,
        sum(usd_value) as to_amount_usd,
        min(value) as min_value,
        max(value) as max_value,
        min(usd_value) as min_usd_value,
        max(usd_value) as max_usd_value,
        count(1) as transaction_cnt,
        min(block_number) as first_block_number,
        min(block_timestamp) as first_block_timestamp,
        max(case when idx = 1 then transaction_hash end) as first_transaction_hash,
        max(block_number) as last_block_number,
        max(block_timestamp) as last_block_timestamp,
        max(case when idx_desc = 1 then transaction_hash end) as last_transaction_hash,
        mod(safe_cast(concat('0x', substr(from_address, -3)) as int64), 4000) as pt
    from transfers2 group by from_address, to_address
),

direct_token_transfers as 
(
    select 
        a.*
    from 
        (
            SELECT 
                from_address,
                to_address,
                token_address,
                value,
                block_timestamp,
                block_number,
                transaction_hash
            FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
        --         WHERE DATE(block_timestamp) = excute_day
        ) a 
    inner join 
        (
            select 
                `hash`, substr(input, 0, 10) as input
            from `bigquery-public-data.crypto_ethereum.transactions`
                WHERE starts_with(input, '0xa9059cbb')
                -- and DATE(block_timestamp) = excute_day 
        ) b
    on a.transaction_hash = b.hash
    -- group by b.input
),

valid_tokens as 
(
    select 
        a.address,
        a.symbol,
        b.decimals
    from
        (select distinct address, symbol from `chaingraph-318604.ethereum_data.eth_token_price`) a
        left join 
        (
            select * from
            (
                select address, decimals from `bigquery-public-data.crypto_ethereum.amended_tokens` where decimals is not null
            )
            union all 
            (
                select * from `chaingraph-318604.ethereum_data.token_decimals`
            )
        ) b using(address) where b.decimals is not null
),

processed_token_transfers as
(
    select 
        a.* except(value),
        safe_cast(a.value as numeric) / pow(10, safe_cast(a.decimals as int64)) as value,
        coalesce(b.price, 0) as price,
        safe_cast(a.value as numeric) / pow(10, safe_cast(a.decimals as int64)) * coalesce(b.price, 0) as usd_value,
        row_number() over (partition by from_address, to_address, token_address order by block_timestamp) as idx,
        row_number() over (partition by from_address, to_address, token_address order by block_timestamp desc) as idx_desc     
    from 
    (
        select
            a.*,
            b.symbol,
            b.decimals
        from 
            direct_token_transfers a 
        left join 
            valid_tokens b
            on a.token_address = b.address 
            where b.address is not null
    ) a
    left join 
    (
        select distinct * from `chaingraph-318604.ethereum_data.eth_token_price` 
    ) b
    on a.token_address = b.address
    and date(a.block_timestamp) = price_day
),

toekn_rst as
(
    select
        from_address,
        to_address,
        token_address,
        any_value(symbol) as symbol,
        sum(value) as to_amount,
        sum(usd_value) as to_amount_usd,
        min(value) as min_value,
        max(value) as max_value,
        min(usd_value) as min_usd_value,
        max(usd_value) as max_usd_value,
        count(1) as transaction_cnt,
        min(block_number) as first_block_number,
        min(block_timestamp) as first_block_timestamp,
        max(case when idx = 1 then transaction_hash end) as first_transaction_hash,
        max(block_number) as last_block_number,
        max(block_timestamp) as lacst_block_timestamp,
        max(case when idx_desc = 1 then transaction_hash end) as last_transaction_hash,
        mod(safe_cast(concat('0x', substr(from_address, -3)) as int64), 4000) as pt
    from processed_token_transfers 
    where from_address is not null and to_address is not null
    group by from_address, to_address, token_address
)

select * from  (select * from eth_rst) union all (select * from toekn_rst);

truncate table `chaingraph-318604.eth_dm.eth_token_transfers_2`;
insert into `chaingraph-318604.eth_dm.eth_token_transfers_2` 
-- PARTITION BY
--     RANGE_BUCKET(pt, GENERATE_ARRAY(0, 3999, 1))
-- cluster by
--     to_address,
--     from_address
-- as

SELECT
    * except(pt),
    mod(safe_cast(concat('0x', substr(to_address, -3)) as int64), 4000) as pt
FROM `chaingraph-318604.eth_dm.eth_token_transfers`

