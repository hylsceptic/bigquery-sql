-- declare day date;
-- set day = date("2021-07-10");
truncate table `chaingraph-318604.btc_dm.btc_transfers`;
insert into `chaingraph-318604.btc_dm.btc_transfers`
-- PARTITION BY
--     RANGE_BUCKET(pt, GENERATE_ARRAY(0, 3999, 1))
-- cluster by
--     from_address,
--     to_address
-- as

with tmp as 
(
    select 
        *
    from
    (
        SELECT
            `hash`,
            sum(case when net_value < 0 then 1 else 0 end) as in_cnts,
            sum(case when net_value < 0 then -net_value else 0 end) as in_total,
            sum(case when net_value > 0 then 1 else 0 end) as out_cnts,
            sum(case when net_value > 0 then net_value else 0 end) as out_total,
            sum(case when net_usd_value > 0 then net_usd_value else 0 end) as out_usd_total,
        FROM `chaingraph-318604.btc_data.btc_value_flow` 
        -- WHERE partition_date = day 
        group by `hash`
    ) where in_cnts = 1 or out_cnts = 1 or (in_cnts < 100 and out_cnts < 100)
),

tmp_in as
(
    select
        a.hash,
        a.block_number,
        a.address,
        a.block_timestamp,
        -a.net_value as net_value,
        b.in_cnts,
        b.in_total,
        b.out_usd_total / b.out_total as price
    from 
    (select * from `chaingraph-318604.btc_data.btc_value_flow`
    WHERE net_value < 0
    -- and partition_date = day
    ) a right join tmp b on a.hash = b.hash
),

tmp_out as
(
    select
        a.hash,
        a.block_number,
        a.address,
        a.block_timestamp,
        a.net_value as net_value,
        b.out_cnts,
        b.out_total
    from 
    (select * from `chaingraph-318604.btc_data.btc_value_flow` 
    WHERE net_value > 0
    -- and partition_date = day
    ) a right join tmp b on a.hash = b.hash
),

splited_transfer as
(
    select 
        tmp_in.hash,
        tmp_in.block_number,
        tmp_in.block_timestamp,
        tmp_in.address as from_address,
        tmp_out.address as to_address,
        tmp_in.net_value * tmp_out.net_value / tmp_in.in_total as value,
        tmp_in.net_value * tmp_out.net_value / tmp_in.in_total * price as usd_value,
        row_number() over (partition by tmp_in.address, tmp_out.address order by tmp_in.block_timestamp) as idx,
        row_number() over (partition by tmp_in.address, tmp_out.address order by tmp_in.block_timestamp desc) as idx_desc

    from 
        tmp_in, tmp_out
        where tmp_in.hash = tmp_out.hash order by tmp_in.hash
)

select
    from_address,
    to_address,
    sum(value) as to_amount,
    sum(usd_value) as to_amount_usd,
    min(value) as min_value,
    max(value) as max_value,
    min(usd_value) as min_usd_value,
    max(usd_value) as max_usd_value,
    count(1) as transaction_cnt,
    min(block_number) as first_block_number,
    min(block_timestamp) as first_block_timestamp,
    max(case when idx = 1 then `hash` end) as first_transaction_hash,
    max(block_number) as last_block_number,
    max(block_timestamp) as last_block_timestamp,
    max(case when idx_desc = 1 then `hash` end) as last_transaction_hash,
    mod(cast(concat('0x', substr(cast(md5(from_address) as string format 'hex'), -3)) as int64), 4000) as pt

from splited_transfer group by from_address, to_address;

truncate table `chaingraph-318604.btc_dm.btc_transfers_2`;
insert into `chaingraph-318604.btc_dm.btc_transfers_2`

-- PARTITION BY
--     RANGE_BUCKET(pt, GENERATE_ARRAY(0, 3999, 1))
-- cluster by
--     from_address,
--     to_address
-- as

SELECT
    * except(pt),
    mod(cast(concat('0x', substr(cast(md5(to_address) as string format 'hex'), -3)) as int64), 4000) as pt
FROM `chaingraph-318604.btc_dm.btc_transfers`
