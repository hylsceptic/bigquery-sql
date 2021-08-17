--###############################################
--# incrementally update addresses' 
--# latest activate time 
--###############################################

declare excute_day date;
declare now timestamp;
declare gap INT64;
set now = current_timestamp();
set excute_day = date(now);
set gap = 365;


-- step 1: delete addresses whose latest activate times are 3 years ago.
delete from `chaingraph-318604.ethereum_data.addr_latest_activate` 
WHERE pt <= date(timestamp_sub(current_timestamp(), interval 3 * (gap - 1) day))
and timestamp_diff(current_timestamp(), latest_activate_timestamp, second) > 3 * (gap + 1) * 24 * 3600;

-- step 1.i1: update poridical inactivate address
create temp table new_addresses as
with 
valid_address as
(
    select 
        `hash`,
        from_address,
        to_address,
        block_timestamp,
        receipt_status
    from `bigquery-public-data.crypto_ethereum.transactions` 
    where DATE(block_timestamp) >= date_sub(excute_day, INTERVAL 1 DAY)
    and receipt_status = 1
)

select 
    address,
    min(block_timestamp) as first_activate_timestamp,
    max(block_timestamp) as latest_activate_timestamp,
from 
(
    (
        select
            from_address as address,
            block_timestamp
        from valid_address
    )
    union all 
    (
        select
            to_address as address,
            block_timestamp
        from valid_address
    )
) group by address;

insert into `chaingraph-318604.ethereum_tags.eth_priodical_inactivate_addr`
(
    select distinct
        a.address,
        now as create_time
    from
    (
        select distinct address, a.first_activate_timestamp, b.latest_activate_timestamp from new_addresses a inner join `chaingraph-318604.ethereum_data.addr_latest_activate` b using(address)
        where timestamp_sub(a.first_activate_timestamp, interval gap day) > b.latest_activate_timestamp
    ) a left join `chaingraph-318604.ethereum_tags.eth_priodical_inactivate_addr` b on a.address = b.address 
    where b.address is null
);

-- step 1.i2: update inactivate addres
insert into `chaingraph-318604.ethereum_tags.eth_inactivate_addr`
select
    a.address,
    a.latest_activate_timestamp,
    now as create_time
from
(
    select
        *
    from `chaingraph-318604.ethereum_data.addr_latest_activate`
    where timestamp_sub(now, interval 3 * gap day) > latest_activate_timestamp 
) a left join `chaingraph-318604.ethereum_tags.eth_inactivate_addr` b on a.address = b.address where b.address is null;

delete from `chaingraph-318604.ethereum_tags.eth_inactivate_addr` where address in 
(
    select distinct
        address
    from new_addresses inner join `chaingraph-318604.ethereum_tags.eth_inactivate_addr` using(address)
);

-- step 2: delete duplacate addresses
delete from `chaingraph-318604.ethereum_data.addr_latest_activate` 
where address in
(
    select distinct address
    from new_addresses
);

-- step 3: insert new addresses
insert into `chaingraph-318604.ethereum_data.addr_latest_activate` 
select 
    address,
    latest_activate_timestamp,
    date(latest_activate_timestamp) as pt
from new_addresses;

--###############################################
--# create lastest activate history address
--###############################################

-- declare excute_day date;
-- declare now timestamp;
-- declare gap INT64;
-- set now = current_timestamp();
-- set excute_day = date(now);
-- set gap = 365;

-- truncate table `chaingraph-318604.ethereum_data.addr_latest_activate`;
-- insert into `chaingraph-318604.ethereum_data.addr_latest_activate`

-- with 
-- valid_address as
-- (
--     select 
--         a.*
--     from
--     (
--         select 
--             `hash`,
--             from_address,
--             to_address,
--             block_timestamp,
--             receipt_status
--         from `bigquery-public-data.crypto_ethereum.transactions` 
--         -- where DATE(block_timestamp) >= "2021-04-01"
--     ) a
--     left join `chaingraph-318604.ethereum_metrics.tx_before_byzantium` b on a.hash = b.transaction_hash
--     where b.receipt_status = 1 or a.receipt_status = 1
-- )

-- select
--     address,
--     max(block_timestamp) as latest_activate_timestamp,
--     date(max(block_timestamp)) as pt
-- from
-- (
--     (
--         select
--             from_address as address,
--             block_timestamp
--         from valid_address
--     )
--     union all 
--     (
--         select
--             to_address as address,
--             block_timestamp
--         from valid_address
--     )
-- ) group by address 

--###############################################
--# create historical periodic inactivate address table
--###############################################

-- declare gap INT64;
-- set gap = 365;

-- truncate table `chaingraph-318604.ethereum_tags.eth_priodical_inactivate_addr`;
-- insert into `chaingraph-318604.ethereum_tags.eth_priodical_inactivate_addr`

-- with 
-- valid_address as
-- (
--     select 
--         a.*
--     from
--     (
--         select 
--             `hash`,
--             from_address,
--             to_address,
--             block_timestamp,
--             receipt_status
--         from `bigquery-public-data.crypto_ethereum.transactions` 
--     ) a
--     left join `chaingraph-318604.ethereum_metrics.tx_before_byzantium` b on a.hash = b.transaction_hash
--     where b.receipt_status = 1 or a.receipt_status = 1
-- )

-- select distinct
--     address,
--     current_timestamp() as create_time
-- from
-- (
--     select
--         address,
--         block_timestamp,
--         lag(block_timestamp) over (partition by address order by block_timestamp) as last_block_timestamp,
--     from 
--     (
--         (
--             select
--                 from_address as address,
--                 block_timestamp
--             from valid_address
--         )
--         union all 
--         (
--             select
--                 to_address as address,
--                 block_timestamp
--             from valid_address
--         )
--     )
-- ) where last_block_timestamp is not null and timestamp_sub(block_timestamp, interval gap day) > last_block_timestamp



--###############################################
--# create historical inactivate address table
--###############################################

-- declare gap INT64;
-- set gap = 365;

-- insert into `chaingraph-318604.ethereum_tags.eth_inactivate_addr`
-- with 
-- valid_address as
-- (
--     select 
--         a.*
--     from
--     (
--         select 
--             `hash`,
--             from_address,
--             to_address,
--             block_timestamp,
--             receipt_status
--         from `bigquery-public-data.crypto_ethereum.transactions`
--     ) a
--     left join `chaingraph-318604.ethereum_metrics.tx_before_byzantium` b on a.hash = b.transaction_hash
--     where b.receipt_status = 1 or a.receipt_status = 1
-- )

-- select 
--     address,
--     latest_activate_timestamp,
--     current_timestamp() as create_time
-- from
-- (
--     select 
--         address,
--         max(block_timestamp) as latest_activate_timestamp
--     from 
--     (
--         (
--             select
--                 from_address as address,
--                 block_timestamp
--             from valid_address
--         )
--         union all 
--         (
--             select
--                 to_address as address,
--                 block_timestamp
--             from valid_address
--         )
--     )
--     group by address
-- ) where timestamp_sub(current_timestamp(), interval 3 * gap day) > latest_activate_timestamp

