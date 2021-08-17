declare excute_day date;
declare now timestamp;
declare gap INT64;
set now = current_timestamp();
set excute_day = date(now);
set gap = 365;

-- incrementally update addresses' latest activate time 

-- step 1: delete addresses whose latest activate times are 3 years ago.
delete from `chaingraph-318604.btc_data.addr_latest_activate` 
WHERE pt <= date(timestamp_sub(current_timestamp(), interval 3 * 364 day))
and timestamp_diff(current_timestamp(), latest_activate_timestamp, second) > 3 * 370 * 24 * 3600;

-- step 1.i1: update poridical inactivate address
create temp table new_addresses as
select 
    address,
    min(block_timestamp) as first_activate_timestamp,
    max(block_timestamp) as latest_activate_timestamp,
from `chaingraph-318604.btc_data.btc_value_flow`
where partition_date >= date_sub(excute_day, INTERVAL 1 DAY) group by address;

insert into `chaingraph-318604.btc_tags.btc_priodical_inactivate_addr`
(
    select distinct
        a.address,
        now as create_time
    from
    (
        select distinct address, a.first_activate_timestamp, b.latest_activate_timestamp from new_addresses a inner join `chaingraph-318604.btc_data.addr_latest_activate` b using(address)
        where timestamp_sub(a.first_activate_timestamp, interval gap day) > b.latest_activate_timestamp
    ) a left join `chaingraph-318604.btc_tags.btc_priodical_inactivate_addr` b on a.address = b.address 
    where b.address is null
);

-- step 1.i2: update inactivate addres
insert into `chaingraph-318604.btc_tags.btc_inactivate_addr`
select
    a.address,
    a.latest_activate_timestamp,
    now as create_time
from
(
    select
        *
    from `chaingraph-318604.btc_data.addr_latest_activate`
    where timestamp_sub(current_timestamp(), interval 3 * 365 day) > latest_activate_timestamp 
) a left join `chaingraph-318604.btc_tags.btc_inactivate_addr` b on a.address = b.address where b.address is null;

delete from `chaingraph-318604.btc_tags.btc_inactivate_addr` where address in 
(
    select distinct
        address
    from new_addresses inner join `chaingraph-318604.btc_tags.btc_inactivate_addr` using(address)
);

-- step 2: delete duplacate addresses
delete from `chaingraph-318604.btc_data.addr_latest_activate` 
where address in
(
    select distinct address
    from `chaingraph-318604.btc_data.btc_value_flow` 
    where partition_date >= date_sub(excute_day, INTERVAL 1 DAY)
);

-- step 3: insert new addresses
insert into `chaingraph-318604.btc_data.addr_latest_activate` 
select 
    address,
    max(block_timestamp) as latest_activate_timestamp,
    case when date(max(block_timestamp)) < '2016-01-01' then DATE_TRUNC(date(max(block_timestamp)), MONTH) else date(max(block_timestamp)) end as pt
from `chaingraph-318604.btc_data.btc_value_flow` 
where partition_date >= date_sub(excute_day, INTERVAL 1 DAY) group by address;

--###############################################
--# create lastest activate history address
--###############################################

-- declare excute_day date;
-- set excute_day = date(current_timestamp());

-- create or replace table `chaingraph-318604.btc_data.addr_latest_activate` 
-- partition by pt
-- as
-- select 
--     address,
--     max(block_timestamp) as latest_activate_timestamp,
--     case when date(max(block_timestamp)) < '2016-01-01' then DATE_TRUNC(date(max(block_timestamp)), MONTH) else date(max(block_timestamp)) end as pt
-- from `chaingraph-318604.btc_data.btc_value_flow` 
-- where partition_date >= date_sub(excute_day, INTERVAL 3 * 370 DAY) 
-- and partition_date < date_sub(excute_day, INTERVAL 1 DAY)
-- group by address


--###############################################
--# create historical periodic inactivate address table
--###############################################

-- declare gap INT64;
-- set gap = 365;

-- create table if not exists `chaingraph-318604.btc_tags.btc_priodical_inactivate_addr` as
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
--         SELECT distinct
--             address,
--             block_timestamp
--         FROM `chaingraph-318604.btc_data.btc_value_flow`
--         -- where partition_date >= "2021-07-01"
--     )  
-- ) where last_block_timestamp is not null and timestamp_sub(block_timestamp, interval gap day) > last_block_timestamp


--###############################################
--# create historical inactivate address table
--###############################################

-- create table if not exists `chaingraph-318604.btc_tags.btc_inactivate_addr` as
-- select 
--     address,
--     latest_activate_timestamp,
--     current_timestamp() as create_time
-- from
-- (
--     select 
--         address,
--         max(block_timestamp) as latest_activate_timestamp
--     from `chaingraph-318604.btc_data.btc_value_flow` 
--     -- where partition_date >= '2021-07-25'
--     group by address
-- ) where timestamp_sub(current_timestamp(), interval 3 * 365 day) > latest_activate_timestamp
