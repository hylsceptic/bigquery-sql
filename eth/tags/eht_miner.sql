declare excute_day date;
set excute_day = date_sub(date(current_timestamp()), interval 1 day);

create temp table new_tags as
select 
    *
from
(
    SELECT distinct 
        to_address,
        case reward_type
        when 'block' then 'miner'
        when 'uncle' then 'uncle miner'
        end as tags,
    FROM `bigquery-public-data.crypto_ethereum.traces` 
    WHERE trace_type = 'reward'
    and DATE(block_timestamp) = excute_day
) union distinct 
(
    select * from `chaingraph-318604.ethereum_data.miners`
);

truncate table `chaingraph-318604.ethereum_tags.eth_miners`;

insert into `chaingraph-318604.ethereum_tags.eth_miners`
select * from new_tags;


-----------------------------------------
-- history data
-----------------------------------------
-- create or replace table `chaingraph-318604.ethereum_tags.eth_miners` as 

-- SELECT distinct 
--     to_address,
--     case reward_type
--     when 'block' then 'miner'
--     when 'uncle' then 'uncle miner'
--     end as tags,
-- FROM `bigquery-public-data.crypto_ethereum.traces` 
-- WHERE trace_type = 'reward'