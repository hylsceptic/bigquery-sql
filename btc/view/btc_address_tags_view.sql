with
tags1 as (
    select 
        *
    from
        `chaingraph-318604.btc_tags.btc_address_tags`
)
,
join_addr as (
    select 
        *
    from 
        `chaingraph-318604.btc_tags.btc_equal_value_join_addr`
)
select 
    *
from 
(
    select * from tags1
    union distinct 
    select 
        address,
        feature,
        null as name,
        plots
    from join_addr
)